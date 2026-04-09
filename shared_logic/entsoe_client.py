import os
import re
import pandas as pd
import logging
from typing import List, Dict
from entsoe.entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from requests.exceptions import RequestException
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

from shared_logic.constants import (
    DEFAULT_COUNTRY,
    DEFAULT_FREQ_GRID,
    DEFAULT_TIMEZONE,
    MAX_RETRY_ATTEMPTS,
    BE_NEIGHBORS,
    DEFAULT_FLOW_FROM,
    DEFAULT_FLOW_TO,
    DEFAULT_PROCESS_TYPE,
    DEFAULT_MARKET_AGREEMENT_TYPE
)

logger = logging.getLogger(__name__)

class EntsoeAPIError(Exception):
    """Custom exception for ENTSO-E API failures."""
    pass

class EntsoeDataClient:
    """
    Client for interacting with the ENTSO-E API with active column sanitization.
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("ENTSOE_API_KEY")
        if not self.api_key:
            raise ValueError("ENTSOE_API_KEY environment variable is missing.")
        
        self.client = EntsoePandasClient(api_key=self.api_key)
        self.default_country = DEFAULT_COUNTRY
        self.freq_grid = DEFAULT_FREQ_GRID

    @retry(stop=stop_after_attempt(MAX_RETRY_ATTEMPTS), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _safe_query(self, query_func, *args, **kwargs):
        try:
            return query_func(*args, **kwargs)
        except NoMatchingDataError:
            return pd.DataFrame()
        except RequestException as e:
            q_name = getattr(query_func, '__name__', 'API_Query')
            logger.error(f"Network error in {q_name}: {e}")
            raise
        except AttributeError as e:
            q_name = getattr(query_func, '__name__', 'API_Query')
            logger.warning(f"[{q_name}] AttributeError in response parsing — skipping: {e}")
            return pd.DataFrame()
        except Exception as e:
            if "duplicate values are not supported in stack" in str(e).lower():
                logger.warning(f"Structural corruption in response: {e}")
                return pd.DataFrame()
            raise

    def _align_and_flatten(self, raw_data, prefix: str) -> pd.DataFrame:
        """
        Aligns raw data with aggressive column suffix cleaning before it enters the master dataframe.
        """
        if raw_data is None or (isinstance(raw_data, (pd.DataFrame, pd.Series)) and raw_data.empty):
            return pd.DataFrame()

        if isinstance(raw_data, pd.Series):
            df = raw_data.to_frame(name=prefix)
        else:
            df = raw_data.copy()

        # Phase 1: Clean internal redundant labels
        if not isinstance(df.columns, pd.MultiIndex):
            df = df.rename(columns=lambda x: str(x).replace('Actual Load', 'Actual').replace('Actual Generation', 'Actual'))

        # Phase 2: Prefixing
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [f"{prefix}_{'_'.join(filter(None, map(str, col))).strip()}" for col in df.columns.values]
        else:
            df = self._handle_legacy_imbalance_names(df)
            df.columns = [f"{prefix}_{str(col).replace(' ', '_')}" if str(col) != prefix and prefix not in str(col) else str(col) for col in df.columns]

        # Phase 3: Aggressive removal of numeric iteration suffixes like _0, _1
        df.columns = [re.sub(r'_\d+\s*$', '', str(col)).strip() for col in df.columns]
        
        # Phase 4: Consolidate repeated prefixes like DA_Price_DA_Price
        df.columns = [col.replace(f"{prefix}_{prefix}", prefix) for col in df.columns]
        
        # De-duplicate locally to prevent joining multi-columns with same name
        df = df.loc[:, ~df.columns.duplicated()]

        return df.resample(self.freq_grid).asfreq()

    def _handle_legacy_imbalance_names(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'imbalance volume' in df.columns and 'imbalance volume.1' in df.columns:
            return df.rename(columns={
                'imbalance volume': 'imbalance_short', 
                'imbalance volume.1': 'imbalance_long'
            })
        return df

    def fetch_comprehensive_market_data(self, start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
        logging.info(f"Executing comprehensive fetch for {self.default_country}")
        master_index = pd.date_range(start=start_time, end=end_time, freq=self.freq_grid, tz=start_time.tz, inclusive='left')
        master_df = pd.DataFrame(index=master_index)
        
        try:
            query_configs = self._get_query_configs(start_time, end_time, self.default_country)
            for q in query_configs:
                result = self._safe_query(q['func'], *q['args'], **q.get('kwargs', {}))
                if not result.empty:
                    aligned = self._align_and_flatten(result, q['prefix'])
                    # Join logic that avoids adding _x, _y suffixes by skipping existing columns
                    new_cols = [c for c in aligned.columns if c not in master_df.columns]
                    if new_cols:
                        master_df = master_df.join(aligned[new_cols], how='left')
        except (RetryError, RequestException) as e:
            raise EntsoeAPIError(f"API fetch failed: {e}")
        except Exception as e:
            raise EntsoeAPIError(f"Unexpected error: {e}")
                
        return self.finalize_dataframe_structure(master_df)

    def fetch_extended_market_data(self, start_time, end_time, target_country=DEFAULT_COUNTRY) -> pd.DataFrame:
        return self.fetch_comprehensive_market_data(start_time, end_time)

    def _get_query_configs(self, start, end, country) -> List[Dict]:
        configs = [
            {'func': self.client.query_day_ahead_prices, 'args': (country,), 'prefix': 'DA_Price', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_load_and_forecast, 'args': (country,), 'prefix': 'Load', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_net_position, 'args': (country,), 'prefix': 'NetPos', 'kwargs': {'dayahead': True, 'start': start, 'end': end}},
            {'func': self.client.query_imbalance_volumes, 'args': (country,), 'prefix': 'Imb', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_generation_forecast, 'args': (country,), 'prefix': 'GenFc', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_wind_and_solar_forecast, 'args': (country,), 'prefix': 'WS_Fc', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_intraday_wind_and_solar_forecast, 'args': (country,), 'prefix': 'WS_ID_Fc', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_generation, 'args': (country,), 'prefix': 'Gen', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_generation_per_plant, 'args': (country,), 'prefix': 'GenPlant', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_aggregated_bids, 'args': (country, DEFAULT_PROCESS_TYPE), 'prefix': 'AggBids', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_current_balancing_state, 'args': (country,), 'prefix': 'BalState', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_contracted_reserve_prices, 'args': (country, DEFAULT_PROCESS_TYPE, DEFAULT_MARKET_AGREEMENT_TYPE), 'prefix': 'ResPrice', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_contracted_reserve_prices_procured_capacity, 'args': (country, DEFAULT_PROCESS_TYPE, DEFAULT_MARKET_AGREEMENT_TYPE), 'prefix': 'ResCap', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_contracted_reserve_amount, 'args': (country, DEFAULT_PROCESS_TYPE, DEFAULT_MARKET_AGREEMENT_TYPE), 'prefix': 'ResAmt', 'kwargs': {'start': start, 'end': end}},
        ]
        for neighbor in BE_NEIGHBORS:
            configs.append({'func': self.client.query_crossborder_flows, 'args': (country, neighbor), 'prefix': f'Export_{neighbor}', 'kwargs': {'start': start, 'end': end}})
            configs.append({'func': self.client.query_crossborder_flows, 'args': (neighbor, country), 'prefix': f'Import_{neighbor}', 'kwargs': {'start': start, 'end': end}})
            configs.append({'func': self.client.query_scheduled_exchanges, 'args': (country, neighbor), 'prefix': f'SchedExc_{neighbor}', 'kwargs': {'start': start, 'end': end, 'dayahead': False}})
            configs.append({'func': self.client.query_net_transfer_capacity_weekahead, 'args': (country, neighbor), 'prefix': f'NTC_Week_{neighbor}', 'kwargs': {'start': start, 'end': end}})
        
        configs.append({'func': self.client.query_physical_crossborder_allborders, 'args': (country, start, end), 'prefix': 'PhysFlow_All', 'kwargs': {'export': True}})
        configs.append({'func': self.client.query_import, 'args': (country, start, end), 'prefix': 'Import_Sum'})
        return configs

    def finalize_dataframe_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None: return pd.DataFrame()
        df = self._flatten_column_names(df)
        df = self._sanitize_column_formatting(df)
        
        # De-duplicate columns (ensures only unique names survive the cleaning process)
        df = df.loc[:, ~df.columns.duplicated()]
        
        if isinstance(df.index, pd.DatetimeIndex):
            df.index.name = 'Time_UTC'
            df = df.reset_index()
            # Clean index artifacts
            df = df.loc[:, ~df.columns.str.contains('^Unnamed|^index')]
        return df

    def _flatten_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(filter(None, [str(c).strip() for c in col])) for col in df.columns.values]
        return df

    def _sanitize_column_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        # Global cleaning pass for suffixes and redundant prefixes
        df.columns = [re.sub(r'_\d+\s*$', '', str(col)).strip() for col in df.columns]
        df.columns = [re.sub(r'[\s\.\-\(\)]+', '_', col).strip('_') for col in df.columns]
        
        # Consolidate Load and actual redundancies recursively
        df.rename(columns=lambda x: x.replace('Load_Actual_Load', 'Load_Actual').replace('Actual_Actual', 'Actual'), inplace=True)
        return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        c = EntsoeDataClient()
        now = pd.Timestamp.now(tz=DEFAULT_TIMEZONE).floor('D')
        data = c.fetch_comprehensive_market_data(now - pd.Timedelta(days=1), now)
        print(f"Final Column Count: {len(data.columns)}")
    except Exception as e:
        print(f"Sample run failed: {e}")