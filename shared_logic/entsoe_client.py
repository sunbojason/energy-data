import os
import re
import pandas as pd
import logging
from typing import List, Dict
from entsoe.entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from requests.exceptions import RequestException
from tenacity import retry, stop_after_attempt, wait_exponential

from shared_logic.constants import (
    DEFAULT_COUNTRY,
    DEFAULT_FREQ_GRID,
    DEFAULT_TIMEZONE,
    MAX_RETRY_ATTEMPTS,
    BE_NEIGHBORS,
    RETRY_WAIT_MAX,
    RETRY_WAIT_MIN,
    DEFAULT_FLOW_FROM,
    DEFAULT_FLOW_TO,
    DEFAULT_PROCESS_TYPE,
    DEFAULT_MARKET_AGREEMENT_TYPE,
)

class EntsoeAPIError(Exception):
    """
    Custom exception raised when the ENTSO-E API fails to return data.
    """
    pass

class EntsoeDataClient:
    def __init__(self):
        self._initialize_client()
        self.default_country = DEFAULT_COUNTRY
        self.freq_grid = DEFAULT_FREQ_GRID

    def _initialize_client(self):
        try:
            api_key = os.environ['ENTSOE_API_KEY']
            self.client = EntsoePandasClient(api_key=api_key)
        except KeyError:
            raise ValueError("ENTSOE_API_KEY environment variable is missing.")

    @retry(stop=stop_after_attempt(MAX_RETRY_ATTEMPTS), wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX), reraise=True)
    def _safe_query(self, query_method, *args, **kwargs) -> pd.DataFrame:
        method_name = getattr(query_method, "__name__", str(query_method))
        try:
            raw_data = query_method(*args, **kwargs)
            return self._standardize_raw_response(raw_data, method_name)
        except NoMatchingDataError:
            return pd.DataFrame()
        except RequestException as e:
            raise EntsoeAPIError(f"Network failure during {method_name}: {str(e)}") from e
        except Exception as e:
            self._handle_structural_errors(e, method_name)
            return pd.DataFrame()

    def _standardize_raw_response(self, raw_data, method_name: str) -> pd.DataFrame:
        if raw_data is None or (isinstance(raw_data, (pd.DataFrame, pd.Series)) and raw_data.empty):
            return pd.DataFrame()
        
        df = raw_data.to_frame() if isinstance(raw_data, pd.Series) else raw_data.copy()
        
        # Flatten MultiIndex columns immediately if present to avoid 'stack' errors
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
            
        # De-duplicate index and columns
        df = df[~df.index.duplicated(keep='last')]
        df = df.loc[:, ~df.columns.duplicated(keep='last')]
        
        return df

    def _handle_structural_errors(self, error: Exception, method_name: str):
        error_msg = str(error)
        if any(keyword in error_msg for keyword in ["stack", "duplicate values"]):
            logging.warning(f"Structural corruption in {method_name}: {error_msg}")
        else:
            logging.warning(f"Error in {method_name}: {error}")

    def _align_and_flatten(self, df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [f"{prefix}_{'_'.join(map(str, col)).strip()}" for col in df.columns.values]
        else:
            df = self._handle_legacy_imbalance_names(df)
            df.columns = [f"{prefix}_{str(col).replace(' ', '_')}" for col in df.columns]

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
        
        master_df = pd.DataFrame(index=pd.date_range(start=start_time, end=end_time, freq=self.freq_grid, tz=start_time.tz, inclusive='left'))
        metrics = self._fetch_all_core_metrics(start_time, end_time)
        
        for prefix, data in metrics.items():
            master_df = master_df.join(self._align_and_flatten(data, prefix), how='left')

        master_df = self._integrate_extended_data(master_df, start_time, end_time)
        return self.finalize_dataframe_structure(master_df)

    def _fetch_all_core_metrics(self, start, end) -> Dict[str, pd.DataFrame]:
        return {
            'DA_Price': self._safe_query(self.client.query_day_ahead_prices, self.default_country, start=start, end=end),
            'Load_Actual': self._safe_query(self.client.query_load, self.default_country, start=start, end=end),
            'Imb': self._safe_query(self.client.query_imbalance_volumes, self.default_country, start=start, end=end)
        }

    def _integrate_extended_data(self, master_df: pd.DataFrame, start, end) -> pd.DataFrame:
        try:
            extended_df = self._fetch_extended_metrics(start, end, self.default_country)
            if not extended_df.empty:
                return master_df.join(extended_df, how='left')
        except Exception as e:
            logging.error(f"Extended metrics integration failed: {e}")
        return master_df

    def fetch_extended_market_data(self, start_time, end_time, target_country=DEFAULT_COUNTRY) -> pd.DataFrame:
        df = self._fetch_extended_metrics(start_time, end_time, target_country)
        return self.finalize_dataframe_structure(df)

    def _fetch_extended_metrics(self, start_time, end_time, target_country) -> pd.DataFrame:
        master_index = pd.date_range(start=start_time, end=end_time, freq=self.freq_grid, tz=start_time.tz, inclusive='left')
        df = pd.DataFrame(index=master_index)

        def add_metric(query_func, args, prefix, extra_kwargs=None):
            nonlocal df
            result = self._safe_query(query_func, *args, **(extra_kwargs or {}))
            if not result.empty:
                df = df.join(self._align_and_flatten(result, prefix), how='left').reindex(master_index)

        queries = self._get_extended_query_configs(start_time, end_time, target_country)
        for q in queries:
            add_metric(q['func'], q['args'], q['prefix'], q.get('kwargs'))

        return df

    def _get_extended_query_configs(self, start, end, country) -> List[Dict]:
        return [
            {'func': self.client.query_net_position, 'args': (country,), 'prefix': 'NetPos', 'kwargs': {'dayahead': True, 'start': start, 'end': end}},
            {'func': self.client.query_load_and_forecast, 'args': (country,), 'prefix': 'Load', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_generation_forecast, 'args': (country,), 'prefix': 'GenFc', 'kwargs': {'start': start, 'end': end}},
            {'func': self.client.query_generation, 'args': (country,), 'prefix': 'Gen', 'kwargs': {'start': start, 'end': end, 'psr_type': None}},
            {'func': self.client.query_generation_per_plant, 'args': (country,), 'prefix': 'GenPlant', 'kwargs': {'start': start, 'end': end}},
        ]

    def finalize_dataframe_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None: return pd.DataFrame()
        
        df = self._flatten_column_names(df)
        df = self._sanitize_column_formatting(df)
        
        if isinstance(df.index, pd.DatetimeIndex):
            df.index.name = 'Time_UTC'
            df = df.reset_index()
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        return df

    def _flatten_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join([str(c) for c in col if str(c).strip()]) for col in df.columns.values]
        return df

    def _sanitize_column_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [re.sub(r'_0\s*$', '', str(col)).strip() for col in df.columns]
        df.columns = [re.sub(r'[\s\.\-\(\)]+', '_', col).strip('_') for col in df.columns]
        return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        c = EntsoeDataClient()
        now = pd.Timestamp.now(tz=DEFAULT_TIMEZONE).floor('D')
        print(c.fetch_comprehensive_market_data(now - pd.Timedelta(days=1), now).head())
    except Exception as e: print(e)