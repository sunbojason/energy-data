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
        try:
            api_key = os.environ['ENTSOE_API_KEY']
        except KeyError:
            raise ValueError("CRITICAL: ENTSOE_API_KEY environment variable is missing.")
        self.client = EntsoePandasClient(api_key=api_key)
        self.default_country = DEFAULT_COUNTRY
        self.freq_grid = DEFAULT_FREQ_GRID

    @retry(stop=stop_after_attempt(MAX_RETRY_ATTEMPTS), wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX), reraise=True)
    def _safe_query(self, query_method, *args, **kwargs) -> pd.DataFrame:
        try:
            raw_data = query_method(*args, **kwargs)
            if raw_data is None:
                return pd.DataFrame()
                
            # Treat anything that isn't a Series or DataFrame (like an un-configured MagicMock)
            # as empty data to prevent downstream attribute errors during alignment.
            if not isinstance(raw_data, (pd.DataFrame, pd.Series)):
                return pd.DataFrame()

            if raw_data.empty:
                return pd.DataFrame()
            
            df = raw_data.to_frame() if isinstance(raw_data, pd.Series) else raw_data.copy()
            
            # Discipline to prevent index corruption
            df = df[~df.index.duplicated(keep='last')]
            return df
        except NoMatchingDataError:
            return pd.DataFrame()
        except RequestException as e:
            raise EntsoeAPIError(f"ENTSO-E network failure: {str(e)}") from e
        except Exception as e:
            error_msg = str(e)
            if "stack" in error_msg or "duplicate values" in error_msg:
                logging.warning(f"DATA QUALITY ALERT: {query_method.__name__} skipped due to structural corruption: {error_msg}")
            else:
                logging.warning(f"API Error in {query_method.__name__}: {e}")
            return pd.DataFrame()

    def _align_and_flatten(self, df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()
        df = df[~df.index.duplicated(keep='last')]
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [f"{prefix}_{'_'.join(map(str, col)).strip()}" for col in df.columns.values]
        else:
            if 'imbalance volume' in df.columns and 'imbalance volume.1' in df.columns:
                df = df.rename(columns={'imbalance volume': 'imbalance_short', 'imbalance volume.1': 'imbalance_long'})
            df.columns = [f"{prefix}_{str(col).replace(' ', '_')}" for col in df.columns]
        return df.resample(self.freq_grid).ffill(limit=4)

    def fetch_comprehensive_market_data(self, start_time: pd.Timestamp, end_time: pd.Timestamp, target_country: str = DEFAULT_COUNTRY) -> pd.DataFrame:
        logging.info(f"Starting pipeline execution for {target_country}...")
        master_index = pd.date_range(start=start_time, end=end_time, freq=self.freq_grid, tz=start_time.tz)
        master_df = pd.DataFrame(index=master_index)

        # 1. Core Data
        da = self._safe_query(self.client.query_day_ahead_prices, target_country, start=start_time, end=end_time)
        master_df = master_df.join(self._align_and_flatten(da, 'DA_Price'), how='left')

        load = self._safe_query(self.client.query_load, target_country, start=start_time, end=end_time)
        master_df = master_df.join(self._align_and_flatten(load, 'Load_Actual'), how='left')

        # 2. Neighbors
        exp_cols, imp_cols = [], []
        for n in BE_NEIGHBORS:
            e = self._safe_query(self.client.query_crossborder_flows, target_country, n, start=start_time, end=end_time)
            if not e.empty:
                al = self._align_and_flatten(e, f"Export_{n}")
                master_df = master_df.join(al, how='left')
                exp_cols.extend(al.columns.tolist())
            i = self._safe_query(self.client.query_crossborder_flows, n, target_country, start=start_time, end=end_time)
            if not i.empty:
                al = self._align_and_flatten(i, f"Import_{n}")
                master_df = master_df.join(al, how='left')
                imp_cols.extend(al.columns.tolist())

        # 3. Imbalance
        imb = self._safe_query(self.client.query_imbalance_volumes, target_country, start=start_time, end=end_time)
        master_df = master_df.join(self._align_and_flatten(imb, 'Imb'), how='left')

        # 4. Extended Data Integration
        try:
            ext_df = self._fetch_extended_worker(start_time, end_time, target_country)
            if not ext_df.empty:
                master_df = master_df.join(ext_df, how='left')
        except Exception as ex:
            logging.error(f"Extended fetch failed: {ex}")

        # Final cleanup and normalization
        master_df['Export_Sum'] = master_df[exp_cols].sum(axis=1) if exp_cols else 0.0
        master_df['Import_Sum'] = master_df[imp_cols].sum(axis=1) if imp_cols else 0.0

        master_df = master_df.ffill(limit=2).fillna(0.0)
        return self.finalize_dataframe_structure(master_df)

    def fetch_extended_market_data(self, start_time, end_time, target_country=DEFAULT_COUNTRY) -> pd.DataFrame:
        df = self._fetch_extended_worker(start_time, end_time, target_country)
        return self.finalize_dataframe_structure(df)

    def _fetch_extended_worker(self, start_time, end_time, target_country, **kwargs) -> pd.DataFrame:
        from_c = kwargs.get('flow_from', DEFAULT_FLOW_FROM)
        to_c = kwargs.get('flow_to', DEFAULT_FLOW_TO)
        p_type = kwargs.get('process_type', DEFAULT_PROCESS_TYPE)
        ma_type = kwargs.get('market_agreement_type', DEFAULT_MARKET_AGREEMENT_TYPE)

        master_index = pd.date_range(start=start_time, end=end_time, freq=self.freq_grid, tz=start_time.tz)
        df = pd.DataFrame(index=master_index)

        # Helper to join safely
        def do_add(qy, args, pref, kw=None):
            nonlocal df
            r = self._safe_query(qy, *args, **(kw or {}))
            if not r.empty:
                df = df.join(self._align_and_flatten(r, pref), how='left').reindex(master_index)

        do_add(self.client.query_net_position, (target_country,), 'NetPos', {'dayahead': True, 'start': start_time, 'end': end_time})
        do_add(self.client.query_aggregated_bids, (target_country, p_type), 'AggBids', {'start': start_time, 'end': end_time})
        do_add(self.client.query_load_and_forecast, (target_country,), 'Load', {'start': start_time, 'end': end_time})
        do_add(self.client.query_generation_forecast, (target_country,), 'GenFc', {'start': start_time, 'end': end_time})
        do_add(self.client.query_wind_and_solar_forecast, (target_country,), 'WS_Fc', {'start': start_time, 'end': end_time, 'psr_type': None})
        do_add(self.client.query_intraday_wind_and_solar_forecast, (target_country,), 'WS_ID_Fc', {'start': start_time, 'end': end_time, 'psr_type': None})
        do_add(self.client.query_generation, (target_country,), 'Gen', {'start': start_time, 'end': end_time, 'psr_type': None})
        do_add(self.client.query_scheduled_exchanges, (from_c, to_c), f'SchedExc_{from_c}_{to_c}', {'start': start_time, 'end': end_time, 'dayahead': False})
        do_add(self.client.query_net_transfer_capacity_weekahead, (from_c, to_c), f'NTC_Week_{from_c}_{to_c}', {'start': start_time, 'end': end_time})
        do_add(self.client.query_net_transfer_capacity_monthahead, (from_c, to_c), f'NTC_Month_{from_c}_{to_c}', {'start': start_time, 'end': end_time})
        do_add(self.client.query_contracted_reserve_prices, (target_country, p_type, ma_type), 'ResPrice', {'start': start_time, 'end': end_time})
        do_add(self.client.query_contracted_reserve_prices_procured_capacity, (target_country, p_type, ma_type), 'ResCap', {'start': start_time, 'end': end_time})
        do_add(self.client.query_contracted_reserve_amount, (target_country, p_type, ma_type), 'ResAmt', {'start': start_time, 'end': end_time, 'psr_type': None})
        do_add(self.client.query_generation_per_plant, (target_country,), 'GenPlant', {'start': start_time, 'end': end_time})
        do_add(self.client.query_physical_crossborder_allborders, (target_country, start_time, end_time), 'PhysFlow_Export', {'export': True})
        do_add(self.client.query_import, (target_country, start_time, end_time), 'Import_Border') # Changed prefix to avoid clash

        return df

    def finalize_dataframe_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None: return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join([str(c) for c in col if str(c).strip()]) for col in df.columns.values]
        
        # Aggressive Sanitization: Replace spaces, dots, and common artifacts with underscores
        df.columns = [re.sub(r'_0\s*$', '', str(col)).strip() for col in df.columns]
        df.columns = [re.sub(r'[\s\.\-\(\)]+', '_', col).strip('_') for col in df.columns]
        
        # Always promote the DatetimeIndex to a named column 'Time_UTC' for SQL compatibility.
        if isinstance(df.index, pd.DatetimeIndex):
            df.index.name = 'Time_UTC'
            df = df.reset_index()
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        c = EntsoeDataClient()
        now = pd.Timestamp.now(tz=DEFAULT_TIMEZONE).floor('D')
        print(c.fetch_comprehensive_market_data(now - pd.Timedelta(days=1), now).head())
    except Exception as e: print(e)