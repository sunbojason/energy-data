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
        NL_NEIGHBORS,
        RETRY_WAIT_MAX,
        RETRY_WAIT_MIN,
    )

class EntsoeAPIError(Exception):
    """
    Custom exception raised when the ENTSO-E API fails to return data 
    after all retry attempts have been exhausted.
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
        """
        Generic wrapper to execute ENTSO-E queries with built-in resilience.
        Added explicit index deduplication to prevent downstream 'stack' errors.
        """
        try:
            raw_data = query_method(*args, **kwargs)
            if raw_data is None or (isinstance(raw_data, (pd.DataFrame, pd.Series)) and raw_data.empty):
                return pd.DataFrame()
                
            if isinstance(raw_data, pd.Series):
                df = raw_data.to_frame()
            else:
                df = raw_data.copy()

            # --- NEW: Discipline to prevent 'duplicate values in stack' error ---
            # Remove any duplicate timestamps returned by the API (keep the most recent one)
            if not df.empty:
                df = df[~df.index.duplicated(keep='last')]
                
            return df
            
        except NoMatchingDataError:
            func_name = getattr(query_method, '__name__', str(query_method))
            logging.warning(f"No matching data found via {func_name} with args {args}.")
            return pd.DataFrame()
        except RequestException as e:
            logging.error(f"Network error during API call: {e}")
            raise EntsoeAPIError(f"ENTSO-E network failure: {str(e)}") from e
        except Exception as e:
            error_msg = str(e)
            if "stack" in error_msg or "duplicate values" in error_msg:
                logging.warning(f"DATA QUALITY ALERT: Metric {query_method.__name__} skipped due to ENTSO-E structural corruption: {error_msg}")
            elif "No matching data" in error_msg:
                 logging.warning(f"No data for {query_method.__name__} in this time window.")
            else:
                logging.error(f"Unexpected API error in {query_method.__name__}: {e}")
            return pd.DataFrame()

    def _align_and_flatten(self, df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        """
        Enforces strict structural discipline:
        1. Flattens multi-index columns into SQL-friendly strings.
        2. Resolves duplicate naming artifacts.
        3. Dedupes index before resampling to prevent aggregation errors.
        """
        if df.empty:
            return df

        # Ensure index is unique before any transformation
        df = df[~df.index.duplicated(keep='last')]

        # 1. Flatten MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [f"{prefix}_{'_'.join(map(str, col)).strip()}" for col in df.columns.values]
        else:
            # Handle specific duplicate columns from ENTSO-E API
            if 'imbalance volume' in df.columns and 'imbalance volume.1' in df.columns:
                df = df.rename(columns={
                    'imbalance volume': 'imbalance_short', 
                    'imbalance volume.1': 'imbalance_long'
                })
            df.columns = [f"{prefix}_{str(col).replace(' ', '_')}" for col in df.columns]

        # 2. Enforce structural time grid
        # Dropping duplicates again here to be safe before resampling
        df_aligned = df.resample(self.freq_grid).ffill(limit=4)
        return df_aligned

    def fetch_comprehensive_market_data(self, start_time: pd.Timestamp, end_time: pd.Timestamp, target_country: str | None = None) -> pd.DataFrame:
        country = target_country or self.default_country
        logging.info(f"Starting pipeline execution for {country}...")
        
        master_index = pd.date_range(start=start_time, end=end_time, freq=self.freq_grid, tz=start_time.tz)
        master_df = pd.DataFrame(index=master_index)

        # --- 1. Core Prices & Load ---
        da_prices = self._safe_query(self.client.query_day_ahead_prices, country, start=start_time, end=end_time)
        master_df = master_df.join(self._align_and_flatten(da_prices, 'DA_Price'), how='left')

        actual_load = self._safe_query(self.client.query_load, country, start=start_time, end=end_time)
        master_df = master_df.join(self._align_and_flatten(actual_load, 'Load_Actual'), how='left')

        # --- 2. Cross-Border Dynamics ---
        export_cols, import_cols = [], []

        for neighbor in NL_NEIGHBORS:
            export_df = self._safe_query(self.client.query_crossborder_flows, country, neighbor, start=start_time, end=end_time)
            if not export_df.empty:
                aligned = self._align_and_flatten(export_df, f"Export_{neighbor}")
                master_df = master_df.join(aligned, how='left')
                export_cols.extend(aligned.columns.tolist())
            
            import_df = self._safe_query(self.client.query_crossborder_flows, neighbor, country, start=start_time, end=end_time)
            if not import_df.empty:
                aligned = self._align_and_flatten(import_df, f"Import_{neighbor}")
                master_df = master_df.join(aligned, how='left')
                import_cols.extend(aligned.columns.tolist())

        # --- 3. Imbalance & Balancing ---
        imbalance_vols = self._safe_query(self.client.query_imbalance_volumes, country, start=start_time, end=end_time)
        master_df = master_df.join(self._align_and_flatten(imbalance_vols, 'Imb'), how='left')

        # Now using the safe wrapper for balancing state as well
        balancing_state = self._safe_query(self.client.query_current_balancing_state, country, start=start_time, end=end_time)
        if not balancing_state.empty:
            master_df = master_df.join(self._align_and_flatten(balancing_state, 'BalState'), how='left')

        # Final Aggregation
        master_df['Export_Sum'] = master_df[export_cols].sum(axis=1) if export_cols else 0
        master_df['Import_Sum'] = master_df[import_cols].sum(axis=1) if import_cols else 0

        # Fill small gaps (max 30 mins) with ffill, then 0.0 for remaining missing data
        master_df = master_df.ffill(limit=2).fillna(0.0)
        master_df = self.finalize_dataframe_structure(master_df)
        
        logging.info(f"Pipeline execution completed. Total rows: {len(master_df)}")
        return master_df
    
    def finalize_dataframe_structure(self,master_df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies strict quantitative data discipline to the final DataFrame before storage.
        Flattens any Pandas MultiIndex columns (like ('Down', 1, 'Activated')) 
        and ensures the timestamp index is securely converted to a named column.
        """
        logging.info("Formatting final DataFrame structure for SQL compatibility...")

        # 1. Flatten MultiIndex columns if they exist (crucial for relational databases)
        if isinstance(master_df.columns, pd.MultiIndex):
            # Join tuple elements with an underscore, ignoring empty strings
            master_df.columns = [
                '_'.join([str(c) for c in col if str(c).strip()]) 
                for col in master_df.columns.values
            ]
            logging.info("Successfully flattened MultiIndex columns.")

        # 2. Clean up default suffixes from entsoe-py (e.g., DA_Price_0 -> DA_Price)
        clean_columns = {
            col: re.sub(r'\s*_0$', '', str(col)) 
            for col in master_df.columns
        }
        
        master_df.rename(columns=clean_columns, inplace=True)

        # 3. Secure the Timestamp Index
        # We use 'Time_UTC' to avoid SQL Server 'timestamp' keyword conflict
        if isinstance(master_df.index, pd.DatetimeIndex):
            master_df.index.name = 'Time_UTC'  # Changed from 'timestamp'
            master_df = master_df.reset_index()
            # Ensure we drop any residual 'Unnamed' columns if they exist
            master_df = master_df.loc[:, ~master_df.columns.str.contains('^Unnamed')]
            logging.info("Time index secured as 'Time_UTC' and 'Unnamed' columns purged.")
        return master_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    tz = DEFAULT_TIMEZONE
    end = pd.Timestamp.now(tz=tz).floor('h')
    start = end - pd.Timedelta(days=1)
    
    try:
        client = EntsoeDataClient()
        df_final = client.fetch_comprehensive_market_data(start_time=start, end_time=end)
    except Exception as e:
        logging.error(f"Critical failure in data orchestration: {e}")