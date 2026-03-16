import os
import pandas as pd
import logging
from typing import List, Dict
from entsoe.entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from requests.exceptions import RequestException
from tenacity import retry, stop_after_attempt, wait_exponential

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
        # Defaulting to Netherlands bidding zone for core strategy execution
        self.default_country = 'NL'
        self.freq_grid = '15min'

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
    def _safe_query(self, query_method, *args, **kwargs) -> pd.DataFrame:
        """
        Generic wrapper to execute ENTSO-E queries with built-in resilience.
        Returns an empty DataFrame on expected missing data exceptions to keep the pipeline intact.
        """
        try:
            raw_data = query_method(*args, **kwargs)
            if raw_data is None or (isinstance(raw_data, (pd.DataFrame, pd.Series)) and raw_data.empty):
                return pd.DataFrame()
                
            # Convert Series to DataFrame safely
            if isinstance(raw_data, pd.Series):
                df = raw_data.to_frame()
            else:
                df = raw_data.copy()
            return df
            
        except NoMatchingDataError:
            func_name = getattr(query_method, '__name__', str(query_method))
            logging.warning(f"No matching data found via {func_name} with args {args}.")
            return pd.DataFrame()
        except RequestException as e:
            logging.error(f"Network error during API call: {e}")
            raise EntsoeAPIError(f"ENTSO-E network failure: {str(e)}") from e
        except Exception as e:
            logging.error(f"Unexpected error executing {query_method}: {e}")
            return pd.DataFrame()

    def _align_and_flatten(self, df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        """
        Enforces strict structural discipline:
        1. Flattens multi-index columns into SQL-friendly strings.
        2. Resolves duplicate naming artifacts (e.g., imbalance volume.1).
        3. Resamples and forward-fills to a strict time grid.
        """
        if df.empty:
            return df

        # 1. Flatten MultiIndex (e.g., from query_current_balancing_state)
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
        # Resample to the target frequency (15min) and forward fill to handle hourly data (like DA prices)
        df_aligned = df.resample(self.freq_grid).ffill(limit=4) # Limit fill to prevent excessive extrapolation
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

        # --- 2. Cross-Border Dynamics (Fixed Neighbors for NL) ---
        # Removed 'FR' as it doesn't border NL. Added BE, NO, DK.
        nl_neighbors = ['BE', 'DE_LU', 'GB', 'NO', 'DK'] 
        export_cols, import_cols = [], []

        for neighbor in nl_neighbors:
            # Export flows
            export_df = self._safe_query(self.client.query_crossborder_flows, country, neighbor, start=start_time, end=end_time)
            if not export_df.empty:
                aligned = self._align_and_flatten(export_df, f"Export_{neighbor}")
                master_df = master_df.join(aligned, how='left')
                export_cols.extend(aligned.columns.tolist())
            
            # Import flows
            import_df = self._safe_query(self.client.query_crossborder_flows, neighbor, country, start=start_time, end=end_time)
            if not import_df.empty:
                aligned = self._align_and_flatten(import_df, f"Import_{neighbor}")
                master_df = master_df.join(aligned, how='left')
                import_cols.extend(aligned.columns.tolist())

        # --- 3. Imbalance & Balancing (Added error protection) ---
        imbalance_vols = self._safe_query(self.client.query_imbalance_volumes, country, start=start_time, end=end_time)
        master_df = master_df.join(self._align_and_flatten(imbalance_vols, 'Imb'), how='left')

        # Specifically catching the 'stack' error for Balancing State
        try:
            balancing_state = self.client.query_current_balancing_state(country, start=start_time, end=end_time)
            if not balancing_state.empty:
                master_df = master_df.join(self._align_and_flatten(balancing_state, 'BalState'), how='left')
        except Exception as e:
            logging.warning(f"Skipping Balancing State due to library error: {e}")

        # Final Aggregation
        master_df['Export_Sum'] = master_df[export_cols].sum(axis=1) if export_cols else 0
        master_df['Import_Sum'] = master_df[import_cols].sum(axis=1) if import_cols else 0

        # Refinement to satisfy SQL constraints
        master_df = master_df.ffill(limit=2).fillna(0.0)
        return master_df

if __name__ == "__main__":
    # Setup standard logging format for the pipeline execution environment
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Example execution scope
    tz = 'Europe/Amsterdam'
    # Defining a specific execution window
    end = pd.Timestamp.now(tz=tz).floor('h')
    start = end - pd.Timedelta(days=1)
    
    try:
        client = EntsoeDataClient()
        df_final = client.fetch_comprehensive_market_data(start_time=start, end_time=end)
        # Process df_final (e.g., push to Azure SQL Database)
    except Exception as e:
        logging.error(f"Critical failure in data orchestration: {e}")