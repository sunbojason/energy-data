import os
import pandas as pd
import logging
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
        api_key = os.environ.get('ENTSOE_API_KEY')
        if not api_key:
            raise ValueError("CRITICAL: ENTSOE_API_KEY environment variable is missing.")
        self.client = EntsoePandasClient(api_key=api_key)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
    def fetch_day_ahead_prices(self, country_code: str, start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
        """
        Fetches Day-Ahead prices with built-in resilience for temporary network failures.
        """
        logging.info(f"Fetching DA prices for {country_code} from {start_time} to {end_time}")
        
        try:
            raw_data = self.client.query_day_ahead_prices(country_code, start=start_time, end=end_time)
            # Safely cast to DataFrame to bypass dynamic Series/DataFrame type ambiguities
            df = pd.DataFrame(raw_data)
            df.columns = ['DayAheadPrice']
            return df
            
        except NoMatchingDataError:
            logging.warning(f"No matching data found for {country_code} in the specified time range.")
            return pd.DataFrame()
        except RequestException as e:
            logging.error(f"Network error during API call: {e}")
            raise EntsoeAPIError(f"ENTSO-E API failure: {str(e)}") from e
        except Exception as e:
             logging.error(f"Unexpected error: {e}")
             raise EntsoeAPIError(f"ENTSO-E API unexpected failure: {str(e)}") from e


    def fetch_comprehensive_market_data(self, country_code: str, start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
        """
        Fetches Load, Cross-Border Flows, and Prices, and joins them into a single wide DataFrame.
        Uses isolated try/except blocks to ensure partial failure resilience.
        """
        logging.info(f"Fetching comprehensive data for {country_code} from {start_time} to {end_time}")
        
        data_frames = []

        # Helper function to safely cast and rename without using .to_frame()
        def _safe_append(raw_data, column_name: str):
            if raw_data is not None and not raw_data.empty:
                df = pd.DataFrame(raw_data)
                # Force rename to handle both 1D Series and potential 2D DataFrame edge cases
                df.columns = [column_name]
                data_frames.append(df)

        # 1. Fetch Day-Ahead Prices
        try:
            raw_prices = self.client.query_day_ahead_prices(country_code, start=start_time, end=end_time)
            _safe_append(raw_prices, 'DayAheadPrice')
        except Exception as e:
            logging.error(f"Failed to fetch DA Prices: {e}")

        # 2. Fetch Actual Load (Isolated)
        try:
            raw_actual_load = self.client.query_load(country_code, start=start_time, end=end_time)
            _safe_append(raw_actual_load, 'Actual Load')
        except Exception as e:
            logging.error(f"Failed to fetch Actual Load: {e}")

        # 3. Fetch Forecasted Load (Isolated)
        try:
            raw_forecast_load = self.client.query_load_forecast(country_code, start=start_time, end=end_time)
            _safe_append(raw_forecast_load, 'Forecasted Load')
        except Exception as e:
            logging.error(f"Failed to fetch Forecasted Load: {e}")

        # 4. Fetch Cross-Border Flows (Isolated per neighbor and direction)
        neighbors = ['FR', 'GB', 'DE_LU'] 
        for neighbor in neighbors:
            # Exports
            try:
                raw_export = self.client.query_crossborder_flows(country_code, neighbor, start=start_time, end=end_time)
                _safe_append(raw_export, neighbor)
            except Exception as e:
                logging.debug(f"Export flow data missing for {country_code} to {neighbor}: {e}")
            
            # Imports
            try:
                raw_import = self.client.query_crossborder_flows(neighbor, country_code, start=start_time, end=end_time)
                _safe_append(raw_import, f"import {neighbor}")
            except Exception as e:
                logging.debug(f"Import flow data missing for {neighbor} to {country_code}: {e}")

        # 5. Merge Everything on the Timestamp Index
        if not data_frames:
            return pd.DataFrame()

        master_df = pd.concat(data_frames, axis=1)

        # Calculate aggregations as requested by the SQL schema
        export_cols = [c for c in neighbors if c in master_df.columns]
        import_cols = [f"import {c}" for c in neighbors if f"import {c}" in master_df.columns]
        
        if export_cols:
            master_df['sum'] = master_df[export_cols].sum(axis=1)
        if import_cols:
            master_df['import sum'] = master_df[import_cols].sum(axis=1)

        return master_df