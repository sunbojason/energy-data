import os
import logging
from datetime import datetime
import pandas as pd
from entsoe.entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

class EntsoeAPIError(Exception):
    """Custom exception for ENTSO-E API failures after retries."""
    pass

class EntsoeDataClient:
    def __init__(self):
        """
        Initializes the ENTSO-E client. 
        Expects ENTSOE_API_KEY to be set in local.settings.json or Azure App Settings.
        """
        self.api_key = os.environ.get("ENTSOE_API_KEY")
        if not self.api_key:
            logger.error("ENTSOE_API_KEY environment variable is missing.")
            raise ValueError("API Key for ENTSO-E is required.")
        
        self.client = EntsoePandasClient(api_key=self.api_key)

    @staticmethod
    def _raise_api_error(retry_state):
        """Called by tenacity after all retry attempts are exhausted."""
        cause = retry_state.outcome.exception()
        raise EntsoeAPIError(f"ENTSO-E API failure: {str(cause)}") from cause


    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=16),
        retry=retry_if_exception_type(RequestException),
        retry_error_callback=_raise_api_error,
    )
    def fetch_day_ahead_prices(self, country_code: str, start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
        """
        Fetches Day-Ahead prices for a specific bidding zone.
        
        Args:
            country_code (str): The bidding zone (e.g., 'NL', 'DE_LU', 'FR').
            start_time (pd.Timestamp): The start timestamp (timezone aware).
            end_time (pd.Timestamp): The end timestamp (timezone aware).
            
        Returns:
            pd.DataFrame: A pandas Series or DataFrame containing the prices, with a datetime index.
        """
        logger.info(f"Fetching Day-Ahead prices for {country_code} from {start_time} to {end_time}")
        
        try:
            # The client returns a pandas Series for prices, we convert to DataFrame for consistency
            data = self.client.query_day_ahead_prices(country_code, start=start_time, end=end_time)
            
            if isinstance(data, pd.Series):
                data = data.to_frame(name='DayAheadPrice')
                
            logger.info(f"Successfully fetched {len(data)} records for {country_code}.")
            return data

        except NoMatchingDataError:
            # If ENTSO-E explicitly says no data is available for this window, we do not retry.
            # We log it and return an empty DataFrame to avoid pipeline crashes.
            logger.warning(f"No matching data found for {country_code} in the specified time range.")
            return pd.DataFrame()

        except RequestException:
            # Re-raise so tenacity's @retry decorator can intercept this and retry.
            # Do NOT wrap it here — the decorator handles the retry logic.
            raise

        except Exception as e:
            logger.error(f"Failed to fetch data for {country_code} after retries. Error: {str(e)}")
            raise EntsoeAPIError(f"ENTSO-E API failure: {str(e)}") from e
