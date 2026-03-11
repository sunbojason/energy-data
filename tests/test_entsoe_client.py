import os
import sys
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from requests.exceptions import RequestException
from entsoe.exceptions import NoMatchingDataError
from tenacity import wait_none

# Assuming the client class is saved in shared_logic/entsoe_client.py
from shared_logic.entsoe_client import EntsoeDataClient, EntsoeAPIError

@pytest.fixture
def mock_env_key(monkeypatch):
    """
    Mock the environment variable to ensure the client initializes correctly
    without needing a real API key in the testing environment.
    """
    monkeypatch.setenv("ENTSOE_API_KEY", "dummy_test_key_123")

@pytest.fixture
def entsoe_client(mock_env_key):
    """
    Fixture to provide an initialized EntsoeDataClient instance.
    """
    return EntsoeDataClient()

@patch("shared_logic.entsoe_client.EntsoePandasClient")
def test_fetch_day_ahead_prices_success(mock_entsoe_pandas_client_class, entsoe_client):
    """
    Test successful data retrieval. It verifies that a pandas Series returned 
    by the API is correctly transformed into a formatted DataFrame.
    """
    # 1. Setup mock data with Amsterdam timezone
    tz = 'Europe/Amsterdam'
    mock_timestamps = pd.date_range(start='2026-03-11 00:00', periods=3, freq='h', tz=tz)
    mock_prices = [45.5, 42.1, 39.8]
    
    # ENTSO-E client typically returns a pandas Series for prices
    mock_series = pd.Series(data=mock_prices, index=mock_timestamps)
    
    # Configure the mock instance
    mock_instance = mock_entsoe_pandas_client_class.return_value
    mock_instance.query_day_ahead_prices.return_value = mock_series
    
    # Replace the internal client with our mock
    entsoe_client.client = mock_instance

    # 2. Execute
    start_time = pd.Timestamp('2026-03-11 00:00', tz=tz)
    end_time = pd.Timestamp('2026-03-11 03:00', tz=tz)
    
    result_df = entsoe_client.fetch_day_ahead_prices(
        country_code='NL', 
        start_time=start_time, 
        end_time=end_time
    )

    # 3. Assert
    assert isinstance(result_df, pd.DataFrame), "Output must be converted to a DataFrame."
    assert 'DayAheadPrice' in result_df.columns, "Column should be renamed correctly."
    assert len(result_df) == 3, "Should contain exactly 3 records."
    assert str(result_df.index.tz) == tz, "Timezone must be preserved."
    
    # Verify the mock was called with correct parameters
    mock_instance.query_day_ahead_prices.assert_called_once_with(
        'NL', start=start_time, end=end_time
    )

@patch("shared_logic.entsoe_client.EntsoePandasClient")
def test_fetch_day_ahead_prices_no_matching_data(mock_entsoe_pandas_client_class, entsoe_client):
    """
    Test handling of NoMatchingDataError. The system should absorb this gracefully
    and return an empty DataFrame without crashing the pipeline.
    """
    # 1. Setup mock to raise NoMatchingDataError
    mock_instance = mock_entsoe_pandas_client_class.return_value
    mock_instance.query_day_ahead_prices.side_effect = NoMatchingDataError("No data found")
    entsoe_client.client = mock_instance

    # 2. Execute
    start_time = pd.Timestamp('2026-03-11 00:00', tz='Europe/Amsterdam')
    end_time = pd.Timestamp('2026-03-11 01:00', tz='Europe/Amsterdam')
    
    result_df = entsoe_client.fetch_day_ahead_prices('NL', start_time, end_time)

    # 3. Assert
    assert isinstance(result_df, pd.DataFrame), "Must return a DataFrame even on missing data."
    assert result_df.empty, "DataFrame should be empty when no matching data is found."

@patch("shared_logic.entsoe_client.EntsoePandasClient")
def test_fetch_day_ahead_prices_network_failure(mock_entsoe_pandas_client_class, entsoe_client):
    """
    Test that a persistent network exception eventually raises our custom EntsoeAPIError
    after tenacity exhausts its retry attempts.
    """
    # 1. Setup mock to raise a RequestException (Network error)
    mock_instance = mock_entsoe_pandas_client_class.return_value
    mock_instance.query_day_ahead_prices.side_effect = RequestException("Connection timeout")
    entsoe_client.client = mock_instance

    # Disable tenacity's sleep so retries are instantaneous in tests
    entsoe_client.fetch_day_ahead_prices.retry.wait = wait_none()

    # 2. Execute and Assert
    start_time = pd.Timestamp('2026-03-11 00:00', tz='Europe/Amsterdam')
    end_time = pd.Timestamp('2026-03-11 01:00', tz='Europe/Amsterdam')
    
    # We expect our custom EntsoeAPIError to be raised after retries fail
    with pytest.raises(EntsoeAPIError) as exc_info:
        entsoe_client.fetch_day_ahead_prices('NL', start_time, end_time)
        
    assert "ENTSO-E API failure" in str(exc_info.value)
    # The underlying tenacity retry mechanism should have attempted the call multiple times.
    # We can check that the mock was called more than once.
    assert mock_instance.query_day_ahead_prices.call_count > 1, "Should have retried multiple times before failing."