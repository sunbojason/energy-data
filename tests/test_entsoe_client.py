import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from requests.exceptions import RequestException
from entsoe.exceptions import NoMatchingDataError

# 1. Direct import of the custom exception and client
from shared_logic.entsoe_client import EntsoeDataClient, EntsoeAPIError

# --- Fixtures ---

@pytest.fixture(autouse=True)
def mock_env_key(monkeypatch):
    """
    Automatically mock the API key for ALL tests in this module.
    autouse=True prevents accidental external API calls.
    """
    monkeypatch.setenv("ENTSOE_API_KEY", "dummy_test_key_123")

@pytest.fixture
def mock_entsoe_pandas_client():
    """
    Centralized mock for the underlying ENTSO-E API client.
    """
    with patch("shared_logic.entsoe_client.EntsoePandasClient") as mock_class:
        yield mock_class.return_value

@pytest.fixture
def entsoe_client(mock_entsoe_pandas_client):
    """
    Provides a clean instance of EntsoeDataClient.
    The internal EntsoePandasClient is automatically patched by the fixture above.
    """
    return EntsoeDataClient()


# --- Test Cases ---

def test_fetch_day_ahead_prices_success(entsoe_client, mock_entsoe_pandas_client):
    """
    Verify successful retrieval and transformation of Day-Ahead prices.
    Ensures index integrity and timezone preservation.
    """
    tz = 'Europe/Amsterdam'
    # Use a specific date to test standard behavior
    mock_timestamps = pd.date_range(start='2026-03-11 00:00', periods=3, freq='h', tz=tz)
    mock_prices = [45.5, 42.1, 39.8]
    mock_series = pd.Series(data=mock_prices, index=mock_timestamps)
    
    mock_entsoe_pandas_client.query_day_ahead_prices.return_value = mock_series

    start_time = pd.Timestamp('2026-03-11 00:00', tz=tz)
    end_time = pd.Timestamp('2026-03-11 03:00', tz=tz)
    
    result_df = entsoe_client.fetch_day_ahead_prices('NL', start_time, end_time)

    # Type & Structure Assertions
    assert isinstance(result_df, pd.DataFrame), "Output must be a DataFrame."
    assert 'DayAheadPrice' in result_df.columns, "Target column must be correctly named."
    assert len(result_df) == 3, "Row count mismatch."
    
    # Time-Series Integrity Assertions
    assert isinstance(result_df.index, pd.DatetimeIndex), "Index must be DatetimeIndex."
    assert str(result_df.index.tz) == tz, f"Timezone must strictly remain {tz}."
    
    mock_entsoe_pandas_client.query_day_ahead_prices.assert_called_once_with(
        'NL', start=start_time, end=end_time
    )

def test_fetch_day_ahead_prices_no_data(entsoe_client, mock_entsoe_pandas_client):
    """
    Verify the client absorbs NoMatchingDataError gracefully,
    returning an empty DataFrame to prevent pipeline crashes.
    """
    mock_entsoe_pandas_client.query_day_ahead_prices.side_effect = NoMatchingDataError("API returned nothing")

    start_time = pd.Timestamp('2026-03-11 00:00', tz='Europe/Amsterdam')
    end_time = pd.Timestamp('2026-03-11 01:00', tz='Europe/Amsterdam')
    
    result_df = entsoe_client.fetch_day_ahead_prices('NL', start_time, end_time)

    assert isinstance(result_df, pd.DataFrame), "Must return DataFrame on empty data."
    assert result_df.empty, "DataFrame must be empty."

@patch("tenacity.nap.time.sleep", return_value=None)  # Bypass tenacity sleep delay for fast tests
def test_fetch_day_ahead_prices_network_failure(mock_sleep, entsoe_client, mock_entsoe_pandas_client):
    """
    Verify that persistent network failures exhaust retries 
    and raise the custom EntsoeAPIError.
    """
    mock_entsoe_pandas_client.query_day_ahead_prices.side_effect = RequestException("Connection timeout")

    start_time = pd.Timestamp('2026-03-11 00:00', tz='Europe/Amsterdam')
    end_time = pd.Timestamp('2026-03-11 01:00', tz='Europe/Amsterdam')
    
    with pytest.raises(EntsoeAPIError, match="ENTSO-E API failure"):
        entsoe_client.fetch_day_ahead_prices('NL', start_time, end_time)
        
    # Verify retry logic fired at least more than once
    assert mock_entsoe_pandas_client.query_day_ahead_prices.call_count > 1, "Tenacity retry mechanism failed."


# --- Aggregator Test (Multi-dimensional Market Data) ---

def test_fetch_comprehensive_market_data_success(entsoe_client, mock_entsoe_pandas_client):
    """
    Verify the aggregator correctly fetches prices, load, and cross-border flows,
    and merges them into a single aligned DataFrame.
    """
    tz = 'Europe/Amsterdam'
    idx = pd.date_range(start='2026-03-11 00:00', periods=2, freq='h', tz=tz)
    
    # Mocking different API endpoint responses
    mock_price_series = pd.Series([50.0, 52.0], index=idx)
    mock_load_actual = pd.Series([10000.0, 10500.0], index=idx)
    mock_export_fr = pd.Series([500.0, 400.0], index=idx)
    
    mock_entsoe_pandas_client.query_day_ahead_prices.return_value = mock_price_series
    mock_entsoe_pandas_client.query_load.return_value = mock_load_actual
    # Setup load forecast to throw NoData to test partial failure resilience
    mock_entsoe_pandas_client.query_load_forecast.side_effect = NoMatchingDataError("No forecast")
    mock_entsoe_pandas_client.query_crossborder_flows.return_value = mock_export_fr

    start_time = idx[0]
    end_time = idx[-1]
    
    # Execute the comprehensive fetch
    result_df = entsoe_client.fetch_comprehensive_market_data('NL', start_time, end_time)
    
    # Assertions
    assert not result_df.empty, "Aggregator should return data."
    
    # Check if the merge aligned columns correctly
    expected_columns = ['DayAheadPrice', 'Actual Load', 'FR']
    for col in expected_columns:
        assert col in result_df.columns, f"Missing expected aggregated column: {col}"
        
    # Verify the partial failure didn't crash the merge
    assert 'Forecasted Load' not in result_df.columns, "Forecast should be safely omitted on error."