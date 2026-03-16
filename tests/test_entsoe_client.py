import os
import pytest
import pandas as pd
import numpy as np
from typing import cast
from unittest.mock import patch, MagicMock
from requests.exceptions import RequestException
from entsoe.exceptions import NoMatchingDataError

# Importing custom client and exception
from shared_logic.entsoe_client import EntsoeDataClient, EntsoeAPIError

# --- Fixtures ---

@pytest.fixture(autouse=True)
def mock_env_key(monkeypatch):
    """
    Ensure a dummy API key is set for all tests to prevent actual network calls.
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
    Provides an instance of EntsoeDataClient with mocked backend.
    """
    return EntsoeDataClient()


# --- Unit Tests for Internal Helper Logic ---

def test_align_and_flatten_resampling(entsoe_client):
    """
    Validate that 1-hour frequency data is correctly upsampled to the 15-min grid.
    """
    tz = 'Europe/Amsterdam'
    idx = pd.date_range(start='2026-03-11 00:00', periods=2, freq='h', tz=tz)
    df_hourly = pd.DataFrame({'price': [50.0, 60.0]}, index=idx)
    
    df_aligned = entsoe_client._align_and_flatten(df_hourly, "DA")
    
    # Assertions: 1 hour span @ 15-min results in 5 timestamps (00, 15, 30, 45, 60)
    assert len(df_aligned) == 5 
    assert df_aligned.columns[0] == "DA_price"
    assert df_aligned.iloc[1]['DA_price'] == 50.0

def test_align_and_flatten_duplicate_resolution(entsoe_client):
    """
    Verify the logic for resolving duplicate 'imbalance volume' columns.
    """
    df_raw = pd.DataFrame({
        'imbalance volume': [100.0],
        'imbalance volume.1': [200.0]
    }, index=[pd.Timestamp('2026-03-11 00:00', tz='Europe/Amsterdam')])
    
    df_cleaned = entsoe_client._align_and_flatten(df_raw, "Imb")
    
    assert "Imb_imbalance_short" in df_cleaned.columns
    assert "Imb_imbalance_long" in df_cleaned.columns
    assert df_cleaned["Imb_imbalance_long"].iloc[0] == 200.0


# --- Aggregator Orchestration Tests ---

def test_fetch_comprehensive_market_data_success(entsoe_client, mock_entsoe_pandas_client):
    """
    Test the full data pipeline: fetching, flattening, and joining into a 15-min grid.
    """
    tz = 'Europe/Amsterdam'
    start_time = pd.Timestamp('2026-03-11 00:00', tz=tz)
    end_time = pd.Timestamp('2026-03-11 01:00', tz=tz)
    
    # Prepare standard indexes
    idx_15min = pd.date_range(start=start_time, end=end_time, freq='15min', tz=tz)
    idx_hourly = pd.date_range(start=start_time, end=end_time, freq='h', tz=tz)

    # 1. Mock Day-Ahead Prices (Hourly Series)
    mock_entsoe_pandas_client.query_day_ahead_prices.return_value = pd.Series([50.0, 55.0], index=idx_hourly)
    
    # 2. Mock Actual Load (15-min Series)
    mock_entsoe_pandas_client.query_load.return_value = pd.Series([10000.0] * 5, index=idx_15min)
    
    # 3. Mock Imbalance Volumes (Requires 2 columns to trigger renaming)
    mock_entsoe_pandas_client.query_imbalance_volumes.return_value = pd.DataFrame({
        'imbalance volume': [100.0] * 5,
        'imbalance volume.1': [50.0] * 5
    }, index=idx_15min)

    # 4. Mock Cross-Border Flows
    # We provide a generic return value to satisfy the NL neighbors loop (BE, DE_LU, etc.)
    mock_flow_data = pd.Series([500.0] * 5, index=idx_15min)
    mock_entsoe_pandas_client.query_crossborder_flows.return_value = mock_flow_data
    
    # 5. Prevent warnings for optional features (Forecasts, Balancing State)
    mock_entsoe_pandas_client.query_load_forecast.return_value = pd.DataFrame()
    mock_entsoe_pandas_client.query_current_balancing_state.return_value = pd.DataFrame()
    mock_entsoe_pandas_client.query_contracted_reserve_prices.return_value = pd.DataFrame()

    # Execute
    result_df = entsoe_client.fetch_comprehensive_market_data(start_time, end_time)

    # Assertions
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 5
    
    # Type narrowing for Pylance: assert the index is a DatetimeIndex
    assert isinstance(result_df.index, pd.DatetimeIndex)
    dt_index = cast(pd.DatetimeIndex, result_df.index)
    assert dt_index.freqstr in ['15T', '15min']

    # Verify column naming and prefixes (Series conversion to DataFrame defaults to col 0)
    assert 'DA_Price_0' in result_df.columns 
    assert 'Imb_imbalance_short' in result_df.columns
    assert 'Imb_imbalance_long' in result_df.columns
    
    # Verify temporal alignment and forward-filling
    assert result_df['DA_Price_0'].iloc[1] == 50.0 # Carried from 00:00
    
    # Verify aggregation (Neighbors were correctly joined and summed)
    assert 'Export_Sum' in result_df.columns
    assert result_df['Export_Sum'].iloc[0] > 0

@patch("tenacity.nap.time.sleep", return_value=None)
def test_fetch_comprehensive_network_resilience(mock_sleep, entsoe_client, mock_entsoe_pandas_client):
    """
    Verify that persistent network errors exhaust retries and raise EntsoeAPIError.
    """
    tz = 'Europe/Amsterdam'
    start = pd.Timestamp('2026-03-11 00:00', tz=tz)
    end = pd.Timestamp('2026-03-11 01:00', tz=tz)
    
    mock_entsoe_pandas_client.query_day_ahead_prices.side_effect = RequestException("Timeout")

    with pytest.raises(EntsoeAPIError):
        entsoe_client.fetch_comprehensive_market_data(start, end)
    
    assert mock_entsoe_pandas_client.query_day_ahead_prices.call_count == 3

def test_fetch_comprehensive_empty_scenario(entsoe_client, mock_entsoe_pandas_client):
    """
    Verify that if all API calls fail, the master grid index is still returned.
    """
    tz = 'Europe/Amsterdam'
    start = pd.Timestamp('2026-03-11 00:00', tz=tz)
    end = pd.Timestamp('2026-03-11 01:00', tz=tz)
    
    # Mock all queries to return NoMatchingDataError
    mock_entsoe_pandas_client.query_day_ahead_prices.side_effect = NoMatchingDataError("Empty")
    mock_entsoe_pandas_client.query_load.side_effect = NoMatchingDataError("Empty")
    mock_entsoe_pandas_client.query_imbalance_volumes.side_effect = NoMatchingDataError("Empty")
    mock_entsoe_pandas_client.query_crossborder_flows.side_effect = NoMatchingDataError("Empty")

    result_df = entsoe_client.fetch_comprehensive_market_data(start, end)
    
    # Should still return 5 timestamps for the hour on a 15-min grid
    assert len(result_df) == 5
    
    # Aggregates like Export_Sum should be initialized to 0.0 even if no neighbor data found
    assert 'Export_Sum' in result_df.columns
    assert result_df['Export_Sum'].sum() == 0.0