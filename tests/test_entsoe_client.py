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
from shared_logic.constants import DEFAULT_FREQ_GRID, DEFAULT_TIMEZONE


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
    tz = DEFAULT_TIMEZONE
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
    }, index=[pd.Timestamp('2026-03-11 00:00', tz=DEFAULT_TIMEZONE)])
    
    df_cleaned = entsoe_client._align_and_flatten(df_raw, "Imb")
    
    assert "Imb_imbalance_short" in df_cleaned.columns
    assert "Imb_imbalance_long" in df_cleaned.columns
    assert df_cleaned["Imb_imbalance_long"].iloc[0] == 200.0


# --- Aggregator Orchestration Tests ---

def test_fetch_comprehensive_market_data_success(entsoe_client, mock_entsoe_pandas_client):
    """
    Test the full data pipeline. 
    Verifies that the final structure is properly formatted for SQL (no _0, reset index).
    """
    tz = DEFAULT_TIMEZONE
    start_time = pd.Timestamp('2026-03-11 00:00', tz=tz)
    end_time = pd.Timestamp('2026-03-11 01:00', tz=tz)
    
    # Prepare standard indexes
    idx_15min = pd.date_range(start=start_time, end=end_time, freq=DEFAULT_FREQ_GRID, tz=tz)
    idx_hourly = pd.date_range(start=start_time, end=end_time, freq='h', tz=tz)

    # 1. Mock Day-Ahead Prices (Hourly Series)
    mock_entsoe_pandas_client.query_day_ahead_prices.return_value = pd.Series([50.0, 55.0], index=idx_hourly)
    
    # 2. Mock Actual Load (15-min Series)
    mock_entsoe_pandas_client.query_load.return_value = pd.Series([10000.0] * 5, index=idx_15min)
    
    # 3. Mock Imbalance Volumes
    mock_entsoe_pandas_client.query_imbalance_volumes.return_value = pd.DataFrame({
        'imbalance volume': [100.0] * 5,
        'imbalance volume.1': [50.0] * 5
    }, index=idx_15min)

    # 4. Mock Cross-Border Flows
    mock_flow_data = pd.Series([500.0] * 5, index=idx_15min)
    mock_entsoe_pandas_client.query_crossborder_flows.return_value = mock_flow_data
    
    # Prevent warnings for optional features
    mock_entsoe_pandas_client.query_load_forecast.return_value = pd.DataFrame()
    mock_entsoe_pandas_client.query_current_balancing_state.return_value = pd.DataFrame()
    mock_entsoe_pandas_client.query_contracted_reserve_prices.return_value = pd.DataFrame()

    # Execute
    result_df = entsoe_client.fetch_comprehensive_market_data(start_time, end_time)

    # --- UPDATED ASSERTIONS FOR CLEAN SCHEMA ---
    
    # 1. Verify Timestamp Column (Index must be reset)
    assert 'Time_UTC' in result_df.columns
    assert 'timestamp' not in result_df.columns
    # Verify that the DatetimeIndex has been promoted to a regular column
    assert not isinstance(result_df.index, pd.DatetimeIndex)
    # Verify the data type of the new column is indeed datetime-like
    assert pd.api.types.is_datetime64_any_dtype(result_df['Time_UTC'])

    # 2. Verify CLEAN Column Names (Stripping _0)
    # entsoe-py usually adds _0 when a Series is converted to DataFrame during join.
    # Our finalize_dataframe_structure must strip it.
    assert 'DA_Price' in result_df.columns
    assert 'DA_Price_0' not in result_df.columns
    assert 'Load_Actual' in result_df.columns
    assert 'Load_Actual_0' not in result_df.columns
    
    # 3. Verify Data Integrity
    assert result_df['DA_Price'].iloc[1] == 50.0
    assert 'Export_Sum' in result_df.columns

@patch("tenacity.nap.time.sleep", return_value=None)
def test_fetch_comprehensive_network_resilience(mock_sleep, entsoe_client, mock_entsoe_pandas_client):
    """
    Verify that persistent network errors exhaust retries and raise EntsoeAPIError.
    """
    tz = DEFAULT_TIMEZONE
    start = pd.Timestamp('2026-03-11 00:00', tz=tz)
    end = pd.Timestamp('2026-03-11 01:00', tz=tz)
    
    mock_entsoe_pandas_client.query_day_ahead_prices.side_effect = RequestException("Timeout")

    with pytest.raises(EntsoeAPIError):
        entsoe_client.fetch_comprehensive_market_data(start, end)
    
    assert mock_entsoe_pandas_client.query_day_ahead_prices.call_count == 3

def test_fetch_comprehensive_empty_scenario(entsoe_client, mock_entsoe_pandas_client):
    """
    Verify that if all API calls fail, the master grid index is still returned 
    in a SQL-ready format (reset index with timestamp column).
    """
    tz = DEFAULT_TIMEZONE
    start = pd.Timestamp('2026-03-11 00:00', tz=tz)
    end = pd.Timestamp('2026-03-11 01:00', tz=tz)
    
    mock_entsoe_pandas_client.query_day_ahead_prices.side_effect = NoMatchingDataError("Empty")
    mock_entsoe_pandas_client.query_load.side_effect = NoMatchingDataError("Empty")
    mock_entsoe_pandas_client.query_imbalance_volumes.side_effect = NoMatchingDataError("Empty")
    mock_entsoe_pandas_client.query_crossborder_flows.side_effect = NoMatchingDataError("Empty")

    result_df = entsoe_client.fetch_comprehensive_market_data(start, end)
    
    # Assertions for empty but structured output
    assert len(result_df) == 5
    assert 'Time_UTC' in result_df.columns
    assert 'timestamp' not in result_df.columns
    # Verify that the DatetimeIndex has been promoted to a regular column
    assert not isinstance(result_df.index, pd.DatetimeIndex)
    # Verify the data type of the new column is indeed datetime-like
    assert pd.api.types.is_datetime64_any_dtype(result_df['Time_UTC'])

    assert result_df['Export_Sum'].sum() == 0.0


# --- New Structural Formatting Tests ---

def test_finalize_dataframe_structure_multiindex(entsoe_client):
    """
    Verify that MultiIndex columns are flattened.
    """
    col_tuples = [('Down', 1, 'Activated'), ('Up', 2, '')]
    multi_columns = pd.MultiIndex.from_tuples(col_tuples)
    time_index = pd.date_range(start='2026-03-18', periods=2, freq='h', tz='UTC')
    
    df_mock = pd.DataFrame(data=[[10.0, 20.0], [15.0, 25.0]], index=time_index, columns=multi_columns)
    df_result = entsoe_client.finalize_dataframe_structure(df_mock)
    
    assert 'Down_1_Activated' in df_result.columns
    assert 'Up_2' in df_result.columns
    assert not any(isinstance(col, tuple) for col in df_result.columns)

def test_finalize_dataframe_structure_suffix_removal(entsoe_client):
    """
    Verify stripping of '_0' suffixes.
    """
    df_mock = pd.DataFrame({
        'DA_Price_0': [50.0],
        'Load_Actual_0': [1000.0],
        'Export_BE': [200.0] 
    })
    
    df_result = entsoe_client.finalize_dataframe_structure(df_mock)
    
    assert 'DA_Price' in df_result.columns
    assert 'Load_Actual' in df_result.columns
    assert 'DA_Price_0' not in df_result.columns

def test_safe_query_graceful_degradation(entsoe_client, caplog):
    """
    Verify handling of ENTSO-E structural corruption (stack error).
    """
    def mock_failing_api_call(*args, **kwargs):
        raise ValueError("Columns with duplicate values are not supported in stack")
    
    result_df = entsoe_client._safe_query(mock_failing_api_call, 'BE')
    
    assert result_df.empty
    assert "DATA QUALITY ALERT" in caplog.text