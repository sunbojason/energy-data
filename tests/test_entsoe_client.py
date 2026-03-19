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
def set_testing_api_key(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_KEY", "dummy_test_key_123")

@pytest.fixture
def mock_entsoe_api():
    with patch("shared_logic.entsoe_client.EntsoePandasClient") as mock_class:
        yield mock_class.return_value

@pytest.fixture
def entsoe_client(mock_entsoe_api):
    return EntsoeDataClient()

# --- Unit Tests: Alignment & Resilience ---

def test_hourly_to_quarter_hourly_upsampling(entsoe_client):
    """
    Business Logic: Raw client alignment should be 'pure' (asfreq).
    """
    hourly_index = pd.date_range(start='2026-03-11 00:00', periods=2, freq='h', tz=DEFAULT_TIMEZONE)
    df_hourly = pd.DataFrame({'price': [50.0, 60.0]}, index=hourly_index)
    
    df_aligned = entsoe_client._align_and_flatten(df_hourly, "DA")
    
    assert len(df_aligned) == 5 
    assert pd.isna(df_aligned.iloc[1]['DA_price'])

def test_legacy_imbalance_column_renaming(entsoe_client):
    """
    Business Logic: ENTSO-E sometimes returns 'imbalance volume' and 'imbalance volume.1' 
    which represent Long and Short positions. We map these to descriptive names.
    """
    df_raw = pd.DataFrame({
        'imbalance volume': [100.0],
        'imbalance volume.1': [200.0]
    }, index=[pd.Timestamp('2026-03-11 00:00', tz=DEFAULT_TIMEZONE)])
    
    df_cleaned = entsoe_client._align_and_flatten(df_raw, "Imb")
    
    assert "Imb_imbalance_short" in df_cleaned.columns
    assert "Imb_imbalance_long" in df_cleaned.columns

def test_comprehensive_fetch_sql_ready_formatting(entsoe_client, mock_entsoe_api):
    """
    Business Logic: Ensure the final output is sanitized for SQL injection (no special chars)
    and uses a strictly monotonic 'Time_UTC' column instead of a complex index.
    """
    tz = DEFAULT_TIMEZONE
    start, end = pd.Timestamp('2026-03-11 00:00', tz=tz), pd.Timestamp('2026-03-11 01:00', tz=tz)
    idx_15min = pd.date_range(start=start, end=end, freq=DEFAULT_FREQ_GRID, tz=tz, inclusive='left')
    idx_hourly = pd.date_range(start=start, end=end, freq='h', tz=tz, inclusive='left')

    mock_entsoe_api.query_day_ahead_prices.return_value = pd.Series([50.0], index=idx_hourly)
    # Mocking Load_and_Forecast instead of separate Load
    mock_entsoe_api.query_load_and_forecast.return_value = pd.DataFrame({
        'Actual Load': [10000.0] * 4, 'Forecasted Load': [10500.0] * 4
    }, index=idx_15min)
    
    mock_entsoe_api.query_imbalance_volumes.return_value = pd.DataFrame({
        'imbalance volume': [100.0] * 4, 'imbalance volume.1': [50.0] * 4
    }, index=idx_15min)
    mock_entsoe_api.query_crossborder_flows.return_value = pd.Series([500.0] * 4, index=idx_15min)
    mock_entsoe_api.query_net_position.return_value = pd.Series([100.0] * 4, index=idx_15min, name='net_position')
    mock_entsoe_api.query_generation.return_value = pd.DataFrame([[1000.0]]*4, index=idx_15min, 
                                                            columns=pd.MultiIndex.from_tuples([('Nuclear', 'Actual Aggregated')]))

    result_df = entsoe_client.fetch_comprehensive_market_data(start, end)

    assert 'Time_UTC' in result_df.columns
    assert not isinstance(result_df.index, pd.DatetimeIndex)
    assert 'DA_Price' in result_df.columns
    # With new naming logic, Load_Actual_Load becomes Load_Actual
    assert 'Load_Actual' in result_df.columns

@patch("tenacity.nap.time.sleep", return_value=None)
def test_api_retry_exhaustion_on_network_failure(mock_sleep, entsoe_client, mock_entsoe_api):
    """
    Business Logic: ENTSO-E API can be flappy. We retry up to 3 times before raising a 
    clean EntsoeAPIError for the orchestrator to handle.
    """
    mock_entsoe_api.query_day_ahead_prices.side_effect = RequestException("Timeout")
    start, end = pd.Timestamp('2026-03-11 00:00', tz='UTC'), pd.Timestamp('2026-03-11 01:00', tz='UTC')

    with pytest.raises(EntsoeAPIError):
        entsoe_client.fetch_comprehensive_market_data(start, end)
    
    assert mock_entsoe_api.query_day_ahead_prices.call_count == 3

def test_graceful_handling_of_no_matching_data(entsoe_client, mock_entsoe_api):
    mock_entsoe_api.query_day_ahead_prices.side_effect = NoMatchingDataError("Empty")
    start, end = pd.Timestamp('2026-03-11 00:00', tz='UTC'), pd.Timestamp('2026-03-11 01:00', tz='UTC')
    result_df = entsoe_client.fetch_comprehensive_market_data(start, end)
    assert len(result_df) == 4

class TestExtendedMarketData:
    def test_multiindex_flattening_for_sql_compatibility(self, entsoe_client, mock_entsoe_api):
        start = pd.Timestamp('2026-03-11 00:00', tz='UTC')
        idx = pd.date_range(start=start, periods=4, freq='15min', tz='UTC')
        gen_df = pd.DataFrame([[1000.0, 200.0]] * 4, index=idx, 
                             columns=pd.MultiIndex.from_tuples([('Nuclear', 'Actual'), ('Solar', 'Actual')]))
        
        mock_entsoe_api.query_generation.return_value = gen_df
        # Mock other calls to prevent EntsoeAPIError from missing mocks
        for attr in ['query_net_position', 'query_load_and_forecast', 'query_generation_forecast', 'query_generation_per_plant', 'query_day_ahead_prices']:
            getattr(mock_entsoe_api, attr).return_value = pd.DataFrame()

        result = entsoe_client.fetch_extended_market_data(start, start + pd.Timedelta(minutes=60))

        assert 'Gen_Nuclear_Actual' in result.columns

    def test_structural_resilience_to_stack_errors(self, entsoe_client, caplog):
        def mock_failing_call(*args, **kwargs):
            raise ValueError("duplicate values are not supported in stack")
        
        result_df = entsoe_client._safe_query(mock_failing_call, 'BE')
        assert result_df.empty
        assert "Structural corruption" in caplog.text