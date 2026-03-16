import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, ANY
import logging
import function_app

from shared_logic.constants import DEFAULT_COUNTRY, DEFAULT_FREQ_GRID, DEFAULT_TIMEZONE


@pytest.fixture
def mock_timer():
    """Mocks the Azure TimerRequest object."""
    timer = MagicMock()
    timer.past_due = False
    return timer

@patch("function_app.blob_service_client")
@patch("function_app.storage_account_name", "test_quant_storage")
@patch("function_app.EntsoeDataClient")
def test_timer_trigger_ingestion_success(mock_entsoe_client_class, mock_global_blob_service, mock_timer, caplog):
    """
    Test a successful ingestion flow for comprehensive market data.
    """
    caplog.set_level(logging.INFO)
    
    mock_client_instance = mock_entsoe_client_class.return_value
    
    # UPDATED: Use 15-min frequency (96 rows/day) to satisfy the expected_min_rows = 92 check.
    # UPDATED: Use column names that reflect the real EntsoeDataClient output.
    mock_df = pd.DataFrame(
        {"DA_Price_0": [45.0] * 96, "Load_Actual_0": [10000.0] * 96}, 
        index=pd.date_range("2026-03-15", periods=96, freq=DEFAULT_FREQ_GRID, tz=DEFAULT_TIMEZONE)
    )
    mock_client_instance.fetch_comprehensive_market_data.return_value = mock_df

    mock_blob_client = MagicMock()
    mock_global_blob_service.get_blob_client.return_value = mock_blob_client

    # Directly call the function without repetitive imports
    function_app.timer_trigger_entsoe_ingestion(mock_timer)

    # UPDATED: Verify the new keyword arguments are passed correctly
    mock_client_instance.fetch_comprehensive_market_data.assert_called_once_with(
        start_time=ANY,
        end_time=ANY,
        target_country=DEFAULT_COUNTRY
    )
    
    mock_global_blob_service.get_blob_client.assert_called_once_with(
        container="raw-data", 
        blob=ANY
    )
    mock_blob_client.upload_blob.assert_called_once()
    
    # UPDATED: uploaded_content is now bytes (utf-8 encoded), decode before asserting
    uploaded_content_bytes = mock_blob_client.upload_blob.call_args[0][0]
    uploaded_content_str = uploaded_content_bytes.decode('utf-8')
    
    assert "DA_Price_0" in uploaded_content_str
    assert "Load_Actual_0" in uploaded_content_str
    assert "INGESTION SUCCESS" in caplog.text


@patch("function_app.blob_service_client")
@patch("function_app.storage_account_name", "test_quant_storage")
@patch("function_app.EntsoeDataClient")
def test_timer_trigger_ingestion_no_data(mock_entsoe_client_class, mock_global_blob_service, mock_timer, caplog):
    """
    Test scenario where the API returns an empty DataFrame.
    """
    mock_client_instance = mock_entsoe_client_class.return_value
    mock_client_instance.fetch_comprehensive_market_data.return_value = pd.DataFrame()

    # Clean call
    function_app.timer_trigger_entsoe_ingestion(mock_timer)
    
    mock_global_blob_service.get_blob_client.assert_not_called()
    assert "DATA GAP" in caplog.text


@patch("function_app.blob_service_client")
@patch("function_app.storage_account_name", "test_quant_storage")
@patch("function_app.EntsoeDataClient")
def test_timer_trigger_ingestion_storage_error(mock_entsoe_client_class, mock_global_blob_service, mock_timer, caplog):
    """
    Test resilience against Azure Storage failures.
    """
    mock_client_instance = mock_entsoe_client_class.return_value
    
    # UPDATED: Generate 96 rows to cleanly bypass the integrity alert and purely test the storage error
    mock_df = pd.DataFrame(
        {"DA_Price_0": [1.0] * 96},
        index=pd.date_range("2026-03-15", periods=96, freq=DEFAULT_FREQ_GRID, tz=DEFAULT_TIMEZONE)
    )
    mock_client_instance.fetch_comprehensive_market_data.return_value = mock_df

    mock_blob_client = MagicMock()
    mock_blob_client.upload_blob.side_effect = Exception("Azure RBAC Identity Authorization Failed")
    mock_global_blob_service.get_blob_client.return_value = mock_blob_client

    # Clean call with context manager
    with pytest.raises(Exception, match="Azure RBAC Identity Authorization Failed"):
        function_app.timer_trigger_entsoe_ingestion(mock_timer)
    
    assert "PIPELINE FAILURE" in caplog.text


@patch("function_app.blob_service_client", None)
@patch("function_app.storage_account_name", None)
def test_timer_trigger_missing_configuration(mock_timer, caplog):
    """
    Test the global configuration safety check.
    """
    # Clean call
    function_app.timer_trigger_entsoe_ingestion(mock_timer)
    assert "CRITICAL: Storage configuration is missing" in caplog.text