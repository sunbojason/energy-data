import logging
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, ANY
from datetime import datetime
import azure.functions as func
import function_app

@pytest.fixture
def mock_timer():
    """Mocks the Azure TimerRequest object."""
    timer = MagicMock(spec=func.TimerRequest)
    timer.past_due = False
    return timer

# Patching the global variables directly inside the function_app module
@patch("function_app.blob_service_client")
@patch("function_app.storage_account_name", "test_quant_storage")
@patch("function_app.EntsoeDataClient")
def test_timer_trigger_ingestion_success(mock_entsoe_client_class, mock_global_blob_service, mock_timer, caplog):
    """
    Test a successful ingestion flow for NL Day-Ahead prices.
    Verifies that the global client is used and data is uploaded.
    """
    caplog.set_level(logging.INFO)

    mock_client_instance = mock_entsoe_client_class.return_value
    mock_df = pd.DataFrame(
        {"DayAheadPrice": [45.0] * 24}, 
        index=pd.date_range("2026-03-10", periods=24, freq="h", tz="Europe/Amsterdam")
    )
    mock_client_instance.fetch_day_ahead_prices.return_value = mock_df

    mock_blob_client = MagicMock()
    mock_global_blob_service.get_blob_client.return_value = mock_blob_client

    function_app.timer_trigger_entsoe_ingestion(mock_timer)

    mock_client_instance.fetch_day_ahead_prices.assert_called_once()
    
    # Verify the global blob service was used to get the specific file client
    mock_global_blob_service.get_blob_client.assert_called_once_with(
        container="raw-data", 
        blob=ANY  # Matches the dynamic timestamped filename
    )

    # Verify upload was executed with overwrite=True
    mock_blob_client.upload_blob.assert_called_once()
    
    # Verify the CSV content integrity
    uploaded_content = mock_blob_client.upload_blob.call_args[0][0]
    assert "DayAheadPrice" in uploaded_content
    assert "2026-03-10" in uploaded_content
    assert "INGESTION SUCCESS" in caplog.text


@patch("function_app.blob_service_client")
@patch("function_app.storage_account_name", "test_quant_storage")
@patch("function_app.EntsoeDataClient")
def test_timer_trigger_ingestion_no_data(mock_entsoe_client_class, mock_global_blob_service, mock_timer, caplog):
    """
    Test scenario where ENTSO-E returns an empty DataFrame.
    The function must exit gracefully without writing to storage.
    """
    mock_client_instance = mock_entsoe_client_class.return_value
    mock_client_instance.fetch_day_ahead_prices.return_value = pd.DataFrame()

    function_app.timer_trigger_entsoe_ingestion(mock_timer)
    
    mock_global_blob_service.get_blob_client.assert_not_called()
    assert "DATA GAP" in caplog.text


@patch("function_app.blob_service_client")
@patch("function_app.storage_account_name", "test_quant_storage")
@patch("function_app.EntsoeDataClient")
def test_timer_trigger_ingestion_storage_error(mock_entsoe_client_class, mock_global_blob_service, mock_timer, caplog):
    """
    Test resilience against Azure Storage failures.
    Ensures the error is caught, logged, and re-raised to trigger @app.retry.
    """
    mock_client_instance = mock_entsoe_client_class.return_value
    mock_client_instance.fetch_day_ahead_prices.return_value = pd.DataFrame({"Price": [1.0]})

    mock_blob_client = MagicMock()
    mock_blob_client.upload_blob.side_effect = Exception("Azure RBAC Identity Authorization Failed")
    mock_global_blob_service.get_blob_client.return_value = mock_blob_client

    with pytest.raises(Exception, match="Azure RBAC Identity Authorization Failed"):
        function_app.timer_trigger_entsoe_ingestion(mock_timer)
    
    assert "PIPELINE FAILURE" in caplog.text


@patch("function_app.blob_service_client", None)
@patch("function_app.storage_account_name", None)
def test_timer_trigger_missing_configuration(mock_timer, caplog):
    """
    Test the global configuration safety check.
    If environment variables fail to load, the execution should abort immediately.
    """
    function_app.timer_trigger_entsoe_ingestion(mock_timer)
    
    assert "CRITICAL: Storage configuration is missing" in caplog.text