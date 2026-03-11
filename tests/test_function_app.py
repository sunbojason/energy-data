import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, ANY
from datetime import datetime
import azure.functions as func

# Import the function from your project root
# Ensure shared_logic and function_app are in the python path (via conftest.py or python -m pytest)
from function_app import timer_trigger_entsoe_ingestion

@pytest.fixture
def mock_timer():
    """Mocks the Azure TimerRequest object."""
    timer = MagicMock(spec=func.TimerRequest)
    timer.past_due = False
    return timer

@patch("function_app.EntsoeDataClient")
@patch("function_app.BlobServiceClient")
@patch("os.environ.get")
def test_timer_trigger_ingestion_success(mock_env_get, mock_blob_service_class, mock_entsoe_client_class, mock_timer):
    """
    Test a successful ingestion flow:
    API returns data -> Storage account is reachable -> File is uploaded.
    """
    # 1. Mock Environment Variables
    def env_side_effect(key, default=None):
        vars = {
            "STORAGE_ACCOUNT_NAME": "test_energy_storage",
            "RAW_DATA_CONTAINER": "raw-data"
        }
        return vars.get(key, default)
    mock_env_get.side_effect = env_side_effect

    # 2. Mock ENTSO-E Client behavior
    mock_client_instance = mock_entsoe_client_class.return_value
    # Create a dummy DataFrame representing Dutch electricity prices
    mock_df = pd.DataFrame(
        {"DayAheadPrice": [45.0, 48.5]}, 
        index=pd.date_range("2026-03-10", periods=2, freq="h", tz="Europe/Amsterdam")
    )
    mock_client_instance.fetch_day_ahead_prices.return_value = mock_df

    # 3. Mock Blob Storage hierarchy (Client -> Container -> Blob)
    mock_blob_service_instance = mock_blob_service_class.return_value
    mock_blob_client = MagicMock()
    mock_blob_service_instance.get_blob_client.return_value = mock_blob_client

    # 4. Run the function
    timer_trigger_entsoe_ingestion(mock_timer)

    # 5. Assertions
    # Check if the API was called with the correct logic
    mock_client_instance.fetch_day_ahead_prices.assert_called_once()
    
    # Check if BlobServiceClient was initialized with the correct URL
    mock_blob_service_class.assert_called_once_with(
        account_url="https://test_energy_storage.blob.core.windows.net",
        credential=ANY
    )

    # Check if upload_blob was called
    mock_blob_client.upload_blob.assert_called_once()
    
    # Verify the content: Ensure the CSV header exists in the uploaded string
    call_args = mock_blob_client.upload_blob.call_args
    uploaded_content = call_args[0][0]
    assert "DayAheadPrice" in uploaded_content
    assert "2026-03-10" in uploaded_content

@patch("function_app.EntsoeDataClient")
@patch("os.environ.get")
def test_timer_trigger_ingestion_no_data(mock_env_get, mock_entsoe_client_class, mock_timer):
    """
    Test the scenario where API returns no data. 
    The function should exit gracefully without attempting to upload.
    """
    mock_env_get.return_value = "test_storage"
    
    # Mock an empty DataFrame response
    mock_client_instance = mock_entsoe_client_class.return_value
    mock_client_instance.fetch_day_ahead_prices.return_value = pd.DataFrame()

    with patch("function_app.BlobServiceClient") as mock_blob_service_class:
        timer_trigger_entsoe_ingestion(mock_timer)
        
        # Ingestion should stop before calling Blob Storage
        mock_blob_service_class.assert_not_called()

@patch("function_app.EntsoeDataClient")
@patch("function_app.BlobServiceClient")
@patch("os.environ.get")
def test_timer_trigger_ingestion_storage_error(mock_env_get, mock_blob_service_class, mock_entsoe_client_class, mock_timer):
    """
    Test handling of Azure Storage failures. 
    The function should catch the error and log it, preventing a complete crash.
    """
    mock_env_get.return_value = "test_storage"
    
    # API works fine
    mock_entsoe_instance = mock_entsoe_client_class.return_value
    mock_entsoe_instance.fetch_day_ahead_prices.return_value = pd.DataFrame({"p": [1.0]})

    # Mock an upload failure (e.g., Auth error or Network timeout)
    mock_blob_client = MagicMock()
    mock_blob_client.upload_blob.side_effect = Exception("Managed Identity Auth Failed")
    mock_blob_service_class.return_value.get_blob_client.return_value = mock_blob_client

    # Execute - should not raise exception up (caught by try-except in function_app)
    timer_trigger_entsoe_ingestion(mock_timer)
    
    # Ensure it reached the upload step before failing
    mock_blob_client.upload_blob.assert_called_once()