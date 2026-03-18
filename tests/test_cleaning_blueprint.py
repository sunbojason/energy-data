import pytest
import azure.functions as func
from unittest.mock import MagicMock, patch
import logging
from blueprints.cleaning import blob_trigger_cleaning_processor

@pytest.fixture
def mock_blob_input():
    """Mocks the Azure Functions InputStream."""
    blob = MagicMock(spec=func.InputStream)
    blob.name = "raw-data/be_market_data_20260315.csv"
    blob.length = 1024
    return blob

@pytest.fixture
def mock_blob_output():
    """Mocks the Azure Functions Out[str] binding."""
    return MagicMock(spec=func.Out)

@patch("blueprints.cleaning.CleaningService")
def test_cleaning_processor_success(mock_cleaning_service, mock_blob_input, mock_blob_output, caplog):
    """
    Test successful cleaning flow: Raw CSV in -> Cleaned CSV out.
    """
    caplog.set_level(logging.INFO)
    
    # Mock raw input
    raw_content = "timestamp,price\n2026-03-15 00:00:00,45.0"
    mock_blob_input.read.return_value = raw_content.encode('utf-8')
    
    # Mock cleaned output
    cleaned_content = "timestamp,price,status\n2026-03-15 00:00:00,45.0,cleaned"
    mock_cleaning_service.clean_energy_data.return_value = cleaned_content

    # Execute trigger
    blob_trigger_cleaning_processor(mock_blob_input, mock_blob_output)

    # Assertions
    mock_cleaning_service.clean_energy_data.assert_called_once_with(raw_content)
    mock_blob_output.set.assert_called_once_with(cleaned_content)
    assert "be_market_data_20260315.csv" in caplog.text
@patch("blueprints.cleaning.CleaningService")
def test_cleaning_processor_empty_input(mock_cleaning_service, mock_blob_input, mock_blob_output, caplog):
    """
    Test scenario where the incoming blob is empty.
    """
    caplog.set_level(logging.WARNING)
    mock_blob_input.read.return_value = b"" # Empty byte stream

    blob_trigger_cleaning_processor(mock_blob_input, mock_blob_output)

    # Verify no processing or output occurred
    mock_cleaning_service.clean_energy_data.assert_not_called()
    mock_blob_output.set.assert_not_called()
    assert "is empty. Terminating process" in caplog.text

@patch("blueprints.cleaning.CleaningService")
def test_cleaning_processor_skipped(mock_cleaning_service, mock_blob_input, mock_blob_output, caplog):
    """
    Test scenario where cleaning service returns an empty string (data filtered out).
    """
    caplog.set_level(logging.WARNING)
    mock_blob_input.read.return_value = b"some,raw,data"
    
    # Mock empty cleaned data (all rows filtered)
    mock_cleaning_service.clean_energy_data.return_value = ""

    blob_trigger_cleaning_processor(mock_blob_input, mock_blob_output)

    mock_blob_output.set.assert_not_called()
    assert "CLEANING SKIPPED" in caplog.text

@patch("blueprints.cleaning.CleaningService")
def test_cleaning_processor_exception(mock_cleaning_service, mock_blob_input, mock_blob_output, caplog):
    """
    Test error handling when CleaningService raises an exception.
    """
    caplog.set_level(logging.ERROR)
    mock_blob_input.read.return_value = b"corrupted,data"
    
    # Mock cleaning failure
    mock_cleaning_service.clean_energy_data.side_effect = Exception("Pandas Transformation Error")

    # Exception is caught to prevent poison queues
    blob_trigger_cleaning_processor(mock_blob_input, mock_blob_output)

    assert "CLEANING FAILURE" in caplog.text
    assert "Pandas Transformation Error" in caplog.text
    mock_blob_output.set.assert_not_called()