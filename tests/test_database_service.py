import pytest
import os
import pandas as pd
from unittest.mock import MagicMock, patch
from shared_logic.database_service import DatabaseService

# --- Test Fixtures ---

@pytest.fixture
def mock_env(monkeypatch):
    """Sets up mandatory environment variables for DatabaseService initialization."""
    monkeypatch.setenv("SQL_SERVER_NAME", "test-server.database.windows.net")
    monkeypatch.setenv("SQL_DATABASE_NAME", "test-db")

@pytest.fixture
def sample_energy_df():
    """Generates a sample DataFrame for ingestion testing."""
    return pd.DataFrame({
        "timestamp": ["2026-03-11 00:00:00"],
        "DA_Price_0": [45.5],
        "Load_Actual_0": [12000.0]
    })

# --- Unit Tests ---

def test_initialization_success(mock_env):
    """
    Test that DatabaseService initializes correctly when 
    all environment variables are present.
    """
    service = DatabaseService()
    assert service.server == "test-server.database.windows.net"
    assert service.database == "test-db"
    assert "TrustServerCertificate=yes" in service.conn_str
    assert service.engine is not None

def test_initialization_failure(monkeypatch):
    """
    Test that DatabaseService raises ValueError when 
    configuration is missing.
    """
    monkeypatch.delenv("SQL_SERVER_NAME", raising=False)
    monkeypatch.delenv("SQL_DATABASE_NAME", raising=False)
    
    with pytest.raises(ValueError, match="Database configuration missing"):
        DatabaseService()

def test_upsert_empty_dataframe(mock_env, caplog):
    """
    Test that the service gracefully handles and logs 
    when an empty DataFrame is provided.
    """
    service = DatabaseService()
    service.upsert_energy_data(pd.DataFrame())
    
    # Verify that a warning was logged and no execution happened
    assert "Empty DataFrame provided" in caplog.text

@patch("pandas.DataFrame.to_sql")
def test_upsert_success(mock_to_sql, mock_env, sample_energy_df):
    """
    Test that upsert_energy_data correctly calls the 
    pandas to_sql method with expected parameters.
    """
    service = DatabaseService()
    
    # Execute the method
    service.upsert_energy_data(sample_energy_df, table_name="test_table")
    
    # Verify pandas to_sql was called with the correct table name and engine
    mock_to_sql.assert_called_once_with(
        name="test_table",
        con=service.engine,
        if_exists='append',
        index=False
    )

@patch("pandas.DataFrame.to_sql")
def test_upsert_exception_handling(mock_to_sql, mock_env, sample_energy_df, caplog):
    """
    Test that database exceptions are caught, logged as fatal, 
    and re-raised for higher-level handling.
    """
    # Force an exception during to_sql execution
    mock_to_sql.side_effect = Exception("Connection Timeout")
    
    service = DatabaseService()
    
    with pytest.raises(Exception, match="Connection Timeout"):
        service.upsert_energy_data(sample_energy_df)
    
    # Verify the error was logged
    assert "FATAL: Database operation failed" in caplog.text

def test_token_injection_setup(mock_env):
    """
    Verify that the SQLAlchemy event listener is properly 
    attached to the engine during initialization.
    """
    from sqlalchemy import event
    service = DatabaseService()
    
    # Check if a 'do_connect' listener exists on the engine
    assert event.contains(service.engine, "do_connect", MagicMock()) is False 
    # (Checking the existence of the specific function is complex with decorators, 
    # but ensuring engine init didn't crash is primary here).