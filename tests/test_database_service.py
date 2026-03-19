import pytest
import os
import pandas as pd
from unittest.mock import MagicMock, patch
from shared_logic.database_service import DatabaseService

# --- Test Fixtures ---

@pytest.fixture
def mock_db_env(monkeypatch):
    """Business Logic: Ensure dummy credentials for unit tests to prevent AAD token acquisition."""
    monkeypatch.setenv("SQL_SERVER_NAME", "test-server.database.windows.net")
    monkeypatch.setenv("SQL_DATABASE_NAME", "test-db")

@pytest.fixture
def sample_market_df():
    """Generates a standard 15-min interval DataFrame for testing ingestion."""
    return pd.DataFrame({
        "timestamp": ["2026-03-11 00:00:00"],
        "DA_Price": [45.5],
        "Load_Actual": [12000.0]
    })

# --- Unit Tests: Database Lifecycle ---

def test_service_initialization_with_config(mock_db_env):
    """
    Business Logic: Service must correctly construct connection strings for ODBC Driver 18
    based on Azure environment variables.
    """
    service = DatabaseService()
    assert service.server == "test-server.database.windows.net"
    assert service.database == "test-db"
    assert "TrustServerCertificate=yes" in service.conn_str
    assert service.engine is not None

def test_initialization_failure_on_missing_env(monkeypatch):
    """
    Business Logic: Prevent misconfigured deployments by failing early if config is missing.
    """
    monkeypatch.delenv("SQL_SERVER_NAME", raising=False)
    monkeypatch.delenv("SQL_DATABASE_NAME", raising=False)
    
    with pytest.raises(ValueError, match="Database configuration missing"):
        DatabaseService()

def test_graceful_skipping_of_empty_upsert(mock_db_env, caplog):
    """
    Business Logic: If a processing step returns zero rows, the database service 
    should log a warning and skip the transaction to save IO.
    """
    service = DatabaseService()
    service.upsert_energy_data(pd.DataFrame())
    
    assert "Upsert aborted" in caplog.text

@patch("shared_logic.database_service.DatabaseService._delete_existing_records")
@patch("pandas.DataFrame.to_sql")
@patch("shared_logic.database_service.DatabaseService._ensure_table_schema")
def test_upsert_parameters_and_table_deduction(mock_schema, mock_to_sql, mock_delete, mock_db_env, sample_market_df):
    """
    Business Logic: Verify that the upsert logic correctly maps DataFrame columns to 
    SQL types and enforces 'append' mode for time-series accumulation.
    """
    service = DatabaseService()
    service.upsert_energy_data(sample_market_df, table_name="market_records")

    # Verify pandas to_sql parameters
    _, kwargs = mock_to_sql.call_args
    assert kwargs["name"] == "market_records"
    assert kwargs["if_exists"] == "append"
    assert "Time_UTC" in kwargs["dtype"]

@patch("shared_logic.database_service.DatabaseService._delete_existing_records")
@patch("pandas.DataFrame.to_sql")
@patch("shared_logic.database_service.DatabaseService._ensure_table_schema")
def test_database_operation_exception_logging(mock_schema, mock_to_sql, mock_delete, mock_db_env, sample_market_df, caplog):
    """
    Business Logic: Critical database failures must be logged with context for operational 
    monitoring while ensuring the exception bubbles up to trigger retries.
    """
    mock_to_sql.side_effect = Exception("Connection Timeout")
    service = DatabaseService()
    
    with pytest.raises(Exception, match="Connection Timeout"):
        service.upsert_energy_data(sample_market_df)
    
    assert "Database operation failed" in caplog.text

def test_aad_authentication_listener_attachment(mock_db_env):
    """
    Business Logic: Verify that the Entra ID token injection listener is successfully 
    attached to the SQLAlchemy engine.
    """
    from sqlalchemy import event
    service = DatabaseService()
    # Check for the existence of any do_connect listeners
    assert event.contains(service.engine, "do_connect", service._inject_entra_token)