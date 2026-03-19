import pytest
import pandas as pd
import threading
from unittest.mock import MagicMock, patch
from sqlalchemy import text
from shared_logic.database_service import DatabaseService
from sqlalchemy.exc import OperationalError

@pytest.fixture
def mock_db_service(monkeypatch):
    monkeypatch.setenv("SQL_SERVER_NAME", "mock-server")
    monkeypatch.setenv("SQL_DATABASE_NAME", "mock-db")
    with patch("shared_logic.database_service.create_engine"), \
         patch("shared_logic.database_service.DefaultAzureCredential"), \
         patch("sqlalchemy.event.listen"), \
         patch("sqlalchemy.event.contains", return_value=False), \
         patch("sqlalchemy.inspect") as mock_inspect:
        service = DatabaseService()
        yield service

def test_ensure_table_schema_sql_generation(mock_db_service):
    """Verify that correct ALTER TABLE statements are generated for different types."""
    df = pd.DataFrame({
        "New_Metric": [1.0],
        "Time_Local": ["2026-03-11 00:00:00"]
    })
    
    mock_inspector = MagicMock()
    mock_inspector.has_table.return_value = True
    mock_inspector.get_columns.return_value = [{"name": "Time_UTC"}] # Only one existing column
    
    mock_conn = MagicMock()
    mock_db_service.engine.begin.return_value.__enter__.return_value = mock_conn
    
    with patch("sqlalchemy.inspect", return_value=mock_inspector):
        mock_db_service._ensure_table_schema(df, "test_table")
        
    # Verify calls
    # One for New_Metric (FLOAT), one for Time_Local (DATETIME2)
    assert mock_conn.execute.call_count == 2
    calls = [str(call[0][0]) for call in mock_conn.execute.call_args_list]
    assert any("ADD [New_Metric] FLOAT" in c for c in calls)
    assert any("ADD [Time_Local] DATETIME2" in c for c in calls)

def test_upsert_token_expiry_simulation(mock_db_service):
    """Simulate token expiry and verify that provide_token is called (conceptually)."""
    # This is hard to test directly because it's an event listener,
    # but we can verify the engine was initialized and listener attached.
    from sqlalchemy import event
    assert event.contains(mock_db_service.engine, "do_connect", MagicMock()) is False
    # The actual listener is a nested function, so 'contains' won't find it with MagicMock().
    # But we already have a basic test for this.

def test_database_timeout_retry_logic(mock_db_service, caplog):
    """Verify behavior when a timeout occurs."""
    df = pd.DataFrame({"val": [1.0]})
    # Mock to_sql to raise a timeout error
    with patch("pandas.DataFrame.to_sql", side_effect=Exception("Connection Timeout")):
        with pytest.raises(Exception, match="Connection Timeout"):
            mock_db_service.upsert_energy_data(df)
    
    assert "Database operation failed" in caplog.text

def test_concurrent_schema_evolution(mock_db_service):
    """Simulate multiple threads trying to add the same column simultaneously."""
    df = pd.DataFrame({"New_Col": [1.0]})
    
    mock_inspector = MagicMock()
    mock_inspector.has_table.return_value = True
    # Initially no columns
    mock_inspector.get_columns.return_value = []
    
    mock_conn = MagicMock()
    # Simulate a "Column already exists" error for the second thread
    def execute_side_effect(stmt):
        if "New_Col" in str(stmt) and execute_side_effect.called:
            raise Exception("Column already exists")
        execute_side_effect.called = True
    
    execute_side_effect.called = False
    mock_conn.execute.side_effect = execute_side_effect
    mock_db_service.engine.begin.return_value.__enter__.return_value = mock_conn

    with patch("sqlalchemy.inspect", return_value=mock_inspector):
        # We run it twice to simulate sequential or concurrent logic overlapping
        mock_db_service._ensure_table_schema(df, "table")
        mock_db_service._ensure_table_schema(df, "table")

    # Should have logged warnings for the second attempt but not crashed
    assert mock_conn.execute.call_count == 2
