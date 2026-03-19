import pytest
import pandas as pd
import io
from unittest.mock import MagicMock, patch
from shared_logic.cleaning_service import CleaningService
from shared_logic.database_service import DatabaseService

@pytest.fixture
def mock_db_env(monkeypatch):
    monkeypatch.setenv("SQL_SERVER_NAME", "mock-server")
    monkeypatch.setenv("SQL_DATABASE_NAME", "mock-db")

def test_e2e_blob_to_sql_flow(mock_db_env):
    """
    Simulates the full pipeline:
    1. Raw ENTSO-E CSV received.
    2. CleaningService processes it (resampling, local time, etc).
    3. DatabaseService upserts it to SQL.
    """
    # 1. Mock Input Data (Raw ENTSO-E style)
    raw_csv = (
        "Time_UTC,DA_Price_0,Load_Actual_0\n"
        "2026-03-11 00:00:00+00:00,50.0,1000.0\n"
        "2026-03-11 01:00:00+00:00,60.0,1200.0\n"
    )
    
    # 2. Step: Cleaning
    cleaned_csv = CleaningService.clean_energy_data(raw_csv)
    df_cleaned = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    # Verification of cleaning step in E2E
    assert len(df_cleaned) == 5 # 00:00 to 01:00 inclusive = 5 slots
    assert 'Time_Local' in df_cleaned.columns
    
    # 3. Step: Database Ingestion (Mocked)
    with patch("shared_logic.database_service.create_engine"), \
         patch("shared_logic.database_service.DefaultAzureCredential"), \
         patch("sqlalchemy.event.listen"), \
         patch("pandas.DataFrame.to_sql") as mock_to_sql, \
         patch("sqlalchemy.inspect") as mock_inspect:
        
        # Setup mock inspector to avoid actual DB calls
        mock_inspect.return_value.has_table.return_value = True
        mock_inspect.return_value.get_columns.return_value = [
            {'name': 'Time_UTC'}, {'name': 'Time_Local'}, 
            {'name': 'DA_Price_0'}, {'name': 'Load_Actual_0'}
        ]
        
        db_service = DatabaseService()
        db_service.upsert_energy_data(df_cleaned, table_name="entsoe_test")
        
        # 4. Final Validation
        assert mock_to_sql.called
        args, kwargs = mock_to_sql.call_args
        assert kwargs['name'] == "entsoe_test"
        assert kwargs['if_exists'] == "append"
        assert 'Time_Local' in kwargs['dtype']

    print("E2E Pipeline Test Passed!")
