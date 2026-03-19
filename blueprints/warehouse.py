import azure.functions as func
import logging
import pandas as pd
from io import StringIO
from shared_logic.database_service import DatabaseService

# Initialize the blueprint
warehouse_bp = func.Blueprint()

@warehouse_bp.blob_trigger(arg_name="cleanedblob", path="cleaned-data/{name}", connection="AzureWebJobsStorage")
def blob_trigger_sql_ingestion(cleanedblob: func.InputStream):
    file_name = cleanedblob.name
    logging.info(f"Warehouse injection started: {file_name}")

    try:
        content = cleanedblob.read().decode('utf-8')
        df = _prepare_data_for_ingestion(content, file_name)
        
        if df.empty:
            return

        db_service = DatabaseService()
        db_service.upsert_energy_data(df, table_name="entsoe")
        
        logging.info(f"Warehouse injection completed for {file_name}")

    except Exception as e:
        logging.error(f"Warehouse system error processing {file_name}: {str(e)}")

def _prepare_data_for_ingestion(content: str, source_name: str) -> pd.DataFrame:
    if not content.strip():
        logging.warning(f"Aborting: {source_name} is empty.")
        return pd.DataFrame()

    try:
        df = pd.read_csv(StringIO(content))
        return _standardize_timestamps(df)
    except pd.errors.EmptyDataError:
        logging.error(f"Parse error: {source_name} has no valid columns.")
        return pd.DataFrame()

def _standardize_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    # Aggressively look for timestamp columns
    for col in df.columns:
        if str(col).lower() in ('timestamp', 'time_utc', 'index'):
            df.rename(columns={col: 'Time_UTC'}, inplace=True)
            df['Time_UTC'] = pd.to_datetime(df['Time_UTC'], utc=True)
        elif str(col).lower() == 'time_local':
            df['Time_Local'] = pd.to_datetime(df['Time_Local'])
    return df