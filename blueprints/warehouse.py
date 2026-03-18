import azure.functions as func
import logging
import pandas as pd
from io import StringIO
from shared_logic.database_service import DatabaseService

# Initialize the blueprint
warehouse_bp = func.Blueprint()

@warehouse_bp.blob_trigger(arg_name="cleanedblob", path="cleaned-data/{name}", connection="AzureWebJobsStorage")
def blob_trigger_sql_ingestion(cleanedblob: func.InputStream):
    """
    Final stage: Triggered automatically when clean data arrives.
    Reads the CSV and dispatches it to the Azure SQL Database.

    Routing logic:
      - Files containing '_extended_' in their name  → 'entsoe_extended' table
      - All other files                               → 'entsoe' table
    """
    file_name = cleanedblob.name
    logging.info(f"WAREHOUSE PIPELINE INITIATED: Reading {file_name} ({cleanedblob.length} bytes)")

    try:
        # 1. Read the blob content
        content = cleanedblob.read().decode('utf-8')
        if not content.strip():
            logging.warning(f"File {file_name} is practically empty. Halting ingestion.")
            return

        # 2. Parse into DataFrame
        df = pd.read_csv(StringIO(content))
        
        # Ensure timestamp columns are treated correctly
        for ts_col in ('timestamp', 'Time_UTC'):
            if ts_col in df.columns:
                df[ts_col] = pd.to_datetime(df[ts_col], utc=True)

        # 3. Initialize connection and push data
        db_service = DatabaseService()
        db_service.upsert_energy_data(df, table_name="entsoe")
        
        logging.info(f"WAREHOUSE SUCCESS: Payload from {file_name} permanently stored.")


    except pd.errors.EmptyDataError:
        logging.error(f"WAREHOUSE FAILURE: {file_name} contains no parsable columns.")
    except Exception as e:
        # Catching all exceptions here ensures the Function App doesn't crash completely,
        # isolating the blast radius to just this specific file execution.
        logging.error(f"WAREHOUSE SYSTEM ERROR processing {file_name}: {str(e)}")