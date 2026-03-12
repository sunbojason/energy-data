import azure.functions as func
import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

from shared_logic.entsoe_client import EntsoeDataClient
from shared_logic.cleaning_service import CleaningService

# --- Global Initialization (Singleton Pattern) ---
# Reusing credentials and clients across warm starts saves ~200-500ms per execution
credential = DefaultAzureCredential()
storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
account_url = f"https://{storage_account_name}.blob.core.windows.net" if storage_account_name else None

# Initialize outside to reuse the connection pool
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential) if account_url else None

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 2 * * *", arg_name="myTimer", run_on_startup=False)
@app.retry(strategy="fixed_delay", max_retry_count="3", delay_interval="00:05:00")
def timer_trigger_entsoe_ingestion(myTimer: func.TimerRequest) -> None:
    if not storage_account_name or not blob_service_client:
        logging.error("CRITICAL: Storage configuration is missing.")
        return

    logging.info("Starting scheduled ingestion for NL Day-Ahead prices.")

    try:
        client = EntsoeDataClient()
        tz = 'Europe/Amsterdam'
        end_date = pd.Timestamp.now(tz=tz).floor('D')
        start_date = end_date - timedelta(days=1)

        data_df = client.fetch_day_ahead_prices(country_code='NL', start_time=start_date, end_time=end_date)

        if data_df is None or data_df.empty:
            logging.warning(f"DATA GAP: No data found for {start_date.date()}.")
            return

        # Data Integrity Check: Verify expected row count for Quant models
        expected_rows = 24 # Standard day
        if len(data_df) != expected_rows:
            logging.warning(f"INTEGRITY ALERT: Expected {expected_rows} rows, but got {len(data_df)}.")

        # Persistence
        container_name = os.environ.get("RAW_DATA_CONTAINER", "raw-data")
        file_name = f"nl_day_ahead_{start_date.strftime('%Y%m%d')}.csv"
        
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.upload_blob(data_df.to_csv(index=True), overwrite=True)
        
        logging.info(f"INGESTION SUCCESS: Saved {file_name} to {container_name}.")

    except Exception as e:
        logging.error(f"PIPELINE FAILURE: {str(e)}")
        raise # Raising allows the @app.retry decorator to catch it

@app.blob_trigger(arg_name="myblob", path="raw-data/{name}", connection="AzureWebJobsStorage")
@app.blob_output(arg_name="outputblob", path="cleaned-data/{name}", connection="AzureWebJobsStorage")
def blob_trigger_cleaning_processor(myblob: func.InputStream, outputblob: func.Out[str]):
    file_name = myblob.name
    logging.info(f"Processing new raw file: {file_name}")

    try:
        raw_content = myblob.read().decode('utf-8')
        if not raw_content: return

        # Transform and Clean
        cleaned_csv = CleaningService.clean_energy_data(raw_content)
        
        if cleaned_csv:
            outputblob.set(cleaned_csv)
            logging.info(f"CLEANING SUCCESS: {file_name} promoted to cleaned layer.")
    except Exception as e:
        logging.error(f"CLEANING FAILURE: {file_name} - {str(e)}")