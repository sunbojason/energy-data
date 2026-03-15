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

    # UPDATED: Log reflects the broader scope of data
    logging.info("Starting scheduled ingestion for comprehensive NL market data.")

    try:
        client = EntsoeDataClient()
        tz = 'Europe/Amsterdam'
        end_date = pd.Timestamp.now(tz=tz).floor('D')
        start_date = end_date - timedelta(days=1)

        # UPDATED: Call the new aggregator method
        data_df = client.fetch_comprehensive_market_data(country_code='NL', start_time=start_date, end_time=end_date)

        if data_df is None or data_df.empty:
            logging.warning(f"DATA GAP: No data found for {start_date.date()}.")
            return

        # UPDATED: Data Integrity Check
        # Because Load data is often 15-min frequency, a full day might have 96 rows instead of 24.
        # We check for a minimum of 23 rows (handling potential Daylight Saving Time 23-hour days).
        expected_min_rows = 23 
        if len(data_df) < expected_min_rows:
            logging.warning(f"INTEGRITY ALERT: Expected at least {expected_min_rows} rows, but got {len(data_df)}.")

        # UPDATED: File naming convention to reflect the new payload
        container_name = os.environ.get("RAW_DATA_CONTAINER", "raw-data")
        file_name = f"nl_market_data_{start_date.strftime('%Y%m%d')}.csv"
        
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.upload_blob(data_df.to_csv(index=True), overwrite=True)
        
        # UPDATED: Log now outputs the number of metrics (columns) retrieved
        logging.info(f"INGESTION SUCCESS: Saved {file_name} ({len(data_df)} rows, {len(data_df.columns)} metrics) to {container_name}.")

    except Exception as e:
        logging.error(f"PIPELINE FAILURE: {str(e)}")
        raise 

@app.blob_trigger(arg_name="myblob", path="raw-data/{name}", connection="AzureWebJobsStorage")
@app.blob_output(arg_name="outputblob", path="cleaned-data/{name}", connection="AzureWebJobsStorage")
def blob_trigger_cleaning_processor(myblob: func.InputStream, outputblob: func.Out[str]):
    file_name = myblob.name
    logging.info(f"Processing new raw file: {file_name} ({myblob.length} bytes)")

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