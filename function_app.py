import azure.functions as func
import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from shared_logic.entsoe_client import EntsoeDataClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

# Global initialization: Reuse credential and clients across executions for better performance
# In serverless, this stays in memory across warm starts.
credential = DefaultAzureCredential()

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 2 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def timer_trigger_entsoe_ingestion(myTimer: func.TimerRequest) -> None:
    # 1. Environment & Pre-flight Checks
    storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
    container_name = os.environ.get("RAW_DATA_CONTAINER", "raw-data")
    
    if not storage_account_name:
        logging.error("ENVIRONMENT ERROR: STORAGE_ACCOUNT_NAME is not configured.")
        return

    if myTimer.past_due:
        logging.warning('Timer is running late, but continuing ingestion...')

    logging.info(f"Starting ingestion for Storage Account: {storage_account_name}")

    # 2. Domain Logic Initialization
    try:
        # Client handles its own internal API key validation
        client = EntsoeDataClient()
    except ValueError as ve:
        logging.error(f"CONFIGURATION ERROR: {str(ve)}")
        return

    # 3. Define Bidding Zone and Time Window
    # For Dutch energy market (NL), timestamps are critical for cross-border settlement.
    tz = 'Europe/Amsterdam'
    try:
        end_date = pd.Timestamp.now(tz=tz).floor('D')
        start_date = end_date - timedelta(days=1)
        logging.info(f"Target Window: {start_date} to {end_date} ({tz})")
    except Exception as te:
        logging.error(f"DATETIME ERROR: Failed to calculate date window: {str(te)}")
        return
    
    # 4. Data Acquisition
    try:
        data_df = client.fetch_day_ahead_prices(
            country_code='NL',
            start_time=start_date,
            end_time=end_date
        )

        if data_df is None or data_df.empty:
            logging.warning(f"DATA GAP: No data found for NL between {start_date} and {end_date}.")
            return

    except Exception as e:
        # This catches errors that tenacity couldn't fix (e.g., persistent 401/403)
        logging.error(f"INGESTION FAILURE: API request failed: {str(e)}")
        return

    # 5. Cloud Storage Persistence (The 'Raw' Layer)
    try:
        account_url = f"https://{storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        
        # Consistent naming convention is vital for backtesting
        file_name = f"nl_day_ahead_{start_date.strftime('%Y%m%d')}.csv"
        
        # Ensure index=True to preserve the 15-min or 60-min interval timestamps
        csv_data = data_df.to_csv(index=True)
        
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        
        # overwrite=True prevents duplicate trigger issues if the function retries
        blob_client.upload_blob(csv_data, overwrite=True)
        
        logging.info(f"STORAGE SUCCESS: Uploaded {file_name} ({len(data_df)} rows) to {container_name}")

    except AzureError as ae:
        logging.error(f"AZURE STORAGE ERROR: Identity/Permission or Network issue: {str(ae)}")
    except Exception as e:
        logging.error(f"UNEXPECTED ERROR: {str(e)}")