import azure.functions as func
import logging
import pandas as pd
import os
from shared_logic.entsoe_client import EntsoeDataClient
from shared_logic.constants import DEFAULT_COUNTRY, DEFAULT_TIMEZONE
from shared_logic.azure_clients import blob_service_client, storage_account_name

ingestion_bp = func.Blueprint()

@ingestion_bp.timer_trigger(schedule="0 0 2 * * *", arg_name="myTimer", run_on_startup=False)
@ingestion_bp.retry(strategy="fixed_delay", max_retry_count="3", delay_interval="00:05:00")
def timer_trigger_entsoe_ingestion(myTimer: func.TimerRequest) -> None:
    """
    Timer trigger function to ingest comprehensive market data daily at 02:00 AM.
    Incorporates retry mechanisms to handle transient network/API failures.
    """
    if not storage_account_name or not blob_service_client:
        logging.error("CRITICAL: Storage configuration is missing. Ensure STORAGE_ACCOUNT_NAME is set.")
        return

    logging.info("Starting scheduled ingestion for comprehensive BE market data (15-min grid).")

    try:
        client = EntsoeDataClient()
        tz = DEFAULT_TIMEZONE
        # Define window: Yesterday 00:00 to Today 00:00
        end_date = pd.Timestamp.now(tz=tz).floor('D')
        start_date = end_date - pd.Timedelta(days=1)

        # FIX 1: Use the correct parameter name (target_country instead of country_code)
        # Using default 'BE' as specified in the client logic
        data_df = client.fetch_comprehensive_market_data(start_time=start_date, end_time=end_date, target_country=DEFAULT_COUNTRY)

        if data_df is None or data_df.empty:
            # Applying "allow small miss, prevent cascade failure" principle
            logging.warning(f"DATA GAP: No data returned from ENTSO-E for {start_date.date()}. Skipping upload.")
            return

        # FIX 2: Data Integrity Check for 15-min frequency
        # 1 day = 24 hours * 4 quarters = 96 rows. 
        # For Daylight Saving Time (DST) spring forward: 23 hours = 92 rows.
        expected_min_rows = 92 
        actual_rows = len(data_df)
        if actual_rows < expected_min_rows:
            logging.warning(f"INTEGRITY ALERT: Expected at least {expected_min_rows} rows for a 15-min grid, but got {actual_rows}.")

        container_name = os.environ.get("RAW_DATA_CONTAINER", "raw-data")
        file_name = f"be_market_data_{start_date.strftime('%Y%m%d')}.csv"
        
        # Write to Blob Storage
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        
        # FIX 3: Explicitly encode to utf-8 bytes for safer blob network transport
        csv_payload = data_df.to_csv(index=True).encode('utf-8')
        blob_client.upload_blob(csv_payload, overwrite=True)
        
        logging.info(f"INGESTION SUCCESS: Saved {file_name} ({actual_rows} rows, {len(data_df.columns)} metrics) to {container_name}.")

    except Exception as e:
        # We raise the exception so the @app.retry decorator can catch it and re-trigger
        logging.error(f"PIPELINE FAILURE in timer trigger: {str(e)}")
        raise 
