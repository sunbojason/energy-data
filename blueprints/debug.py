import azure.functions as func
import logging
import pandas as pd
import json
from shared_logic.entsoe_client import EntsoeDataClient
from shared_logic.cleaning_service import CleaningService
from shared_logic.constants import DEFAULT_COUNTRY, DEFAULT_TIMEZONE
from shared_logic.azure_clients import blob_service_client

# Define the blueprint
debug_bp = func.Blueprint()

@debug_bp.route(route="manual_run", auth_level=func.AuthLevel.ANONYMOUS)
def manual_run_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """
    Manual HTTP trigger to test the Fetch -> Clean pipeline in isolation.
    Does not write to Blob Storage; returns result summary to the caller.
    """
    logging.info("MANUAL TEST: Starting pipeline verification...")

    test_date_str = req.params.get('date')
    
    try:
        # 1. Initialize Client
        client = EntsoeDataClient()
        tz = DEFAULT_TIMEZONE
        
        if test_date_str:
            logging.info(f"Received test date parameter: {test_date_str}")
            test_date = pd.to_datetime(test_date_str).tz_localize(tz)
            end_date = test_date.floor('D')
            start_date = end_date - pd.Timedelta(days=1)
        else:
            # Define window: Last 24 hours
            end_date = pd.Timestamp.now(tz=tz).floor('D')
            start_date = end_date - pd.Timedelta(days=1)

        # 2. Fetch Logic (Mirroring your timer trigger)
        logging.info(f"Fetching data for {DEFAULT_COUNTRY} from {start_date} to {end_date}")
        data_df = client.fetch_comprehensive_market_data(
            start_time=start_date, 
            end_time=end_date
        )

        # --- 1. Guard Clause: Early exit if data is missing ---
        if data_df is None or data_df.empty:
            logging.warning("No data fetched from ENTSO-E. Aborting ingestion.")
            return func.HttpResponse("No data found for this period", status_code=204)

        # --- 2. Operational Logic: Data exists, proceed to upload ---
        try:
            container_name = "raw-data"
            test_file = f"manual_test_{start_date.strftime('%Y%m%d')}.csv"

            if blob_service_client:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=test_file)
                
                # Debugging logs for your 404 issue
                logging.info(f"DEBUG: Targeting Account: {blob_service_client.account_name}")
                logging.info(f"DEBUG: Target URL: {blob_client.url}")

                # Upload data
                blob_client.upload_blob(data_df.to_csv(index=True).encode('utf-8'), overwrite=True)
                logging.info(f"UPLOAD SUCCESS: {test_file} persisted to {container_name}")
            else:
                logging.error("Blob client missing - Skipping persistence step.")
        except Exception as upload_err:
            # We catch it here so the rest of the function (cleaning preview) still runs
            logging.error(f"PERSISTENCE FAILURE: {str(upload_err)}")

        # 3. Prepare for Cleaning
        # The CleaningService expects a CSV string based on your blob_trigger logic
        raw_csv_string = data_df.to_csv(index=True)
        
        # 4. Cleaning Logic
        cleaned_csv_string = CleaningService.clean_energy_data(raw_csv_string)
        
        if not cleaned_csv_string:
            return func.HttpResponse("Cleaning failed: CleaningService returned empty result.", status_code=500)

        # 5. Result Verification (JSON response for easy reading)
        # Convert back to DF briefly just to get metadata for the response
        from io import StringIO
        cleaned_df = pd.read_csv(StringIO(cleaned_csv_string))
        
        response_payload = {
            "status": "Success",
            "date_range": {
                "start": str(start_date),
                "end": str(end_date)
            },
            "raw_stats": {
                "rows": len(data_df),
                "cols": len(data_df.columns)
            },
            "cleaned_stats": {
                "rows": len(cleaned_df),
                "cols": len(cleaned_df.columns)
            },
            "preview_columns": list(cleaned_df.columns)[:5] # Show first 5 columns
        }

        return func.HttpResponse(
            json.dumps(response_payload, indent=4),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"MANUAL TEST FAILURE: {str(e)}")
        return func.HttpResponse(f"Pipeline error: {str(e)}", status_code=500)