import azure.functions as func
import logging
import pandas as pd
import json
from shared_logic.entsoe_client import EntsoeDataClient
from shared_logic.cleaning_service import CleaningService
from shared_logic.constants import DEFAULT_COUNTRY, DEFAULT_TIMEZONE

# Define the blueprint
debug_bp = func.Blueprint()

@debug_bp.route(route="manual_run", auth_level=func.AuthLevel.ANONYMOUS)
def manual_run_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """
    Manual HTTP trigger to test the Fetch -> Clean pipeline in isolation.
    Does not write to Blob Storage; returns result summary to the caller.
    """
    logging.info("MANUAL TEST: Starting pipeline verification...")
    
    try:
        # 1. Initialize Client
        client = EntsoeDataClient()
        tz = DEFAULT_TIMEZONE
        
        # Define window: Last 24 hours
        end_date = pd.Timestamp.now(tz=tz).floor('D')
        start_date = end_date - pd.Timedelta(days=1)

        # 2. Fetch Logic (Mirroring your timer trigger)
        logging.info(f"Fetching data for {DEFAULT_COUNTRY} from {start_date} to {end_date}")
        data_df = client.fetch_comprehensive_market_data(
            start_time=start_date, 
            end_time=end_date, 
            target_country=DEFAULT_COUNTRY
        )

        if data_df is None or data_df.empty:
            return func.HttpResponse("Fetch failed: No data returned from API.", status_code=404)

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