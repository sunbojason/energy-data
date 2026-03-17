import os
import pandas as pd
import logging
from shared_logic.entsoe_client import EntsoeDataClient
from shared_logic.constants import DEFAULT_COUNTRY, DEFAULT_TIMEZONE

def test_live_api_connection(caplog):
    caplog.set_level(logging.INFO)
    logging.info("Initializing EntsoeDataClient...")
    client = EntsoeDataClient()
    
    # Define a 24-hour window in the recent past
    # Using Amsterdam time as per your Dutch energy focus
    tz = DEFAULT_TIMEZONE
    end_date = pd.Timestamp.now(tz=tz).floor('D')
    start_date = end_date - pd.Timedelta(days=1)
    
    logging.info(f"Requesting comprehensive market data for {DEFAULT_COUNTRY} from {start_date} to {end_date}...")
    
    try:
        # UPDATED: Call the refactored comprehensive method with correct arguments
        df = client.fetch_comprehensive_market_data(
            start_time=start_date,
            end_time=end_date,
            target_country=DEFAULT_COUNTRY
        )
        
        if not df.empty:
            logging.info("Successfully retrieved data!")
            logging.info(df.head())
            logging.info(f"Total metrics (columns) retrieved: {len(df.columns)}")
            logging.info(f"Total rows retrieved: {len(df)}")
            
            # Basic validation for Dutch Market (15-min grid mapping)
            if 'DA_Price_0' in df.columns:
                # Prices are usually between -500 and 5000 EUR/MWh
                avg_price = df['DA_Price_0'].mean()
                logging.info(f"Average DA Price for the period: {avg_price:.2f} EUR/MWh")
            else:
                logging.warning("Warning: DA_Price_0 column is missing from the output.")
                
            if 'Load_Actual_0' in df.columns:
                avg_load = df['Load_Actual_0'].mean()
                logging.info(f"Average Actual Load for the period: {avg_load:.2f} MW")
            
            # Verify row counts for a 15-min grid (typically 96 rows for a standard 24h period)
            expected_min_rows = 92 # Accounting for 23-hour DST days
            if len(df) >= expected_min_rows: 
                logging.info(f"Row count validation passed: {len(df)} rows.")
            else:
                logging.warning(f"Warning: Unexpected row count. Expected at least {expected_min_rows}, got {len(df)}.")
                
        else:
            logging.warning("API returned an empty DataFrame. Check if the market gate is closed or if the date range is valid.")
            
    except Exception as e:
        logging.error(f"Integration Test Failed: {str(e)}")