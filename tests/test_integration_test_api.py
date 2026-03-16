import os
import pandas as pd
from shared_logic.entsoe_client import EntsoeDataClient

# Load variables from local.settings.json
# For a quick test, you can also just set it in the shell: export ENTSOE_API_KEY='...'

def test_live_api_connection():
    print("Initializing EntsoeDataClient...")
    client = EntsoeDataClient()
    
    # Define a 24-hour window in the recent past
    # Using Amsterdam time as per your Dutch energy focus
    tz = 'Europe/Amsterdam'
    end_date = pd.Timestamp.now(tz=tz).floor('D')
    start_date = end_date - pd.Timedelta(days=1)
    
    print(f"Requesting comprehensive market data for NL from {start_date} to {end_date}...")
    
    try:
        # UPDATED: Call the refactored comprehensive method with correct arguments
        df = client.fetch_comprehensive_market_data(
            start_time=start_date,
            end_time=end_date,
            target_country='NL'
        )
        
        if not df.empty:
            print("Successfully retrieved data!")
            print(df.head())
            print(f"Total metrics (columns) retrieved: {len(df.columns)}")
            print(f"Total rows retrieved: {len(df)}")
            
            # Basic validation for Dutch Market (15-min grid mapping)
            if 'DA_Price_0' in df.columns:
                # Prices are usually between -500 and 5000 EUR/MWh
                avg_price = df['DA_Price_0'].mean()
                print(f"Average DA Price for the period: {avg_price:.2f} EUR/MWh")
            else:
                print("Warning: DA_Price_0 column is missing from the output.")
                
            if 'Load_Actual_0' in df.columns:
                avg_load = df['Load_Actual_0'].mean()
                print(f"Average Actual Load for the period: {avg_load:.2f} MW")
            
            # Verify row counts for a 15-min grid (typically 96 rows for a standard 24h period)
            expected_min_rows = 92 # Accounting for 23-hour DST days
            if len(df) >= expected_min_rows: 
                print(f"Row count validation passed: {len(df)} rows.")
            else:
                print(f"Warning: Unexpected row count. Expected at least {expected_min_rows}, got {len(df)}.")
                
        else:
            print("API returned an empty DataFrame. Check if the market gate is closed or if the date range is valid.")
            
    except Exception as e:
        print(f"Integration Test Failed: {str(e)}")

if __name__ == "__main__":
    test_live_api_connection()