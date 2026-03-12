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
    
    print(f"Requesting Day-Ahead prices for NL from {start_date} to {end_date}...")
    
    try:
        df = client.fetch_day_ahead_prices(
            country_code='NL',
            start_time=start_date,
            end_time=end_date
        )
        
        if not df.empty:
            print("Successfully retrieved data!")
            print(df.head())
            print(f"Total rows retrieved: {len(df)}")
            
            # Basic validation for Dutch Market
            # Prices are usually between -500 and 5000 EUR/MWh
            avg_price = df.iloc[:, 0].mean()
            print(f"Average Price for the period: {avg_price:.2f} EUR/MWh")
        else:
            print("API returned an empty DataFrame. Check if the market gate is closed or if the date range is valid.")
            
    except Exception as e:
        print(f"Integration Test Failed: {str(e)}")

if __name__ == "__main__":
    test_live_api_connection()