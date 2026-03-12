import sys
import os
import io
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# 1. Dynamic path injection to locate 'shared_logic'
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from shared_logic.entsoe_client import EntsoeDataClient
from shared_logic.cleaning_service import CleaningService

def load_environment_config():
    """Load settings from local.settings.json into environment variables."""
    config_path = os.path.join(root_path, 'local.settings.json')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            settings = json.load(f)
            for k, v in settings.get("Values", {}).items():
                os.environ[k] = str(v)

def run_price_visualization():
    """Fetch, clean, and visualize energy prices with data integrity stats."""
    load_environment_config()
    client = EntsoeDataClient()
    
    # Define window (Last 3 days)
    tz_local = 'Europe/Amsterdam'
    current_time = pd.Timestamp.now(tz=tz_local)
    end_date = current_time.ceil('h')
    start_date = end_date - pd.Timedelta(days=3)
    
    print(f"STATUS: Fetching raw data from {start_date} to {end_date}...")
    try:
        raw_df = client.fetch_day_ahead_prices('NL', start_time=start_date, end_time=end_date)
    except Exception as e:
        print(f"ERROR: Failed to fetch data: {e}")
        return
    
    if raw_df.empty:
        print("WARNING: No data returned from API.")
        return

    # 2. Process through CleaningService
    print("STATUS: Applying cleaning and resampling logic...")
    raw_csv_buffer = raw_df.to_csv()
    cleaned_csv_str = CleaningService.clean_energy_data(raw_csv_buffer)
    
    df_cleaned = pd.read_csv(io.StringIO(cleaned_csv_str), index_col=0, parse_dates=True)
    df_cleaned.index = pd.to_datetime(df_cleaned.index).tz_convert(tz_local)

    # Calculate Data Stats
    total_hours = len(df_cleaned)
    missing_hours = df_cleaned.iloc[:, 0].isna().sum()

    # 3. Visualization with Light Theme, Stats, and Timestamp
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Main Price Curve with Hour Count in label
    ax.plot(df_cleaned.index, df_cleaned.iloc[:, 0], 
            label=f'Cleaned Price ({total_hours}h total)', 
            color='#1f77b4', linewidth=2, zorder=3)
    
    # Raw Points
    ax.scatter(raw_df.index, raw_df.iloc[:, 0], 
               color='#ff7f0e', s=15, alpha=0.5, 
               label=f'Raw API Points (missing: {missing_hours}h)', zorder=2)

    # Reference lines
    ax.axhline(0, color='black', linestyle='-', linewidth=0.8, alpha=0.4)
    avg_price = df_cleaned.iloc[:, 0].mean()
    ax.axhline(avg_price, color='green', linestyle='--', linewidth=1, alpha=0.5, 
               label=f'Avg Price: {avg_price:.2f} EUR')

    # Formatting UI
    ax.set_title(f'NL Day-Ahead Price Integrity Check: {start_date.date()} to {end_date.date()}', 
                 fontsize=14, fontweight='bold', pad=15)
    ax.set_ylabel('Price [EUR / MWh]', fontsize=11)
    
    # X-Axis Time Formatting
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d\n%H:%M', tz=tz_local))
    ax.xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))
    
    # Add Timestamp at the bottom right
    timestamp_str = f"Generated at: {current_time.strftime('%Y-%m-%d %H:%M:%S')} ({tz_local})"
    fig.text(0.99, 0.01, timestamp_str, transform=fig.transFigure,
             ha='right', va='bottom', fontsize=9, color='gray', fontstyle='italic')

    plt.legend(loc='upper left', frameon=True, shadow=False)
    plt.tight_layout()
    
    # Save output
    output_path = os.path.join(root_path, "scripts", "price_viz_final.png")
    plt.savefig(output_path, dpi=300)
    print(f"SUCCESS: Visualization saved to {output_path}")
    plt.show()

if __name__ == "__main__":
    run_price_visualization()