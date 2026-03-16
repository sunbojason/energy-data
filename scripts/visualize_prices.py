import sys
import os
import io
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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
    """Fetch, clean, and visualize comprehensive market data using the refactored pipeline."""
    load_environment_config()
    client = EntsoeDataClient()
    
    # Define window (Last 3 days) aligned with Amsterdam time
    tz_local = 'Europe/Amsterdam'
    current_time = pd.Timestamp.now(tz=tz_local)
    end_date = current_time.ceil('h')
    start_date = end_date - pd.Timedelta(days=3)
    
    print(f"STATUS: Fetching comprehensive market data for NL from {start_date} to {end_date}...")
    try:
        # Refactored call: start_time and end_time are positional; target_country is optional
        raw_df = client.fetch_comprehensive_market_data(start_time=start_date, end_time=end_date)
    except Exception as e:
        print(f"ERROR: Failed to fetch data: {e}")
        return
    
    if raw_df.empty:
        print("WARNING: No data returned from API.")
        return

    # 2. Process through CleaningService (Enforces 15-min alignment and filling strategies)
    print("STATUS: Applying 15-minute resampling and asset-specific filling logic...")
    raw_csv_buffer = raw_df.to_csv()
    cleaned_csv_str = CleaningService.clean_energy_data(raw_csv_buffer)
    
    # Load cleaned data and ensure index is localized for plotting
    df_cleaned = pd.read_csv(io.StringIO(cleaned_csv_str), index_col=0, parse_dates=True)
    if not df_cleaned.empty:
        df_cleaned.index = pd.to_datetime(df_cleaned.index).tz_convert(tz_local)

    # 3. Visualization: Dual-Axis Chart
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax1 = plt.subplots(figsize=(14, 7))
    
    # --- AXIS 1 (Left): Day-Ahead Prices (Step Function) ---
    # Dynamically find columns with the 'DA_' prefix from the refactored client
    price_cols = [c for c in df_cleaned.columns if c.startswith('DA_')]
    if price_cols:
        color1 = '#1f77b4'
        # Using the first price column found
        target_price = price_cols[0]
        ax1.plot(df_cleaned.index, df_cleaned[target_price], 
                 label='Day-Ahead Price (EUR/MWh)', color=color1, linewidth=2, drawstyle='steps-post')
        
        ax1.set_ylabel('Price [EUR / MWh]', color=color1, fontsize=12, fontweight='bold')
        ax1.tick_params(axis='y', labelcolor=color1)
        
        avg_price = df_cleaned[target_price].mean()
        ax1.axhline(avg_price, color=color1, linestyle='--', linewidth=1, alpha=0.5, 
                    label=f'Avg Price: {avg_price:.2f} EUR')

    # --- AXIS 2 (Right): Actual Load (Continuous Curve) ---
    # Dynamically find columns with the 'Load_Actual' prefix
    load_cols = [c for c in df_cleaned.columns if c.startswith('Load_Actual')]
    ax2 = ax1.twinx()
    if load_cols:
        color2 = '#ff7f0e'
        target_load = load_cols[0]
        ax2.plot(df_cleaned.index, df_cleaned[target_load], 
                 label='Actual System Load (MW)', color=color2, linewidth=2, alpha=0.8)
        
        ax2.fill_between(df_cleaned.index, df_cleaned[target_load], alpha=0.1, color=color2)
        ax2.set_ylabel('System Load [MW]', color=color2, fontsize=12, fontweight='bold')
        ax2.tick_params(axis='y', labelcolor=color2)
        
        if not df_cleaned[target_load].empty:
            ax2.set_ylim(bottom=df_cleaned[target_load].min() * 0.9)

    # Formatting UI
    ax1.set_title(f'NL Market Dynamics: {start_date.date()} to {end_date.date()} (15-min Alignment)', 
                  fontsize=14, fontweight='bold', pad=15)
    
    # X-Axis Time Formatting
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d\n%H:%M', tz=tz_local))
    ax1.xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))
    
    # Merge legends
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left', frameon=True)
    
    # Metadata info
    timestamp_str = f"Pipeline Output: {current_time.strftime('%Y-%m-%d %H:%M:%S')} ({tz_local})"
    fig.text(0.99, 0.01, timestamp_str, transform=fig.transFigure,
             ha='right', va='bottom', fontsize=9, color='gray', fontstyle='italic')

    plt.tight_layout()
    print("SUCCESS: Rendering 15-minute resolution market chart...")
    plt.show()

if __name__ == "__main__":
    run_price_visualization()