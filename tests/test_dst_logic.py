import pandas as pd
import numpy as np
from shared_logic.cleaning_service import CleaningService
from shared_logic.constants import DEFAULT_FREQ_GRID

def test_dst_transitions():
    # 1. Test Spring Forward (March 31, 2024)
    # UTC 00:00 to 02:00
    # Local 01:00 to 03:00 (02:00 disappears)
    start_utc = "2024-03-31 00:00:00"
    end_utc = "2024-03-31 02:00:00"
    times = pd.date_range(start=start_utc, end=end_utc, freq=DEFAULT_FREQ_GRID, tz='UTC')
    df = pd.DataFrame(index=times)
    df['value'] = 1.0
    
    csv_content = df.to_csv()
    cleaned_csv = CleaningService.clean_energy_data(csv_content)
    cleaned_df = pd.read_csv(pd.io.common.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    print("Spring Forward Test (2024-03-31):")
    # UTC 00:00 (Local 01:00)
    # UTC 01:00 (Local 03:00) - 02:00 should be skipped in local wall time
    print(cleaned_df[['Time_Local']])
    
    # 2. Test Fall Back (October 27, 2024)
    # UTC 00:00 to 02:00
    # Local 02:00 to 03:00 (repeats)
    start_utc_fall = "2024-10-27 00:00:00"
    end_utc_fall = "2024-10-27 02:00:00"
    times_fall = pd.date_range(start=start_utc_fall, end=end_utc_fall, freq=DEFAULT_FREQ_GRID, tz='UTC')
    df_fall = pd.DataFrame(index=times_fall)
    df_fall['value'] = 1.0
    
    csv_fall_content = df_fall.to_csv()
    cleaned_csv_fall = CleaningService.clean_energy_data(csv_fall_content)
    cleaned_df_fall = pd.read_csv(pd.io.common.StringIO(cleaned_csv_fall), index_col=0, parse_dates=True)
    
    print("\nFall Back Test (2024-10-27):")
    # UTC 00:00 (Local 02:00 CEST)
    # UTC 01:00 (Local 02:00 CET) - 02:00 repeats
    print(cleaned_df_fall[['Time_Local']])

    # Assertions
    # March: UTC 00:45 -> Local 01:45, UTC 01:00 -> Local 03:00
    assert cleaned_df.loc['2024-03-31 00:45:00+00:00', 'Time_Local'] == '2024-03-31 01:45:00'
    assert cleaned_df.loc['2024-03-31 01:00:00+00:00', 'Time_Local'] == '2024-03-31 03:00:00'
    
    # October: UTC 00:45 -> Local 02:45, UTC 01:00 -> Local 02:00
    assert cleaned_df_fall.loc['2024-10-27 00:45:00+00:00', 'Time_Local'] == '2024-10-27 02:45:00'
    assert cleaned_df_fall.loc['2024-10-27 01:00:00+00:00', 'Time_Local'] == '2024-10-27 02:00:00'
    
    print("\nAll DST tests passed!")

if __name__ == "__main__":
    test_dst_transitions()
