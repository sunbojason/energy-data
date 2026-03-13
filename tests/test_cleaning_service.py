import pytest
import pandas as pd
import numpy as np
import io
from shared_logic.cleaning_service import CleaningService

def test_clean_energy_data_spring_forward():
    """
    Test Spring DST (March): Clocks jump from 02:00 to 03:00 local time.
    The time elapsed between 01:00 CET and 03:00 CEST is exactly 1 hour.
    """
    # Raw data skipping 02:00 because it doesn't exist in Amsterdam on this day
    raw_data = (
        "timestamp,DayAheadPrice\n"
        "2024-03-31 01:00:00+01:00,50.0\n" # 00:00 UTC
        "2024-03-31 03:00:00+02:00,60.0\n" # 01:00 UTC
    )
    
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df_result = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    # 1 hour elapsed at 15-minute intervals = exactly 5 data points
    # (00:00, 00:15, 00:30, 00:45, 01:00 in UTC)
    assert len(df_result) == 5
    
    # Verify the step-function ffill(limit=3) worked perfectly
    # The :15, :30, and :45 marks should carry the 50.0 price
    assert df_result.iloc[1]['DayAheadPrice'] == 50.0 
    assert df_result.iloc[3]['DayAheadPrice'] == 50.0
    
    # The final point is the new hour price
    assert df_result.iloc[4]['DayAheadPrice'] == 60.0

def test_clean_energy_data_fall_back():
    """
    Test Autumn DST (October): Clocks jump back from 03:00 to 02:00.
    The day has 25 hours. There are two '02:00' timestamps with different offsets.
    """
    # 01:00 CEST to 03:00 CET is a span of exactly 3 hours (23:00 UTC to 02:00 UTC)
    raw_data = (
        "timestamp,DayAheadPrice\n"
        "2024-10-27 01:00:00+02:00,40.0\n" # 23:00 UTC
        "2024-10-27 02:00:00+02:00,42.0\n" # 00:00 UTC
        "2024-10-27 02:00:00+01:00,45.0\n" # 01:00 UTC
        "2024-10-27 03:00:00+01:00,38.0\n" # 02:00 UTC
    )
    
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df_result = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    # 3 hours at 15-min intervals = 12 intervals + 1 closing point = 13 records
    assert len(df_result) == 13
    assert len(df_result.index.unique()) == 13

def test_clean_energy_data_price_step_function():
    """
    Verify that ffill patches the intra-hour 15-min gaps (limit=3), 
    but correctly leaves actual missing hours as NaN to prevent quant hallucinations.
    """
    raw_data = (
        "timestamp,DayAheadPrice\n"
        "2026-03-11 00:00:00+01:00,100.0\n"
        # 01:00 Local Time is completely missing
        "2026-03-11 02:00:00+01:00,120.0\n"
    )
    
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df_result = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    # 00:00, 00:15, 00:30, 00:45 UTC should be 100.0
    assert df_result.iloc[1]['DayAheadPrice'] == 100.0
    assert df_result.iloc[3]['DayAheadPrice'] == 100.0
    
    # 01:00 Local Time (00:00 UTC) and its quarters should be NaN because limit=3 was exhausted
    assert pd.isna(df_result.iloc[4]['DayAheadPrice'])
    assert pd.isna(df_result.iloc[7]['DayAheadPrice'])

def test_clean_energy_data_continuous_interpolation():
    """
    Verify that continuous variables (like Load) use time-based linear interpolation 
    instead of forward-filling.
    """
    raw_data = (
        "timestamp,Actual Load\n"
        "2026-03-11 00:00:00+01:00,1000.0\n"
        # Missing 00:15
        # Missing 00:30
        "2026-03-11 00:45:00+01:00,1300.0\n"
    )
    
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df_result = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    # It should draw a perfect line: 1000 -> 1100 -> 1200 -> 1300
    assert df_result.iloc[0]['Actual Load'] == 1000.0
    assert df_result.iloc[1]['Actual Load'] == 1100.0
    assert df_result.iloc[2]['Actual Load'] == 1200.0
    assert df_result.iloc[3]['Actual Load'] == 1300.0