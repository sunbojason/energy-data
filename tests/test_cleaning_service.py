import pytest
import pandas as pd
import io
from shared_logic.cleaning_service import CleaningService

def test_clean_energy_data_spring_forward():
    """
    Test Spring DST (March): Clocks jump from 02:00 to 03:00.
    The day has only 23 hours. 
    Resample should NOT create a record for 02:00.
    """
    tz = 'Europe/Amsterdam'
    # Raw data skipping 02:00 because it doesn't exist in Amsterdam on this day
    raw_data = (
        ",DayAheadPrice\n"
        "2024-03-31 01:00:00+01:00,50.0\n"
        "2024-03-31 03:00:00+02:00,60.0\n"
    )
    
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df_result = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    # In a 23-hour day, we expect exactly 2 records here 
    # (01:00 CET and 03:00 CEST)
    assert len(df_result) == 2
    # Verify 02:00 was not "hallucinated" by resample
    assert "2024-03-31 02:00:00" not in df_result.index.astype(str).values

def test_clean_energy_data_fall_back():
    """
    Test Autumn DST (October): Clocks jump back from 03:00 to 02:00.
    The day has 25 hours. There are two '02:00' timestamps.
    """
    tz = 'Europe/Amsterdam'
    # Raw data including the "double hour" at 02:00
    # 02:00+02:00 (CEST) and 02:00+01:00 (CET)
    raw_data = (
        ",DayAheadPrice\n"
        "2024-10-27 01:00:00+02:00,40.0\n"
        "2024-10-27 02:00:00+02:00,42.0\n" # First 02:00
        "2024-10-27 02:00:00+01:00,45.0\n" # Second 02:00
        "2024-10-27 03:00:00+01:00,38.0\n"
    )
    
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df_result = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    # We should have all 4 distinct timestamps preserved
    assert len(df_result) == 4
    # Ensure deduplication logic (keep='first') didn't accidentally kill the second 02:00
    # because they have different UTC offsets, they are NOT duplicates.
    assert len(df_result.index.unique()) == 4

def test_clean_energy_data_ffill_limit():
    """
    Verify that ffill patches small gaps but leaves large gaps as NaN.
    Essential for quant safety.
    """
    raw_data = (
        ",DayAheadPrice\n"
        "2026-03-11 00:00:00+01:00,100.0\n"
        # 01:00 Missing
        # 02:00 Missing
        # 03:00 Missing
        "2026-03-11 04:00:00+01:00,120.0\n"
    )
    
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df_result = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    # 01:00 and 02:00 should be filled (limit=2)
    assert df_result.loc["2026-03-11 01:00:00+01:00", "DayAheadPrice"] == 100.0
    assert df_result.loc["2026-03-11 02:00:00+01:00", "DayAheadPrice"] == 100.0
    
    # 03:00 should remain NaN (limit 2 exhausted)
    assert pd.isna(df_result.loc["2026-03-11 03:00:00+01:00", "DayAheadPrice"])