import pytest
import pandas as pd
import numpy as np
import io
from shared_logic.cleaning_service import CleaningService
from shared_logic.constants import DEFAULT_FREQ_GRID

def test_clean_all_nan_columns():
    """Verify that columns with only NaN are handled correctly."""
    raw_data = (
        "Time_UTC,DA_Price,Load_Actual\n"
        "2026-03-11 00:00:00+00:00,,1000.0\n"
        "2026-03-11 00:15:00+00:00,,\n"
        "2026-03-11 00:30:00+00:00,,1200.0\n"
    )
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    assert df['DA_Price'].isna().all()
    # Load_Actual should now be NULL at 00:15 per strict policy
    assert pd.isna(df.loc['2026-03-11 00:15:00+00:00', 'Load_Actual'])

def test_clean_mixed_frequencies():
    raw_data = (
        "Time_UTC,DA_Price\n"
        "2026-03-11 00:00:00+00:00,50.0\n"
        "2026-03-11 01:00:00+00:00,60.0\n"
    )
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    # Should have 5 timestamps (00, 15, 30, 45, 60 mins)
    assert len(df) == 5
    # DA_Price should be forward-filled because it's in step_prefixes
    assert df.loc['2026-03-11 00:15:00+00:00', 'DA_Price'] == 50.0
    assert df.loc['2026-03-11 00:45:00+00:00', 'DA_Price'] == 50.0

def test_time_local_dst_overlap_uniqueness():
    raw_data = (
        "Time_UTC,DA_Price\n"
        "2024-10-27 00:00:00+00:00,40.0\n"
        "2024-10-27 01:00:00+00:00,45.0\n"
    )
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    assert df.index.is_unique
    # 02:00 local time repeat
    local_times = df['Time_Local'].tolist()
    assert local_times[0] == '2024-10-27 02:00:00'
    assert local_times[4] == '2024-10-27 02:00:00'

def test_clean_non_standard_column_names():
    raw_data = (
        "timestamp,DA_Price\n"
        "2026-03-11 00:00:00+00:00,50.0\n"
        "2026-03-11 00:15:00+00:00,55.0\n"
    )
    cleaned_csv = CleaningService.clean_energy_data(raw_data)
    df = pd.read_csv(io.StringIO(cleaned_csv), index_col=0, parse_dates=True)
    
    assert isinstance(df.index, pd.DatetimeIndex)
    assert 'DA_Price' in df.columns
