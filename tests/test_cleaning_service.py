import pytest
import pandas as pd
import numpy as np
import io
from shared_logic.cleaning_service import CleaningService

def test_spring_dst_transition_hourly_resampling():
    """
    Business Logic: Clocks jump from 02:00 to 03:00 CET in March (Spring Forward).
    Test Case: Verify that 1-hour samples across this gap are correctly upsampled
    to 15-minute intervals.
    """
    raw_csv = (
        "timestamp,DA_Price\n"
        "2024-03-31 01:00:00+01:00,50.0\n" # 00:00 UTC
        "2024-03-31 03:00:00+02:00,60.0\n" # 01:00 UTC
    )
    
    cleaned_string = CleaningService.clean_energy_data(raw_csv)
    result_df = pd.read_csv(io.StringIO(cleaned_string), index_col=0, parse_dates=True)
    
    # 1 hour elapsed (00:00 to 01:00 UTC) = 5 data points on a 15-min grid.
    assert len(result_df) == 5
    
    # DA_ (Day-Ahead) prices are constant for the hour. ffill(limit=3) must populate the 15-min buckets.
    assert result_df.iloc[1]['DA_Price'] == 50.0 
    assert result_df.iloc[3]['DA_Price'] == 50.0
    assert result_df.iloc[4]['DA_Price'] == 60.0

def test_autumn_dst_transition_uniqueness():
    """
    Business Logic: Clocks jump back from 03:00 to 02:00 CEST in October (Fall Back).
    """
    raw_csv = (
        "timestamp,DA_Price\n"
        "2024-10-27 01:00:00+02:00,40.0\n" # 23:00 UTC
        "2024-10-27 02:00:00+02:00,42.0\n" # 00:00 UTC
        "2024-10-27 02:00:00+01:00,45.0\n" # 01:00 UTC
        "2024-10-27 03:00:00+01:00,38.0\n" # 02:00 UTC
    )
    
    cleaned_string = CleaningService.clean_energy_data(raw_csv)
    result_df = pd.read_csv(io.StringIO(cleaned_string), index_col=0, parse_dates=True)
    
    # 3 UTC hours elapsed = 13 records on 15-min grid.
    assert len(result_df) == 13
    assert len(result_df.index.unique()) == 13

def test_price_step_function_filling_limits():
    """
    Business Logic: Prices (DA_) are semi-static. We fill up to 45 mins (limit=3)
    to patch sub-hourly upsampling, but retain NaN for major outages.
    """
    raw_csv = (
        "timestamp,DA_Price\n"
        "2026-03-11 00:00:00+01:00,100.0\n"
        "2026-03-11 02:00:00+01:00,120.0\n" # 1-hour gap
    )
    
    cleaned_string = CleaningService.clean_energy_data(raw_csv)
    result_df = pd.read_csv(io.StringIO(cleaned_string), index_col=0, parse_dates=True)
    
    # Sub-hourly gap: 00:15, 00:30, 00:45 should be forward-filled.
    assert result_df.iloc[1]['DA_Price'] == 100.0
    assert result_df.iloc[3]['DA_Price'] == 100.0
    
    # Major gap: The 01:00 local slot exceeds limit=3 and must remain NaN.
    assert pd.isna(result_df.iloc[4]['DA_Price'])
    assert pd.isna(result_df.iloc[7]['DA_Price'])

def test_physical_metric_linear_interpolation_disabled():
    """
    Business Logic: Physical metrics should NOT be interpolated per "NULL if NULL" policy.
    """
    raw_csv = (
        "timestamp,Load_Actual\n"
        "2026-03-11 00:00:00+01:00,1000.0\n"
        "2026-03-11 00:45:00+01:00,1300.0\n"
    )
    
    cleaned_string = CleaningService.clean_energy_data(raw_csv)
    result_df = pd.read_csv(io.StringIO(cleaned_string), index_col=0, parse_dates=True)
    
    # Gap (00:15, 00:30) should now be NULL instead of interpolated.
    assert pd.isna(result_df.iloc[1]['Load_Actual'])
    assert pd.isna(result_df.iloc[2]['Load_Actual'])

def test_invalid_and_empty_input_handling():
    assert CleaningService.clean_energy_data("") == ""
    assert CleaningService.clean_energy_data("   ") == ""

def test_ntc_capacity_step_function_resampling():
    """
    Business Logic: NTC upsampling works with step-function ffill.
    """
    raw_csv = (
        "timestamp,NTC_Week_FR_DE_LU_value\n"
        "2026-03-11 00:00:00+01:00,1500.0\n"
        "2026-03-11 01:00:00+01:00,1600.0\n"
    )
    cleaned_string = CleaningService.clean_energy_data(raw_csv)
    result_df = pd.read_csv(io.StringIO(cleaned_string), index_col=0, parse_dates=True)

    assert result_df.iloc[1]['NTC_Week_FR_DE_LU_value'] == 1500.0
    assert result_df.iloc[4]['NTC_Week_FR_DE_LU_value'] == 1600.0

def test_sparse_plant_level_data_pruning():
    sparse_raw_csv = (
        "timestamp,GenPlant_PlantA\n"
        "2026-03-11 00:00:00+01:00,1000.0\n" +
        "\n".join([f"2026-03-11 0{i}:00:00+01:00," for i in range(1, 8)])
    )
    cleaned_string = CleaningService.clean_energy_data(sparse_raw_csv)
    result_df = pd.read_csv(io.StringIO(cleaned_string), index_col=0, parse_dates=True)

    assert len(result_df) > 0

def test_legacy_unnamed_index_identification():
    from shared_logic.entsoe_client import EntsoeDataClient
    
    raw_df = pd.DataFrame({
        'DA_Price': [50.0, 55.0, 60.0]
    }, index=pd.date_range('2024-01-01', periods=3, freq='15min', tz='UTC'))
    
    flattened_df = EntsoeDataClient().finalize_dataframe_structure(raw_df)
    csv_payload = flattened_df.to_csv(index=True)
    
    cleaned_string = CleaningService.clean_energy_data(csv_payload)
    result_df = pd.read_csv(io.StringIO(cleaned_string), index_col=0, parse_dates=True)
    
    assert isinstance(result_df.index, pd.DatetimeIndex)