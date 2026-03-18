import pandas as pd
import io
import logging

try:
    from shared_logic.constants import DEFAULT_FREQ_GRID
except ImportError:
    from constants import DEFAULT_FREQ_GRID

logger = logging.getLogger(__name__)

class CleaningService:
    """
    Standardizes and cleans multi-source energy data.
    Ensures structural integrity for the Azure SQL Database schema.
    """

    @staticmethod
    def clean_energy_data(raw_csv_content: str) -> str:
        if not raw_csv_content or len(raw_csv_content.strip()) == 0:
            return ""
        try:
            # 1. Read Data - We read with index_col=0 to handle standard CSVs, 
            # but then check if the actual time is tucked in a named column.
            df = pd.read_csv(io.StringIO(raw_csv_content), index_col=0)
            
            # Robust Index Identification: 
            # If EntsoeDataClient reset the index, the true time is in 'Time_UTC'.
            # If it's a legacy or test-generated CSV, it might be in 'timestamp'.
            time_cols = ['Time_UTC', 'timestamp']
            found_time_col = next((c for c in time_cols if c in df.columns), None)

            if found_time_col:
                df.index = pd.to_datetime(df[found_time_col], utc=True)
                df = df.drop(columns=[found_time_col])
            elif not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index, utc=True)

            if df.empty:
                return ""

            # 2. Structural Discipline: Sort and Deduplicate
            # Removes potential overlaps from overlapping API fetch windows.
            df = df.sort_index()
            df = df[~df.index.duplicated(keep='first')]

            # 3. ALIGNMENT: Enforce 15-minute Grid
            # Resampling to '15min' aligns all features (Prices, Load, Flows) 
            # to the standard Dutch/Belgian imbalance settlement period (ISP).
            df = df.resample(DEFAULT_FREQ_GRID).asfreq()

            # 4. ASSET-BASED FILLING STRATEGY
            # Different energy assets require different mathematical treatments.
            
            # Strategy A: Step-Function (Forward-Fill)
            # Used for: Day-ahead prices, NTC, Reserve prices/amounts, and Aggregated bids.
            # These values are typically constant for an entire hour (or longer)
            # and should be carried forward into sub-period buckets.
            step_function_prefixes = ('DA_', 'NTC_', 'ResPrice_', 'ResCap_', 'ResAmt_', 'AggBids_')
            step_cols = [col for col in df.columns if col.startswith(step_function_prefixes)]
            if step_cols:
                # Limit to 3 intervals (45 mins) to fill a single hour block
                df[step_cols] = df[step_cols].ffill(limit=3)

            # Strategy B: Physical Grid Metrics (Continuous Interpolation)
            # Used for: actual load, generation, cross-border flows, imbalance,
            # net position, wind/solar forecasts, and scheduled exchanges.
            # Time-based interpolation respects the physical continuity of these signals.
            continuous_prefixes = (
                'Load_', 'Imb_', 'Export_', 'Import_', 'BalState_',
                'Gen_', 'GenFc_', 'WS_Fc_', 'WS_ID_Fc_',
                'NetPos_', 'SchedExc_', 'PhysFlow_Export_',
            )
            continuous_cols = [
                col for col in df.columns
                if col.startswith(continuous_prefixes) or col.endswith('_Sum')
            ]
            if continuous_cols:
                # Interpolate up to 30 mins (2 intervals); method='time' respects spacing
                df[continuous_cols] = df[continuous_cols].interpolate(
                    method='time',
                    limit=2
                )

            # Strategy C: Sparse Per-Plant / Per-Border Data
            # query_generation_per_plant and query_import return wide, sparse datasets.
            # Drop any column that is >80% NaN to avoid hallucinating plant-level signals.
            sparse_prefixes = ('GenPlant_',)
            sparse_cols = [col for col in df.columns if col.startswith(sparse_prefixes)]
            if sparse_cols:
                threshold = int(len(df) * 0.2)  # must have at least 20% real values
                cols_to_drop = [c for c in sparse_cols if df[c].count() < threshold]
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)

            # 5. FINAL REFINEMENT
            # Zero-fill aggregated sum columns and Net Position (can legitimately be 0).
            zero_fill_cols = [
                col for col in df.columns
                if col.endswith('_Sum') or col.startswith('NetPos_')
            ]
            if zero_fill_cols:
                df[zero_fill_cols] = df[zero_fill_cols].fillna(0.0)

            # Export back to CSV for downstream Blob Storage or SQL ingestion
            return df.to_csv(index=True)

        except Exception as e:
            logger.error(f"Data cleaning pipeline failed: {str(e)}")
            raise