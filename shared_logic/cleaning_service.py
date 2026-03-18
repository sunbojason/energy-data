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
            # 1. Read Data with Timezone Awareness
            # The index is expected to be a UTC timestamp for database consistency.
            df = pd.read_csv(io.StringIO(raw_csv_content), index_col=0)
            
            if not isinstance(df.index, pd.DatetimeIndex):
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
            
            # Strategy A: Market-Clearing Prices (Step Functions)
            # Day-ahead prices are fixed for 60 mins. We forward-fill (ffill)
            # the hourly value into the subsequent 15-min buckets.
            price_cols = [col for col in df.columns if col.startswith('DA_')]
            if price_cols:
                # Limit to 3 intervals (45 mins) to fill a single hour block
                df[price_cols] = df[price_cols].ffill(limit=3)

            # Strategy B: Physical Grid Metrics (Continuous Curves)
            # Load, Generation, and Cross-border flows are physical real-time signals.
            # Time-based interpolation is more accurate for missing physical snapshots.
            physical_prefixes = ('Load_', 'Imb_', 'Export_', 'Import_', 'BalState_')
            continuous_cols = [
                col for col in df.columns 
                if col.startswith(physical_prefixes) or col.endswith('_Sum')
            ]
            
            if continuous_cols:
                # Interpolate missing physical data up to 30 mins (2 intervals)
                # method='time' ensures spacing is respected even if timestamps are non-linear
                df[continuous_cols] = df[continuous_cols].interpolate(
                    method='time', 
                    limit=2
                )

            # 5. FINAL REFINEMENT
            zero_fill_cols = [col for col in df.columns if col.endswith('_Sum')]
            if zero_fill_cols:
                df[zero_fill_cols] = df[zero_fill_cols].fillna(0.0)

            # Export back to CSV for downstream Blob Storage or SQL ingestion
            return df.to_csv(index=True)

        except Exception as e:
            logger.error(f"Data cleaning pipeline failed: {str(e)}")
            raise