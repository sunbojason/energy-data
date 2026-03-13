import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)

class CleaningService:
    @staticmethod
    def clean_energy_data(raw_csv_content: str) -> str:
        if not raw_csv_content or len(raw_csv_content.strip()) == 0:
            return ""

        try:
            # 1. Read Data
            df = pd.read_csv(io.StringIO(raw_csv_content), index_col=0)
            
            # 2. Enforce UTC Timezone for Database Consistency
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index, utc=True)

            if df.empty:
                return ""

            # 3. Sort and Deduplicate
            df = df.sort_index()
            df = df[~df.index.duplicated(keep='first')]

            # 4. ALIGNMENT: Upsample to 15-minute frequency to prevent data loss
            # '15min' creates a continuous grid of 15-minute intervals
            df = df.resample('15min').asfreq()

            # 5. COLUMN-SPECIFIC FILLING STRATEGY
            
            # Strategy A: Step Functions (Market Prices)
            # Day-Ahead clears hourly. The XX:00 price is valid for :15, :30, and :45.
            if 'DayAheadPrice' in df.columns:
                df['DayAheadPrice'] = df['DayAheadPrice'].ffill(limit=3)

            # Strategy B: Continuous Functions (Physical Grid Load & Flows)
            # If a 15-min interval is missing, interpolate the physical curve.
            continuous_cols = [col for col in df.columns if col != 'DayAheadPrice']
            if continuous_cols:
                # Interpolate up to 2 missing 15-min blocks (30 mins of missing data)
                df[continuous_cols] = df[continuous_cols].interpolate(method='time', limit=2)

            return df.to_csv(index=True)

        except Exception as e:
            logger.error(f"Data cleaning failed: {str(e)}")
            raise