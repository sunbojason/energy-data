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
            # 1. Load data
            df = pd.read_csv(io.StringIO(raw_csv_content), index_col=0)
            
            # 2. FORCE conversion to DatetimeIndex (The fix for your TypeError)
            # This ensures resample() knows it is dealing with time.
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index, utc=True)

            if df.empty:
                return ""

            # 3. Sort and Deduplicate
            df = df.sort_index()
            df = df[~df.index.duplicated(keep='first')]

            # 4. Standardize Frequency
            # Now resample() will work because the index is guaranteed to be a DatetimeIndex
            df = df.resample('h').asfreq()

            # 5. Handle missing values
            df = df.ffill(limit=2)

            return df.to_csv(index=True)

        except Exception as e:
            logger.error(f"Data cleaning failed: {str(e)}")
            raise