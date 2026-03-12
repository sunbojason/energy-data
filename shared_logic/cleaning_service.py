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
            df = pd.read_csv(io.StringIO(raw_csv_content), index_col=0)
            
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index, utc=True)

            if df.empty:
                return ""

            df = df.sort_index()
            df = df[~df.index.duplicated(keep='first')]

            df = df.resample('h').asfreq()

            df = df.ffill(limit=2)

            return df.to_csv(index=True)

        except Exception as e:
            logger.error(f"Data cleaning failed: {str(e)}")
            raise