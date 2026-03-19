import pandas as pd
import io
import logging
import re

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
        if not raw_csv_content or not raw_csv_content.strip():
            return ""

        try:
            df = CleaningService._load_raw_data(raw_csv_content)
            df = CleaningService._standardize_time_index(df)
            
            if df.empty:
                return ""

            df = CleaningService._apply_structural_discipline(df)
            df = CleaningService._align_to_grid(df)
            df = CleaningService._add_local_belgian_time(df)
            
            df = CleaningService._apply_filling_strategies(df)
            df = CleaningService._prune_sparse_metrics(df)
            df = CleaningService._finalize_refinement(df)

            return df.to_csv(index=True)

        except Exception as e:
            logger.error(f"Data cleaning pipeline failed: {str(e)}")
            raise

    @staticmethod
    def _load_raw_data(content: str) -> pd.DataFrame:
        return pd.read_csv(io.StringIO(content), index_col=0)

    @staticmethod
    def _standardize_time_index(df: pd.DataFrame) -> pd.DataFrame:
        time_indicators = ['Time_UTC', 'timestamp']
        found_column = next((col for col in time_indicators if col in df.columns), None)

        if found_column:
            df.index = pd.to_datetime(df[found_column], utc=True)
            df = df.drop(columns=[found_column])
        elif not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, utc=True)
        return df

    @staticmethod
    def _apply_structural_discipline(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_index()
        return df[~df.index.duplicated(keep='first')]

    @staticmethod
    def _align_to_grid(df: pd.DataFrame) -> pd.DataFrame:
        return df.resample(DEFAULT_FREQ_GRID).asfreq()

    @staticmethod
    def _add_local_belgian_time(df: pd.DataFrame) -> pd.DataFrame:
        # Business Rule: Belgian market operations use Europe/Brussels wall-clock time.
        df['Time_Local'] = df.index.tz_convert('Europe/Brussels').tz_localize(None)
        return df

    @staticmethod
    def _apply_filling_strategies(df: pd.DataFrame) -> pd.DataFrame:
        # Step-Function: Metrics that remain constant across an hour or represent 
        # market commitments/forecasts. We forward-fill to map 60-min data to 15-min ISPs.
        step_patterns = ('DA_', 'NTC_', 'ResPrice_', 'ResCap_', 'ResAmt_', 'AggBids_', 'SchedExc_', 'Forecast', 'Fc_')
        
        step_cols = [c for c in df.columns if any(p in c for p in step_patterns) or 'Price' in c]
        if step_cols:
            df[step_cols] = df[step_cols].ffill(limit=3)

        # Power System Core Rule: "NULL if NULL".
        # Physical signals (Actual Load, Gen, Flows) must NOT be interpolated/filled.
        return df

    @staticmethod
    def _prune_sparse_metrics(df: pd.DataFrame) -> pd.DataFrame:
        # Drop plant-level data if coverage is below 20% to avoid hallucinating sparse signals.
        sparse_prefixes = ('GenPlant_',)
        sparse_cols = [c for c in df.columns if c.startswith(sparse_prefixes)]
        if sparse_cols:
            threshold = int(len(df) * 0.2)
            cols_to_drop = [c for c in sparse_cols if df[c].count() < threshold]
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
        return df

    @staticmethod
    def _finalize_refinement(df: pd.DataFrame) -> pd.DataFrame:
        # Final safety cleanup for column headers to ensure no redundant suffixes like _0 survive.
        df.columns = [re.sub(r'_\d+\s*$', '', str(col)).strip() for col in df.columns]

        # De-duplicate any newly colliding names and drop index artifacts
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.loc[:, ~df.columns.str.contains('^Unnamed|^index')]

        return df