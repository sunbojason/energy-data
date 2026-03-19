import os
import logging
import struct
import pandas as pd
from sqlalchemy import create_engine, event, types
from azure.identity import DefaultAzureCredential

class DatabaseService:
    def __init__(self):
        self._initialize_configuration()
        self._initialize_engine()
        self._setup_authentication_listener()

    def _initialize_configuration(self):
        self.server = os.environ.get("SQL_SERVER_NAME")
        self.database = os.environ.get("SQL_DATABASE_NAME")
        self.driver = "{ODBC Driver 18 for SQL Server}" 
        
        if not self.server or not self.database:
            raise ValueError("Database configuration missing in environment variables.")

        self.conn_str = (
            f"DRIVER={self.driver};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30"
        )

    def _initialize_engine(self):
        self.engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={self.conn_str}", 
            fast_executemany=True
        )

    def _setup_authentication_listener(self):
        event.listen(self.engine, "do_connect", self._inject_entra_token)

    def _inject_entra_token(self, dialect, conn_rec, cargs, cparams):
        token_bytes = self._acquire_aad_token()
        # SQL_COPT_SS_ACCESS_TOKEN = 1256
        cparams["attrs_before"] = {1256: token_bytes}

    def _acquire_aad_token(self) -> bytes:
        logging.info("Acquiring Entra ID token for Azure SQL...")
        credential = DefaultAzureCredential()
        token_object = credential.get_token("https://database.windows.net/.default")
        
        token_as_bytes = token_object.token.encode("utf-16-le")
        return struct.pack(f"<I{len(token_as_bytes)}s", len(token_as_bytes), token_as_bytes)

    def _ensure_table_schema(self, df: pd.DataFrame, table_name: str):
        from sqlalchemy import inspect, text
        
        inspector = inspect(self.engine)
        schema = 'dbo'  # Default for Azure SQL
        
        if not inspector.has_table(table_name, schema=schema):
            logging.info(f"Table '{table_name}' not found in schema '{schema}'. Skipping manual schema sync.")
            return

        missing_columns = self._get_missing_columns(df, inspector, table_name, schema=schema)
        if not missing_columns:
            return

        self._add_missing_columns(table_name, missing_columns)

    def _get_missing_columns(self, df: pd.DataFrame, inspector, table_name: str, schema: str = 'dbo') -> list:
        existing_cols = {col['name'].lower() for col in inspector.get_columns(table_name, schema=schema)}
        return [c for c in df.columns if c.lower() not in existing_cols]

    def _add_missing_columns(self, table_name: str, columns: list):
        from sqlalchemy import text
        logging.info(f"Adding {len(columns)} missing columns to '{table_name}'.")
        
        with self.engine.begin() as conn:
            for col in columns:
                col_type = 'DATETIME2' if col.lower() == 'time_local' else 'FLOAT'
                alter_stmt = text(f'ALTER TABLE [dbo].[{table_name}] ADD [{col}] {col_type}')
                try:
                    conn.execute(alter_stmt)
                except Exception as ex:
                    logging.warning(f"Failed to add column [{col}]: {ex}")

    def _delete_existing_records(self, table_name: str, time_utc_list: list):
        from sqlalchemy import text, inspect, bindparam
        if not time_utc_list:
            return
            
        inspector = inspect(self.engine)
        if not inspector.has_table(table_name, schema='dbo'):
            return

        # Convert potentially diverse timestamp formats to standard Python datetimes
        py_timestamps = [pd.to_datetime(t).to_pydatetime() for t in time_utc_list]

        logging.info(f"Removing existing records for {len(py_timestamps)} timestamps from '{table_name}'.")
        # Use expanding=True for SQLAlchemy to handle the list correctly in the IN clause
        sql = text(f"DELETE FROM [dbo].[{table_name}] WHERE [Time_UTC] IN :timestamps")
        sql = sql.bindparams(bindparam("timestamps", expanding=True))
        
        with self.engine.begin() as conn:
            conn.execute(sql, {"timestamps": py_timestamps})

    def upsert_energy_data(self, df: pd.DataFrame, table_name: str = "entsoe"):
        if df is None or df.empty:
            logging.warning("Upsert aborted: Empty DataFrame.")
            return

        try:
            df_prepared = self._prepare_dataframe_for_sql(df)
            self._ensure_table_schema(df_prepared, table_name)
            
            # Idempotency: Remove existing records before append to simulate an "Upsert"
            if 'Time_UTC' in df_prepared.columns:
                self._delete_existing_records(table_name, df_prepared['Time_UTC'].tolist())

            logging.info(f"Buffered upsert: {len(df_prepared)} rows to '{table_name}'.")

            df_prepared.to_sql(
                name=table_name, 
                con=self.engine, 
                schema='dbo',
                if_exists='append', 
                index=False,
                dtype={
                    'Time_UTC': types.DateTime(),
                    'Time_Local': types.DateTime()
                }
            )
        except Exception as e:
            logging.error(f"Database operation failed: {str(e)}")
            raise

    def _prepare_dataframe_for_sql(self, df: pd.DataFrame) -> pd.DataFrame:
        df_out = df.copy()
        
        # Consistent Column Naming: Handle common timestamp variations case-insensitively
        new_columns = []
        for col in df_out.columns:
            col_str = str(col).strip()
            if col_str.lower() in ('timestamp', 'time_utc', 'index'):
                new_columns.append('Time_UTC')
            else:
                new_columns.append(col_str)
        df_out.columns = new_columns

        if isinstance(df_out.index, pd.DatetimeIndex):
            df_out.index.name = 'Time_UTC'
            df_out = df_out.reset_index()
            # If resetting index created a duplicate Time_UTC, keep the index one
            df_out = df_out.loc[:, ~df_out.columns.duplicated(keep='first')]
        
        # Type Safety: Ensure date columns are formatted correctly for pyodbc + fast_executemany
        if 'Time_UTC' in df_out.columns:
            df_out['Time_UTC'] = pd.to_datetime(df_out['Time_UTC'], utc=True)
        if 'Time_Local' in df_out.columns:
            df_out['Time_Local'] = pd.to_datetime(df_out['Time_Local'])

        return df_out.loc[:, ~df_out.columns.str.contains('^Unnamed')]