import os
import logging
import struct
import pandas as pd
from sqlalchemy import create_engine, event, types
from azure.identity import DefaultAzureCredential

class DatabaseService:
    def __init__(self):
        self.server = os.environ.get("SQL_SERVER_NAME")
        self.database = os.environ.get("SQL_DATABASE_NAME")
        # Ensure the ODBC Driver 18 is installed in your runtime/local environment
        self.driver = "{ODBC Driver 18 for SQL Server}" 
        
        if not self.server or not self.database:
            raise ValueError("Database configuration missing in environment variables.")

        # Connection string tailored for Entra ID (Managed Identity) Token auth
        self.conn_str = (
            f"DRIVER={self.driver};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30"
        )
        
        # Initialize SQLAlchemy engine optimized for bulk inserts
        self.engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={self.conn_str}", 
            fast_executemany=True
        )

        # Intercept connection to inject the OAuth token securely
        @event.listens_for(self.engine, "do_connect")
        def provide_token(dialect, conn_rec, cargs, cparams):
            logging.info("Requesting Entra ID token for Azure SQL authentication...")
            credential = DefaultAzureCredential()
            # The exact scope required for Azure SQL
            token_object = credential.get_token("https://database.windows.net/.default")
            
            # Format the token for ODBC driver consumption (UTF-16 Little Endian)
            token_as_bytes = token_object.token.encode("utf-16-le")
            encoded_token = struct.pack(f"<I{len(token_as_bytes)}s", len(token_as_bytes), token_as_bytes)
            
            # SQL_COPT_SS_ACCESS_TOKEN = 1256
            cparams["attrs_before"] = {1256: encoded_token}

    def _ensure_table_schema(self, df: pd.DataFrame, table_name: str):
        """
        Detects columns in the DataFrame that are missing from the SQL table
        and adds them via ALTER TABLE. Only supports FLOAT for new columns
        to maintain compatibility with the energy data pipeline.
        """
        from sqlalchemy import inspect, text
        
        inspector = inspect(self.engine)
        if not inspector.has_table(table_name):
            logging.info(f"Table '{table_name}' does not exist yet. It will be created by to_sql.")
            return

        existing_columns = {col['name'].lower() for col in inspector.get_columns(table_name)}
        df_columns = [c for c in df.columns if c.lower() not in existing_columns]

        if not df_columns:
            return

        logging.info(f"Schema mismatch detected. Adding {len(df_columns)} missing columns to '{table_name}'.")
        
        with self.engine.begin() as conn:
            for col in df_columns:
                # SQL Server requires brackets for column names with special characters or reserved words
                alter_stmt = text(f'ALTER TABLE {table_name} ADD [{col}] FLOAT')
                try:
                    conn.execute(alter_stmt)
                    logging.info(f"Successfully added column [{col}] to '{table_name}'.")
                except Exception as ex:
                    logging.warning(f"Failed to add column [{col}]: {ex}")

    def upsert_energy_data(self, df: pd.DataFrame, table_name: str = "entsoe"):
        """
        Inserts the cleaned DataFrame into Azure SQL.
        Synchronizes schema before insertion to prevent 'Invalid Column' errors.
        """
        if df is None or df.empty:
            logging.warning("Execution halted: Empty DataFrame provided to DatabaseService.")
            return

        try:
            df_to_insert = df.copy()
            
            # Ensure index is handled if not already done in client
            if isinstance(df_to_insert.index, pd.DatetimeIndex):
                df_to_insert.index.name = 'Time_UTC'
                df_to_insert = df_to_insert.reset_index()
            
            # Double check for naming conflicts and redundant columns
            df_to_insert.columns = [str(c) for c in df_to_insert.columns]
            df_to_insert.rename(columns={'timestamp': 'Time_UTC', 'index': 'Time_UTC'}, inplace=True)
            df_to_insert = df_to_insert.loc[:, ~df_to_insert.columns.str.contains('^Unnamed')]
            
            # 1. SYNCHRONIZE SCHEMA
            self._ensure_table_schema(df_to_insert, table_name)

            logging.info(f"Inserting {len(df_to_insert)} rows into table '{table_name}'.")

            # Force Time_UTC to be DATETIME2 to avoid SQL Server 'TIMESTAMP' (rowversion) conflict
            dtype_map = {'Time_UTC': types.DateTime()}

            df_to_insert.to_sql(
                name=table_name, 
                con=self.engine, 
                if_exists='append', 
                index=False,
                dtype=dtype_map
            )
            
            logging.info("Database transaction completed successfully.")
            
        except Exception as e:
            logging.error(f"FATAL: Database operation failed. Details: {str(e)}")
            raise