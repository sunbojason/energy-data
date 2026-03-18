import os
import logging
import struct
import pandas as pd
from sqlalchemy import create_engine, event
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

    def upsert_energy_data(self, df: pd.DataFrame, table_name: str = "entsoe"):
        """
        Inserts the cleaned DataFrame into Azure SQL with strict keyword guarding.
        """
        if df is None or df.empty:
            logging.warning("Execution halted: Empty DataFrame provided to DatabaseService.")
            return

        try:
            # 1. Defensive copy
            df_to_insert = df.copy()
            
            # 2. Handle the Index (The "Time Preservation" step)
            # If time is in the index, move it to a real column named 'Time_UTC'
            if isinstance(df_to_insert.index, pd.DatetimeIndex):
                df_to_insert.index.name = 'Time_UTC'
                df_to_insert = df_to_insert.reset_index()
            
            # 3. Clean up Column Names (The "Anti-Conflict" step)
            # Rename 'timestamp' to 'Time_UTC' to avoid SQL Server keyword error
            # Also remove any 'Unnamed' columns leaked from CSV/Index issues
            df_to_insert.columns = [str(c) for c in df_to_insert.columns]
            
            rename_map = {
                'timestamp': 'Time_UTC',
                'index': 'Time_UTC'
            }
            df_to_insert.rename(columns=rename_map, inplace=True)
            
            # Remove redundant 'Unnamed' columns
            cols_to_keep = [c for c in df_to_insert.columns if not c.startswith('Unnamed')]
            df_to_insert = df_to_insert[cols_to_keep]

            logging.info(f"Inserting {len(df_to_insert)} rows. Columns: {list(df_to_insert.columns)}")

            # 4. Use index=False because time is now a regular column 'Time_UTC'
            df_to_insert.to_sql(
                name=table_name, 
                con=self.engine, 
                if_exists='append', 
                index=False 
            )
            
            logging.info("Database transaction completed successfully.")
            
        except Exception as e:
            logging.error(f"FATAL: Database operation failed. Details: {str(e)}")
            raise