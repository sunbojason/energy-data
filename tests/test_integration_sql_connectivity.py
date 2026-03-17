import os
import json
import struct
import pyodbc
import logging
import pandas as pd
from azure.identity import DefaultAzureCredential

# Set up logging to match your style
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')

def load_local_settings():
    """Reads configuration from local.settings.json to maintain environment consistency."""
    settings_path = os.path.join(os.getcwd(), 'local.settings.json')
    
    if not os.path.exists(settings_path):
        logging.error(f"CRITICAL: {settings_path} not found. Ensure you are in the project root.")
        return {}

    with open(settings_path, 'r') as f:
        data = json.load(f)
        return data.get('Values', {})

def test_sql_integration():
    """
    Integration test to verify local-to-cloud SQL connectivity.
    Validates Entra ID Token injection and Firewall/SSL rules.
    """
    logging.info("INTEGRATION TEST: Verifying SQL Connectivity...")
    
    # 1. Load Environment
    settings = load_local_settings()
    server = settings.get("SQL_SERVER_NAME")
    database = settings.get("SQL_DATABASE_NAME")
    # Defaulting to Driver 18 for macOS ARM64 compatibility
    driver = "{ODBC Driver 18 for SQL Server}"

    if not server or not database:
        logging.error("ENVIRONMENT ERROR: SQL configuration missing in local.settings.json.")
        return

    try:
        # 2. Identity & Auth (Entra ID)
        credential = DefaultAzureCredential()
        logging.info(f"Attempting to acquire token for {server}...")
        token_obj = credential.get_token("https://database.windows.net/.default")
        
        # SQL Server expects token in UTF-16-LE
        token_bytes = token_obj.token.encode("utf-16-le")
        encoded_token = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        logging.info("Entra ID Token acquired.")

        # 3. Connection Discipline (The "Magic" Parameters)
        # TrustServerCertificate is mandatory for local dev on macOS to avoid SSL 404/Timeout
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )

        # 4. Handshake Execution
        SQL_COPT_SS_ACCESS_TOKEN = 1256 
        logging.info(f"Opening connection to {database}...")
        
        with pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: encoded_token}) as conn:
            logging.info("SUCCESS: Handshake confirmed. Firewall is OPEN.")
            
            # Metadata Check
            cursor = conn.cursor()
            cursor.execute("SELECT TOP 3 timestamp, DA_Price_0 FROM entsoe ORDER BY timestamp DESC")
            rows = cursor.fetchall()
            
            if rows:
                logging.info(f"Data Preview: Found {len(rows)} records. Latest: {rows[0][0]}")
            else:
                logging.warning("Connection works, but table 'entsoe' has no data.")

    except pyodbc.OperationalError as e:
        if "HYT00" in str(e):
            logging.error("TIMEOUT: Firewall is likely blocking your current IP.")
        else:
            logging.error(f"DATABASE ERROR: {str(e)}")
    except Exception as e:
        logging.error(f"UNEXPECTED FAILURE: {str(e)}")

if __name__ == "__main__":
    test_sql_integration()