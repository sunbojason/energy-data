import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, AzureError

# Configure the standard logger
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Example usage:
logger.info("Service started successfully.")
logger = logging.getLogger(__name__)

def generate_mock_energy_data() -> pd.DataFrame:
    """Generates synthetic hourly energy time-series data."""
    logger.info("Initializing mock energy data generation...")
    
    time_index = pd.date_range(start=datetime.now(), periods=24, freq='h')

    np.random.seed(42) 
    data = {
        "Forecasted Load": np.random.normal(50000, 5000, 24),
        "Actual Load": np.random.normal(50500, 5500, 24),
        "Imbalance Volume": np.random.normal(0, 500, 24),
        "import FR": np.random.uniform(1000, 3000, 24),
        "import GB": np.random.uniform(500, 2000, 24),
        "import DE_LU": np.random.uniform(2000, 5000, 24),
    }
    
    df = pd.DataFrame(data, index=time_index)
    df.index.name = 'Timestamp'
    df['import sum'] = df['import FR'] + df['import GB'] + df['import DE_LU']
    
    logger.debug("Successfully generated 24-hour mock time-series data.")
    return df

def upload_to_blob_storage(df: pd.DataFrame, container_name: str, blob_name: str) -> None:
    """Uploads a Pandas DataFrame as a CSV to Azure Blob Storage."""
    logger.info("Attempting to read local.settings.json for connection string...")
    
    try:
        with open('local.settings.json', 'r') as f:
            settings = json.load(f)
        connection_string = settings['Values']['AzureWebJobsStorage']
    except FileNotFoundError:
        logger.error("local.settings.json not found. Please ensure it exists in the root directory.")
        return
    except KeyError:
        logger.error("AzureWebJobsStorage key missing in local.settings.json.")
        return

    csv_data = df.to_csv(index=True)
    
    try:
        logger.info("Connecting to Azure Blob Storage service...")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        logger.info(f"Starting upload for blob: {blob_name}")
        blob_client.upload_blob(csv_data, overwrite=True)
        logger.info(f"Upload successful! Data written to container '{container_name}'.")
        
    except AzureError as e:
        logger.error(f"Azure Storage operation failed: {str(e)}")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during upload: {str(e)}")

if __name__ == "__main__":
    CONTAINER = "raw-data"
    BLOB_FILENAME = f"mock_entsoe_data_{datetime.now().strftime('%Y%m%d%H%M')}.csv"
    
    logger.info("--- Starting Local Upload Test Pipeline ---")
    
    mock_df = generate_mock_energy_data()
    upload_to_blob_storage(mock_df, CONTAINER, BLOB_FILENAME)
    
    logger.info("--- Local Upload Test Pipeline Completed ---")