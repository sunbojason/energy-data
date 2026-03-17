import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, AzureError

from shared_logic.constants import DEFAULT_FREQ_GRID


# Configure the standard logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

logger.info("Service started successfully.")

def generate_mock_energy_data() -> pd.DataFrame:
    """Generates synthetic 15-minute energy time-series data matching the real pipeline schema."""
    logger.info("Initializing mock energy data generation (15-min grid)...")
    
    # 96 periods for a standard 24-hour day in 15-min intervals
    start_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    time_index = pd.date_range(start=start_time, periods=96, freq=DEFAULT_FREQ_GRID)

    np.random.seed(42) 
    
    # Matching the exact output schema of EntsoeDataClient
    data = {
        "DA_Price_0": np.random.normal(50, 20, 96),
        "Load_Actual_0": np.random.normal(15000, 2000, 96),
        "Imb_imbalance_short": np.random.uniform(0, 500, 96),
        "Imb_imbalance_long": np.random.uniform(0, 500, 96),
        "Export_BE_0": np.random.uniform(100, 500, 96),
        "Import_BE_0": np.random.uniform(50, 300, 96),
        "Export_DE_LU_0": np.random.uniform(500, 1500, 96),
        "Import_DE_LU_0": np.random.uniform(200, 1000, 96),
        "Export_GB_0": np.random.uniform(200, 800, 96),
        "Import_GB_0": np.random.uniform(100, 500, 96),
        "Export_NO_0": np.random.uniform(0, 400, 96),
        "Import_NO_0": np.random.uniform(0, 400, 96),
        "Export_DK_0": np.random.uniform(0, 300, 96),
        "Import_DK_0": np.random.uniform(0, 300, 96),
    }
    
    df = pd.DataFrame(data, index=time_index)
    df.index.name = 'Timestamp'
    
    # Dynamically calculate the sum columns
    export_cols = [c for c in df.columns if c.startswith('Export_') and c != 'Export_Sum']
    import_cols = [c for c in df.columns if c.startswith('Import_') and c != 'Import_Sum']
    
    df['Export_Sum'] = df[export_cols].sum(axis=1)
    df['Import_Sum'] = df[import_cols].sum(axis=1)
    
    logger.debug("Successfully generated 96-period mock time-series data.")
    return df

def upload_to_blob_storage(df: pd.DataFrame, container_name: str, blob_name: str) -> None:
    """Uploads a Pandas DataFrame as a CSV to Azure Blob Storage, auto-provisioning the container if needed."""
    logger.info("Attempting to read local.settings.json for connection string...")
    
    try:
        with open('local.settings.json', 'r') as f:
            settings = json.load(f)
        connection_string = settings['Values']['AzureWebJobsStorage']
    except FileNotFoundError as e:
        logger.error("local.settings.json not found. Integration test requires proper Azure credentials.")
        raise e
    except KeyError as e:
        logger.error("AzureWebJobsStorage key missing in local.settings.json. Integration test requires proper Azure credentials.")
        raise e

    # Explicitly encode the CSV string to bytes for safer Azure Blob network transport
    csv_data = df.to_csv(index=True).encode('utf-8')
    
    try:
        logger.info("Connecting to Azure Blob Storage service...")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # --- FIX: Auto-provisioning the container to prevent ContainerNotFound errors ---
        container_client = blob_service_client.get_container_client(container_name)
        try:
            container_client.create_container()
            logger.info(f"Container '{container_name}' did not exist and was successfully provisioned.")
        except ResourceExistsError:
            logger.info(f"Container '{container_name}' already exists. Proceeding with pipeline.")

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        logger.info(f"Starting upload for blob: {blob_name}")
        blob_client.upload_blob(csv_data, overwrite=True)
        logger.info(f"Upload successful! Data securely written to container '{container_name}'.")
        
    except AzureError as e:
        logger.error(f"Azure Storage operation failed: {str(e)}")
        raise e
    except Exception as e:
        logger.critical(f"An unexpected error occurred during upload: {str(e)}")
        raise e

def test_local_upload_pipeline():
    """
    Integration test to generate mock data and upload it to Azure Blob Storage locally.
    Pytest will automatically discover and run this function.
    """
    CONTAINER = "raw-data"
    BLOB_FILENAME = f"mock_entsoe_data_{datetime.now().strftime('%Y%m%d%H%M')}.csv"
    
    logger.info("--- Starting Local Upload Integration Test ---")
    
    mock_df = generate_mock_energy_data()
    
    # Assertions to ensure our mock generator maintains strict schema discipline
    assert not mock_df.empty, "Dataframe should not be empty"
    assert len(mock_df) == 96, f"Expected 96 15-min periods, got {len(mock_df)}"
    assert 'DA_Price_0' in mock_df.columns, "Expected schema column DA_Price_0 is missing"
    
    # Perform the upload. The test will naturally fail if any exception is raised during the upload.
    upload_to_blob_storage(mock_df, CONTAINER, BLOB_FILENAME)
    
    logger.info("--- Local Upload Integration Test Completed Successfully ---")

if __name__ == "__main__":
    # Provides a direct execution path outside of pytest for cleaner log visualization
    test_local_upload_pipeline()