import os
import logging
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Initialize Identity once
credential = DefaultAzureCredential()

storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
account_url = f"https://{storage_account_name}.blob.core.windows.net" if storage_account_name else None

# Initialize the Blob Service Client once for reuse across all blueprints
if account_url:
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    logging.info("BlobServiceClient initialized successfully.")
else:
    blob_service_client = None
    logging.error("CRITICAL: STORAGE_ACCOUNT_NAME environment variable is missing.")