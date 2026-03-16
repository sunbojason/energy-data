import azure.functions as func
import logging
from shared_logic.cleaning_service import CleaningService

cleaning_bp = func.Blueprint()

@cleaning_bp.blob_trigger(arg_name="myblob", path="raw-data/{name}", connection="AzureWebJobsStorage")
@cleaning_bp.blob_output(arg_name="outputblob", path="cleaned-data/{name}", connection="AzureWebJobsStorage")
def blob_trigger_cleaning_processor(myblob: func.InputStream, outputblob: func.Out[str]):
    """
    Event-driven processor: Triggered automatically when a new CSV lands in 'raw-data'.
    Cleans the payload and promotes it to the 'cleaned-data' container.
    """
    file_name = myblob.name
    logging.info(f"Processing new raw file: {file_name} ({myblob.length} bytes)")

    try:
        raw_content = myblob.read().decode('utf-8')
        if not raw_content: 
            logging.warning(f"File {file_name} is empty. Terminating process.")
            return

        # Transform and Clean via Shared Service
        cleaned_csv = CleaningService.clean_energy_data(raw_content)
        
        if cleaned_csv:
            # func.Out binding handles the actual upload to Blob Storage automatically
            outputblob.set(cleaned_csv)
            logging.info(f"CLEANING SUCCESS: {file_name} successfully promoted to cleaned-data layer.")
        else:
            logging.warning(f"CLEANING SKIPPED: CleaningService returned empty string for {file_name}.")
            
    except Exception as e:
        logging.error(f"CLEANING FAILURE processing {file_name}: {str(e)}")
        # Do not raise here to prevent poison queue loops, unless strict DLQ (Dead Letter Queue) is configured