from celery import Celery
import logging
import zipfile
import json
import csv
import io
import xml.etree.ElementTree as ET
from config import settings

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Celery
celery_app = Celery(
    "ufdr_worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1
)

@celery_app.task(bind=True, name="parse_ufdr_archive", max_retries=3)
def parse_ufdr_archive(self, job_id: str, file_path: str):
    """
    Background task to process the uploaded UFDR file.
    Phase 1: In-Memory Decompression & Parsing
    """
    logger.info(f" [JOB: {job_id}] Worker picked up task. Target file: {file_path}")
    
    try:
        # ---------------------------------------------------------
        # PHASE 1: DECOMPRESS & PARSE (In-Memory Streaming)
        # ---------------------------------------------------------
        logger.info(f" [JOB: {job_id}] Phase 1: Unzipping and extracting entities...")
        
        # This dictionary will hold all our data to pass to the databases
        parsed_data = {
            "manifest": {},
            "contacts": [],
            "calls": [],
            "messages": []
        }
        
        # Open the ZIP archive in read mode
        with zipfile.ZipFile(file_path, 'r') as zf:
            file_list = zf.namelist()
            
            # 1. Parse Manifest (XML)
            if 'manifest.xml' in file_list:
                with zf.open('manifest.xml') as f:
                    tree = ET.parse(f)
                    root = tree.getroot()
                    device_node = root.find('.//Device')
                    if device_node is not None:
                        parsed_data["manifest"]["imei"] = device_node.findtext('IMEI', 'Unknown')
                        parsed_data["manifest"]["model"] = device_node.findtext('Model', 'Unknown')
                        parsed_data["manifest"]["os"] = device_node.findtext('OSVersion', 'Unknown')
            
            # 2. Parse Contacts (JSON)
            if 'contacts.json' in file_list:
                with zf.open('contacts.json') as f:
                    parsed_data["contacts"] = json.load(f)
                    
            # 3. Parse Messages (JSON)
            if 'messages.json' in file_list:
                with zf.open('messages.json') as f:
                    parsed_data["messages"] = json.load(f)
                    
            # 4. Parse Calls (CSV)
            if 'calls.csv' in file_list:
                with zf.open('calls.csv') as f:
                    # CSV requires decoding bytes to string
                    csv_data = f.read().decode('utf-8')
                    reader = csv.DictReader(io.StringIO(csv_data))
                    parsed_data["calls"] = list(reader)

        logger.info(f" [JOB: {job_id}] Phase 1 Complete. "
                    f"Parsed {len(parsed_data['contacts'])} contacts, "
                    f"{len(parsed_data['calls'])} calls, and "
                    f"{len(parsed_data['messages'])} messages.")

        # ---------------------------------------------------------
        # PHASE 2: POSTGRESQL BULK INSERT (Coming Next)
        # ---------------------------------------------------------
        logger.info(f" [JOB: {job_id}] Phase 2: Ready for database insertion...")
        # (We will add the asyncpg database logic here in the next step)
        
        return {
            "status": "success", 
            "job_id": job_id, 
            "data_summary": {
                "device_model": parsed_data["manifest"].get("model"),
                "contacts_found": len(parsed_data["contacts"]),
                "calls_found": len(parsed_data["calls"]),
                "messages_found": len(parsed_data["messages"])
            }
        }

    except zipfile.BadZipFile:
        logger.error(f" [JOB: {job_id}] Failed: File is not a valid zip/ufdr archive.")
        raise
    except Exception as e:
        logger.error(f" [JOB: {job_id}] Failed during parsing: {str(e)}")
        # Exponential backoff retry for transient errors
        raise self.retry(exc=e, countdown=2 ** self.request.retries)