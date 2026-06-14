from celery import Celery
import logging
import zipfile
import json
import csv
import io
import xml.etree.ElementTree as ET
import asyncio
import asyncpg
import uuid
from datetime import datetime
from neo4j import AsyncGraphDatabase
from qdrant_client import QdrantClient
from config import settings
from celery.signals import worker_process_init
from telemetry import configure_telemetry
from opentelemetry import trace
import ijson

tracer = trace.get_tracer(__name__)
logger = logging.getLogger(__name__)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

celery_app = Celery(
    "ufdr_worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL
)

# Ensure telemetry is configured when the worker process boots
@worker_process_init.connect(weak=False)
def init_celery_tracing(*args, **kwargs):
    configure_telemetry(is_worker=True)


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def parse_iso_date(date_str):
    if not date_str:
        return None
    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))

async def bulk_insert_postgres(job_id: str, parsed_data: dict):
    # Acquire a connection from the pool
    conn = await asyncpg.connect(
        user=settings.POSTGRES_USER, password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB, host=settings.POSTGRES_SERVER, port=settings.POSTGRES_PORT
    )
    
    try:
        async with conn.transaction():
            
            if parsed_data.get("messages"):
                # 1. Create a temporary staging table that drops itself when the transaction ends
                await conn.execute("CREATE TEMP TABLE tmp_messages (LIKE messages INCLUDING ALL) ON COMMIT DROP;")
                
                # 2. Format the data for COPY
                message_tuples = [
                    (job_id, m.get("message_id"), m.get("thread_id"), m.get("platform"), 
                     m.get("sender_phone"), m.get("recipient_phones", []), m.get("content_text"), 
                     parse_iso_date(m.get("sent_at")), m.get("is_deleted", False)) 
                    for m in parsed_data["messages"]
                ]
                
                # 3. Stream binary data directly to the staging table (Blazing Fast)
                await conn.copy_records_to_table(
                    'tmp_messages',
                    columns=['job_id', 'message_id', 'thread_id', 'platform', 'sender_phone', 'recipient_phones', 'content_text', 'sent_at', 'is_deleted'],
                    records=message_tuples
                )
                
                # 4. Bulk INSERT from staging to production, handling conflicts
                await conn.execute("""
                    INSERT INTO messages 
                    SELECT * FROM tmp_messages 
                    ON CONFLICT (job_id, message_id) DO NOTHING;
                """)

    finally:
        await conn.close()

async def build_graph_neo4j(job_id: str, case_id: str, parsed_data: dict):
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI, 
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    )
    try:
        async with driver.session() as session:
            # 1. Anchor the Case Node (The Root Tenant Boundary)
            await session.run("MERGE (c:Case {case_id: $case_id})", case_id=case_id)

            # 2. Ingest Contacts
            if parsed_data.get("contacts"):
                cypher_contacts = """
                MATCH (case:Case {case_id: $case_id})
                UNWIND $contacts AS c 
                MERGE (ct:Contact {contact_id: c.contact_id, job_id: $job_id}) 
                SET ct.display_name = c.display_name, ct.organization = c.organization 
                MERGE (case)-[:OWNS]->(ct)
                WITH ct, c, case 
                UNWIND c.phone_numbers AS phone 
                MERGE (pn:PhoneNumber {e164: phone}) 
                MERGE (case)-[:OWNS]->(pn)
                MERGE (pn)-[:ASSIGNED_TO]->(ct)
                """
                await session.run(cypher_contacts, contacts=parsed_data["contacts"], job_id=job_id, case_id=case_id)
                
            # 3. Ingest Calls
            if parsed_data.get("calls"):
                cypher_calls = """
                MATCH (case:Case {case_id: $case_id})
                UNWIND $calls AS call
                MERGE (cl:Call {call_id: call.call_id, job_id: $job_id})
                SET cl.duration_seconds = toInteger(call.duration_seconds),
                    cl.started_at = datetime(call.started_at),
                    cl.direction = call.direction,
                    cl.call_type = call.call_type
                MERGE (case)-[:OWNS]->(cl)
                WITH cl, call, case
                MERGE (caller:PhoneNumber {e164: call.caller_phone})
                MERGE (callee:PhoneNumber {e164: call.callee_phone})
                MERGE (case)-[:OWNS]->(caller)
                MERGE (case)-[:OWNS]->(callee)
                MERGE (caller)-[:MADE_CALL]->(cl)
                MERGE (cl)-[:RECEIVED_BY]->(callee)
                """
                await session.run(cypher_calls, calls=parsed_data["calls"], job_id=job_id, case_id=case_id)

            # 4. Ingest Messages
            if parsed_data.get("messages"):
                cypher_messages = """
                MATCH (case:Case {case_id: $case_id})
                UNWIND $messages AS msg
                MERGE (m:Message {message_id: msg.message_id, job_id: $job_id})
                SET m.content_text = msg.content_text,
                    m.sent_at = datetime(msg.sent_at),
                    m.platform = msg.platform,
                    m.is_deleted = msg.is_deleted
                MERGE (case)-[:OWNS]->(m)
                WITH m, msg, case
                MERGE (sender:PhoneNumber {e164: msg.sender_phone})
                MERGE (case)-[:OWNS]->(sender)
                MERGE (sender)-[:SENT_MESSAGE]->(m)
                WITH m, msg, case
                UNWIND msg.recipient_phones AS recipient
                MERGE (rec_pn:PhoneNumber {e164: recipient})
                MERGE (case)-[:OWNS]->(rec_pn)
                MERGE (m)-[:RECEIVED_BY]->(rec_pn)
                """
                await session.run(cypher_messages, messages=parsed_data["messages"], job_id=job_id, case_id=case_id)

    except Exception as e:
        logger.error(f"[JOB: {job_id}] Neo4j Graph Build Failed: {str(e)}")
        raise e
    finally:
        await driver.close()

def build_vector_index(job_id: str, parsed_data: dict):
    """
    Phase 4: Semantic Chunking and Vector Upsert using FastEmbed and Qdrant.
    Uses a sliding window to group conversational context.
    """
    messages = parsed_data.get("messages", [])
    if not messages:
        return

    logger.info(f"[JOB: {job_id}] Initializing Qdrant and FastEmbed model...")
    # Using the synchronous Qdrant client to natively handle fastembed
    client = QdrantClient(host=settings.QDRANT_HOST, port=settings.QDRANT_PORT)
    
    # FastEmbed runs locally. It will download the ~130MB model on the very first run.
    client.set_model("BAAI/bge-small-en-v1.5")

    collection_name = "forensic_chunks"
    if not client.collection_exists(collection_name):
        client.create_collection(
            collection_name=collection_name,
            vectors_config=client.get_fastembed_vector_params()
        )

    # 1. Group messages by thread
    threads = {}
    for msg in messages:
        tid = msg.get("thread_id", "unknown")
        if tid not in threads:
            threads[tid] = []
        threads[tid].append(msg)

    documents = []
    metadata = []
    ids = []

    # 2. Sliding Window Parameters
    window_size = 5
    step = 3 # This creates an overlap of 2 messages

    for tid, msgs in threads.items():
        # Ensure chronological order
        msgs.sort(key=lambda x: x.get("sent_at", ""))

        for i in range(0, len(msgs), step):
            window = msgs[i:i+window_size]
            if not window: continue

            chunk_lines = []
            sender_phones = set()
            message_ids = []

            # Format the chunk to look like a chat transcript
            for msg in window:
                sender = msg.get("sender_phone", "UNKNOWN")
                time_str = msg.get("sent_at", "")
                content = msg.get("content_text", "")
                chunk_lines.append(f"[{time_str}] {sender}: {content}")
                sender_phones.add(sender)
                message_ids.append(msg.get("message_id"))

            chunk_text = "\n".join(chunk_lines)
            
            documents.append(chunk_text)
            metadata.append({
                "job_id": job_id,
                "thread_id": tid,
                "start_time": window[0].get("sent_at"),
                "end_time": window[-1].get("sent_at"),
                "sender_phones": list(sender_phones),
                "message_ids": message_ids
            })
            ids.append(str(uuid.uuid4()))

    if documents:
        logger.info(f"[JOB: {job_id}] Upserting {len(documents)} semantic chunks to Qdrant...")
        # The add() method automatically runs the text through FastEmbed and upserts the vectors!
        client.add(
            collection_name=collection_name,
            documents=documents,
            metadata=metadata,
            ids=ids
        )
        logger.info(f"[JOB: {job_id}] Successfully embedded and indexed {len(documents)} chunks.")


@celery_app.task(bind=True, name="parse_ufdr_archive", max_retries=3)
def parse_ufdr_archive(self, job_id: str, file_path: str, case_id: str):
    logger.info(f"[JOB: {job_id}] Worker picked up task. Target file: {file_path}")
    
    try:
        # PHASE 1: DECOMPRESS & PARSE
        logger.info(f"[JOB: {job_id}] Phase 1: Unzipping and extracting entities...")
        parsed_data = {"manifest": {}, "contacts": [], "calls": [], "messages": []}
        
        with tracer.start_as_current_span("unzip_and_parse_ufdr") as span:
            span.set_attribute("job_id", job_id)
            span.set_attribute("file_path", file_path)
            
            with zipfile.ZipFile(file_path, 'r') as zf:
                file_list = zf.namelist()
                
                # --- MANIFEST (XML) ---
                if 'manifest.xml' in file_list:
                    with zf.open('manifest.xml') as f:
                        device_node = ET.parse(f).getroot().find('.//Device')
                        if device_node is not None:
                            parsed_data["manifest"]["imei"] = device_node.findtext('IMEI', 'Unknown')
                            parsed_data["manifest"]["model"] = device_node.findtext('Model', 'Unknown')
                            parsed_data["manifest"]["os"] = device_node.findtext('OSVersion', 'Unknown')
                            
                # --- CONTACTS (STREAMING JSON) ---
                if 'contacts.json' in file_list:
                    with zf.open('contacts.json') as f: 
                        import ijson # Make sure 'ijson' is in requirements.txt
                        parser = ijson.items(f, 'item')
                        for contact in parser:
                            parsed_data["contacts"].append(contact)
                        
                # --- MESSAGES (STREAMING JSON) ---
                if 'messages.json' in file_list:
                    with zf.open('messages.json') as f: 
                        parser = ijson.items(f, 'item')
                        for message in parser:
                            parsed_data["messages"].append(message)
                        
                # --- CALLS (STREAMING CSV) ---
                if 'calls.csv' in file_list:
                    with zf.open('calls.csv') as f: 
                        import io, csv
                        text_stream = io.TextIOWrapper(f, encoding='utf-8')
                        reader = csv.DictReader(text_stream)
                        for call in reader:
                            parsed_data["calls"].append(call)

        # PHASE 2: POSTGRESQL BULK INSERT
        logger.info(f"[JOB: {job_id}] Phase 2: Inserting into PostgreSQL...")
        asyncio.run(bulk_insert_postgres(job_id, parsed_data))
        
        # PHASE 3: NEO4J GRAPH CONSTRUCTION
        logger.info(f"[JOB: {job_id}] Phase 3: Building Neo4j Graph Topology...")
        asyncio.run(build_graph_neo4j(job_id, case_id, parsed_data))

        # PHASE 4: QDRANT SEMANTIC CHUNKING
        logger.info(f"[JOB: {job_id}] Phase 4: Building Vector Index...")
        build_vector_index(job_id, parsed_data)

        logger.info(f"[JOB: {job_id}] ALL PHASES COMPLETE. Extraction successfully ingested.")
        return {"status": "success", "job_id": job_id, "pipeline": "100% complete"}

    except Exception as e:
        logger.error(f"[JOB: {job_id}] Failed: {str(e)}")
        raise self.retry(exc=e, countdown=2 ** self.request.retries)