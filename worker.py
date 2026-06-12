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
    conn = await asyncpg.connect(
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB,
        host=settings.POSTGRES_SERVER,
        port=settings.POSTGRES_PORT
    )
    try:
        if parsed_data["contacts"]:
            contact_tuples = [(job_id, c.get("contact_id"), c.get("display_name"), c.get("phone_numbers", []), c.get("organization"), c.get("is_foreign", False)) for c in parsed_data["contacts"]]
            await conn.executemany("INSERT INTO contacts (job_id, contact_id, display_name, phone_numbers, organization, is_foreign) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (job_id, contact_id) DO NOTHING", contact_tuples)

        if parsed_data["calls"]:
            call_tuples = [(job_id, c.get("call_id"), c.get("caller_phone"), c.get("callee_phone"), c.get("direction"), c.get("call_type"), int(c.get("duration_seconds", 0)), parse_iso_date(c.get("started_at"))) for c in parsed_data["calls"]]
            await conn.executemany("INSERT INTO calls (job_id, call_id, caller_phone, callee_phone, direction, call_type, duration_seconds, started_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (job_id, call_id) DO NOTHING", call_tuples)

        if parsed_data["messages"]:
            message_tuples = [(job_id, m.get("message_id"), m.get("thread_id"), m.get("platform"), m.get("sender_phone"), m.get("recipient_phones", []), m.get("content_text"), parse_iso_date(m.get("sent_at")), m.get("is_deleted", False)) for m in parsed_data["messages"]]
            await conn.executemany("INSERT INTO messages (job_id, message_id, thread_id, platform, sender_phone, recipient_phones, content_text, sent_at, is_deleted) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (job_id, message_id) DO NOTHING", message_tuples)
    finally:
        await conn.close()

async def build_graph_neo4j(job_id: str, parsed_data: dict):
    driver = AsyncGraphDatabase.driver(settings.NEO4J_URI, auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD))
    try:
        async with driver.session() as session:
            if parsed_data["contacts"]:
                await session.run("UNWIND $contacts AS c MERGE (ct:Contact {contact_id: c.contact_id}) SET ct.display_name = c.display_name, ct.organization = c.organization, ct.job_id = $job_id WITH ct, c UNWIND c.phone_numbers AS phone MERGE (pn:PhoneNumber {e164: phone}) MERGE (pn)-[:ASSIGNED_TO]->(ct)", contacts=parsed_data["contacts"], job_id=job_id)

            if parsed_data["messages"]:
                await session.run("UNWIND $messages AS m MERGE (msg:Message {message_id: m.message_id}) SET msg.platform = m.platform, msg.sent_at = datetime(m.sent_at), msg.job_id = $job_id WITH msg, m MERGE (sender:PhoneNumber {e164: m.sender_phone}) MERGE (msg)-[:SENT_BY]->(sender) WITH msg, m, sender UNWIND m.recipient_phones AS recip_phone MERGE (recip:PhoneNumber {e164: recip_phone}) MERGE (msg)-[:RECEIVED_BY]->(recip) MERGE (sender)-[rel:CONTACTED]->(recip) ON CREATE SET rel.count = 1, rel.last_contact = datetime(m.sent_at) ON MATCH SET rel.count = rel.count + 1, rel.last_contact = CASE WHEN datetime(m.sent_at) > coalesce(rel.last_contact, datetime('1970-01-01T00:00:00Z')) THEN datetime(m.sent_at) ELSE rel.last_contact END", messages=parsed_data["messages"], job_id=job_id)

            if parsed_data["calls"]:
                await session.run("UNWIND $calls AS c MERGE (caller:PhoneNumber {e164: c.caller_phone}) MERGE (callee:PhoneNumber {e164: c.callee_phone}) MERGE (caller)-[rel:CALLED]->(callee) ON CREATE SET rel.count = 1, rel.total_duration = toInteger(c.duration_seconds) ON MATCH SET rel.count = rel.count + 1, rel.total_duration = rel.total_duration + toInteger(c.duration_seconds)", calls=parsed_data["calls"], job_id=job_id)
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
def parse_ufdr_archive(self, job_id: str, file_path: str):
    logger.info(f"[JOB: {job_id}] Worker picked up task. Target file: {file_path}")
    
    try:
        # PHASE 1: DECOMPRESS & PARSE
        logger.info(f"[JOB: {job_id}] Phase 1: Unzipping and extracting entities...")
        parsed_data = {"manifest": {}, "contacts": [], "calls": [], "messages": []}
        
        with zipfile.ZipFile(file_path, 'r') as zf:
            file_list = zf.namelist()
            if 'manifest.xml' in file_list:
                with zf.open('manifest.xml') as f:
                    device_node = ET.parse(f).getroot().find('.//Device')
                    if device_node is not None:
                        parsed_data["manifest"]["imei"] = device_node.findtext('IMEI', 'Unknown')
                        parsed_data["manifest"]["model"] = device_node.findtext('Model', 'Unknown')
                        parsed_data["manifest"]["os"] = device_node.findtext('OSVersion', 'Unknown')
            if 'contacts.json' in file_list:
                with zf.open('contacts.json') as f: parsed_data["contacts"] = json.load(f)
            if 'messages.json' in file_list:
                with zf.open('messages.json') as f: parsed_data["messages"] = json.load(f)
            if 'calls.csv' in file_list:
                with zf.open('calls.csv') as f: parsed_data["calls"] = list(csv.DictReader(io.StringIO(f.read().decode('utf-8'))))

        # PHASE 2: POSTGRESQL BULK INSERT
        logger.info(f"[JOB: {job_id}] Phase 2: Inserting into PostgreSQL...")
        asyncio.run(bulk_insert_postgres(job_id, parsed_data))
        
        # PHASE 3: NEO4J GRAPH CONSTRUCTION
        logger.info(f"[JOB: {job_id}] Phase 3: Building Neo4j Graph Topology...")
        asyncio.run(build_graph_neo4j(job_id, parsed_data))

        # PHASE 4: QDRANT SEMANTIC CHUNKING
        logger.info(f"[JOB: {job_id}] Phase 4: Building Vector Index...")
        build_vector_index(job_id, parsed_data)

        logger.info(f"[JOB: {job_id}] ALL PHASES COMPLETE. Extraction successfully ingested.")
        return {"status": "success", "job_id": job_id, "pipeline": "100% complete"}

    except Exception as e:
        logger.error(f"[JOB: {job_id}] Failed: {str(e)}")
        raise self.retry(exc=e, countdown=2 ** self.request.retries)