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

# Ensure telemetry is configured when the worker process boots
@worker_process_init.connect(weak=False)
def init_celery_tracing(*args, **kwargs):
    configure_telemetry(is_worker=True)

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

async def bulk_insert_postgres(job_id: str, case_id: str, parsed_data: dict):
    """Phase 2: Idempotent SQL UPSERTS"""
    conn = await asyncpg.connect(
        user=settings.POSTGRES_USER, password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB, host=settings.POSTGRES_SERVER, port=settings.POSTGRES_PORT
    )
    
    try:
        async with conn.transaction():
            # 1. UPSERT Contacts
            if parsed_data.get("contacts"):
                contacts_data = [
                    (job_id, case_id, c.get("contact_id"), c.get("display_name"), c.get("phone_numbers", []), c.get("organization")) 
                    for c in parsed_data["contacts"]
                ]
                await conn.executemany("""
                    INSERT INTO contacts (job_id, case_id, contact_id, display_name, phone_numbers, organization)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (case_id, contact_id) 
                    DO UPDATE SET display_name = EXCLUDED.display_name, job_id = EXCLUDED.job_id;
                """, contacts_data)

            # 2. UPSERT Calls
            if parsed_data.get("calls"):
                calls_data = [
                    (job_id, case_id, c.get("call_id"), c.get("caller_phone"), c.get("callee_phone"), 
                     c.get("direction"), parse_iso_date(c.get("started_at"))) 
                    for c in parsed_data["calls"]
                ]
                await conn.executemany("""
                    INSERT INTO calls (job_id, case_id, call_id, caller_phone, callee_phone, direction, started_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (case_id, call_id) 
                    DO UPDATE SET started_at = EXCLUDED.started_at, job_id = EXCLUDED.job_id;
                """, calls_data)

            # 3. UPSERT Messages
            if parsed_data.get("messages"):
                msg_data = [
                    (job_id, case_id, m.get("message_id"), m.get("thread_id"), m.get("platform"), 
                     m.get("sender_phone"), m.get("recipient_phones", []), m.get("content_text"), 
                     parse_iso_date(m.get("sent_at"))) 
                    for m in parsed_data["messages"]
                ]
                await conn.executemany("""
                    INSERT INTO messages (job_id, case_id, message_id, thread_id, platform, sender_phone, recipient_phones, content_text, sent_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (case_id, message_id) 
                    DO UPDATE SET content_text = EXCLUDED.content_text, job_id = EXCLUDED.job_id;
                """, msg_data)
    finally:
        await conn.close()

async def build_graph_neo4j(job_id: str, case_id: str, parsed_data: dict):
    """Phase 3: Idempotent Graph MERGES"""
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI, 
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    )
    try:
        async with driver.session() as session:
            await session.run("MERGE (c:Case {case_id: $case_id})", case_id=case_id)

            if parsed_data.get("contacts"):
                cypher_contacts = """
                MATCH (case:Case {case_id: $case_id})
                UNWIND $contacts AS c 
                MERGE (ct:Contact {contact_id: c.contact_id, case_id: $case_id}) 
                SET ct.display_name = c.display_name, ct.organization = c.organization, ct.job_id = $job_id 
                MERGE (case)-[:OWNS]->(ct)
                WITH ct, c, case 
                UNWIND c.phone_numbers AS phone 
                MERGE (pn:PhoneNumber {e164: phone}) 
                MERGE (case)-[:OWNS]->(pn)
                MERGE (pn)-[:ASSIGNED_TO]->(ct)
                """
                await session.run(cypher_contacts, contacts=parsed_data["contacts"], job_id=job_id, case_id=case_id)
                
            if parsed_data.get("calls"):
                cypher_calls = """
                MATCH (case:Case {case_id: $case_id})
                UNWIND $calls AS call
                MERGE (cl:Call {call_id: call.call_id, case_id: $case_id})
                SET cl.duration_seconds = toInteger(call.duration_seconds),
                    cl.started_at = datetime(call.started_at),
                    cl.direction = call.direction,
                    cl.call_type = call.call_type,
                    cl.job_id = $job_id
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

            if parsed_data.get("messages"):
                cypher_messages = """
                MATCH (case:Case {case_id: $case_id})
                UNWIND $messages AS msg
                MERGE (m:Message {message_id: msg.message_id, case_id: $case_id})
                SET m.content_text = msg.content_text,
                    m.sent_at = datetime(msg.sent_at),
                    m.platform = msg.platform,
                    m.is_deleted = msg.is_deleted,
                    m.job_id = $job_id
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

def build_vector_index(job_id: str, case_id: str, parsed_data: dict):
    """Phase 4: Semantic Chunking with Deterministic UUIDs"""
    messages = parsed_data.get("messages", [])
    if not messages:
        return

    logger.info(f"[JOB: {job_id}] Initializing Qdrant and FastEmbed model...")
    client = QdrantClient(host=settings.QDRANT_HOST, port=settings.QDRANT_PORT)
    client.set_model("BAAI/bge-small-en-v1.5")

    collection_name = "forensic_chunks"
    if not client.collection_exists(collection_name):
        client.create_collection(
            collection_name=collection_name,
            vectors_config=client.get_fastembed_vector_params()
        )

    threads = {}
    for msg in messages:
        tid = msg.get("thread_id", "unknown")
        if tid not in threads:
            threads[tid] = []
        threads[tid].append(msg)

    documents = []
    metadata = []
    ids = []

    window_size = 5
    step = 3

    for tid, msgs in threads.items():
        msgs.sort(key=lambda x: x.get("sent_at", ""))

        for i in range(0, len(msgs), step):
            window = msgs[i:i+window_size]
            if not window: continue

            chunk_lines = []
            sender_phones = set()
            message_ids = []

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
                "case_id": case_id,
                "thread_id": tid,
                "start_time": window[0].get("sent_at"),
                "end_time": window[-1].get("sent_at"),
                "sender_phones": list(sender_phones),
                "message_ids": message_ids
            })
            
            # Deterministic UUID prevents vector duplication
            chunk_hash = f"{case_id}_{tid}_{chunk_text}"
            deterministic_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk_hash))
            ids.append(deterministic_id)

    if documents:
        logger.info(f"[JOB: {job_id}] Upserting {len(documents)} semantic chunks to Qdrant...")
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
        self.update_state(state='PARSING', meta={'progress': 'Unzipping and extracting entities...'})
        logger.info(f"[JOB: {job_id}] Phase 1: Unzipping and extracting entities...")
        parsed_data = {"manifest": {}, "contacts": [], "calls": [], "messages": []}
        
        with tracer.start_as_current_span("unzip_and_parse_ufdr") as span:
            span.set_attribute("job_id", job_id)
            span.set_attribute("file_path", file_path)
            
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
                    with zf.open('contacts.json') as f: 
                        parser = ijson.items(f, 'item')
                        for contact in parser: parsed_data["contacts"].append(contact)
                        
                if 'messages.json' in file_list:
                    with zf.open('messages.json') as f: 
                        parser = ijson.items(f, 'item')
                        for message in parser: parsed_data["messages"].append(message)
                        
                if 'calls.csv' in file_list:
                    with zf.open('calls.csv') as f: 
                        text_stream = io.TextIOWrapper(f, encoding='utf-8')
                        reader = csv.DictReader(text_stream)
                        for call in reader: parsed_data["calls"].append(call)

        # PHASE 2: POSTGRESQL BULK INSERT
        self.update_state(state='SQL_DONE', meta={'progress': 'Inserting into PostgreSQL...'})
        logger.info(f"[JOB: {job_id}] Phase 2: Inserting into PostgreSQL...")
        asyncio.run(bulk_insert_postgres(job_id, case_id, parsed_data))
        
        # PHASE 3: NEO4J GRAPH CONSTRUCTION
        self.update_state(state='GRAPH_DONE', meta={'progress': 'Building Neo4j Graph Topology...'})
        logger.info(f"[JOB: {job_id}] Phase 3: Building Neo4j Graph Topology...")
        asyncio.run(build_graph_neo4j(job_id, case_id, parsed_data))

        # PHASE 4: QDRANT SEMANTIC CHUNKING
        self.update_state(state='EMBEDDING', meta={'progress': 'Building Vector Index...'})
        logger.info(f"[JOB: {job_id}] Phase 4: Building Vector Index...")
        build_vector_index(job_id, case_id, parsed_data)

        logger.info(f"[JOB: {job_id}] ALL PHASES COMPLETE. Extraction successfully ingested.")
        # Celery automatically sets the state to 'SUCCESS' when it returns
        return {"status": "success", "job_id": job_id, "pipeline": "100% complete"}

    except Exception as e:
        logger.error(f"[JOB: {job_id}] Failed: {str(e)}")
        raise self.retry(exc=e, countdown=2 ** self.request.retries)