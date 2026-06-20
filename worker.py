import tempfile
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
import qdrant_client
from config import settings
from celery.signals import worker_process_init
from telemetry import configure_telemetry
from opentelemetry import trace
import ijson
import redis
import boto3
import os
from qdrant_client import models
from database import db
from celery.signals import worker_process_init, task_prerun, task_postrun
from opentelemetry.propagate import extract
from opentelemetry import context as otel_context

redis_publisher = redis.from_url(settings.REDIS_URL)
s3_worker_client = boto3.client(
    's3', 
    endpoint_url=settings.MINIO_ENDPOINT,
    aws_access_key_id=settings.MINIO_ACCESS_KEY,
    aws_secret_access_key=settings.MINIO_SECRET_KEY
)

def broadcast_update(job_id, status, phase=None, error=None):
    """Pushes real-time updates directly to the FastAPI SSE stream."""
    payload = {"job_id": job_id, "status": status, "phase": phase, "error_message": error}
    redis_publisher.publish(f"job_{job_id}", json.dumps(payload))

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

@task_prerun.connect
def setup_distributed_tracing(task_id, task, *args, **kwargs):
    try:
        # Grab the headers from the Celery message
        headers = task.request.headers or {}
        
        # W3C Context Extraction: Translates 'traceparent' string back into an active Context
        parent_context = extract(headers)
        
        # Attach the context to the current thread/worker
        token = otel_context.attach(parent_context)
        
        # Save the token to safely detach it later
        task.request.otel_token = token
        
    except Exception as e:
        # Graceful degradation: fail open
        logger.warning(f"[O11Y] Distributed trace propagation failed for task {task_id}: {e}. Falling back to unlinked root span.")

# Clean up the trace context when the task finishes (success or failure)
@task_postrun.connect
def teardown_distributed_tracing(task_id, task, *args, **kwargs):
    token = getattr(task.request, "otel_token", None)
    if token:
        otel_context.detach(token)

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


def broadcast_update(job_id, status, phase=None, error=None):
    """Pushes real-time updates directly to the Redis Pub/Sub for FastAPI to stream via SSE."""
    payload = {"job_id": job_id, "status": status, "phase": phase, "error_message": error}
    redis_publisher.publish(f"job_{job_id}", json.dumps(payload))


@celery_app.task(bind=True, name="parse_ufdr_archive")
def parse_ufdr_archive(self, job_id: str, object_key: str, case_id: str):
    # Guaranteed Cross-Platform Temp Path
    local_temp_path = os.path.join(tempfile.gettempdir(), object_key)
    
    try:
        # PHASE 0: Distributed Storage Download
        broadcast_update(job_id, "DOWNLOADING", "Downloading from Secure Object Storage...")
        logger.info(f"[JOB: {job_id}] Downloading {object_key} from MinIO to {local_temp_path}...")
        s3_worker_client.download_file(settings.MINIO_BUCKET, object_key, local_temp_path)

        # PHASE 1: Parsing
        broadcast_update(job_id, "PARSING", "Unzipping and extracting entities...")
        logger.info(f"[JOB: {job_id}] Phase 1: Parsing UFDR...")
        
        parsed_data = {"manifest": {}, "contacts": [], "calls": [], "messages": []}
        
        with tracer.start_as_current_span("unzip_and_parse_ufdr") as span:
            span.set_attribute("job_id", job_id)
            span.set_attribute("file_path", local_temp_path)
            
            with zipfile.ZipFile(local_temp_path, 'r') as archive:
                file_list = archive.namelist()
                
                if 'manifest.xml' in file_list:
                    with archive.open('manifest.xml') as f:
                        device_node = ET.parse(f).getroot().find('.//Device')
                        if device_node is not None:
                            parsed_data["manifest"]["imei"] = device_node.findtext('IMEI', 'Unknown')
                            parsed_data["manifest"]["model"] = device_node.findtext('Model', 'Unknown')
                            parsed_data["manifest"]["os"] = device_node.findtext('OSVersion', 'Unknown')
                            
                if 'contacts.json' in file_list:
                    with archive.open('contacts.json') as f:
                        parser = ijson.items(f, 'item')
                        for contact in parser: parsed_data["contacts"].append(contact)
                        
                if 'messages.json' in file_list:
                    with archive.open('messages.json') as f:
                        parser = ijson.items(f, 'item')
                        for message in parser: parsed_data["messages"].append(message)
                        
                if 'calls.csv' in file_list:
                    with archive.open('calls.csv') as f:
                        text_stream = io.TextIOWrapper(f, encoding='utf-8')
                        reader = csv.DictReader(text_stream)
                        for call in reader: parsed_data["calls"].append(call)

        # Safe Async Database Execution
        async def execute_db_inserts():
            await db.connect()  # Safely open pool in THIS loop
            try:
                broadcast_update(job_id, "SQL_DONE", "Inserting into PostgreSQL...")
                await bulk_insert_postgres(job_id, case_id, parsed_data)
                
                broadcast_update(job_id, "GRAPH_DONE", "Building Neo4j Topology...")
                await build_graph_neo4j(job_id, case_id, parsed_data)
            finally:
                await db.disconnect()  # Safely close pool
                
        asyncio.run(execute_db_inserts())

        # PHASE 4: Qdrant
        broadcast_update(job_id, "EMBEDDING", "Building Vector Index...")
        logger.info(f"[JOB: {job_id}] Phase 4: Building Vector Index...")
        build_vector_index(job_id, case_id, parsed_data)

        # SUCCESS
        broadcast_update(job_id, "SUCCESS", "Pipeline Complete.")
        logger.info(f"[JOB: {job_id}] ALL PHASES COMPLETE.")
        return {"status": "success", "job_id": job_id}

    except Exception as e:
        logger.error(f"[SAGA INITIATED] Pipeline failed. Error: {str(e)}")
        broadcast_update(job_id, "FAILED", error=str(e))
        
        # Safe Async Saga Rollback
        async def execute_rollback():
            await db.connect()  # Open connection specifically for rollback
            try:
                async with db.pg_pool.acquire() as conn:
                    await conn.execute("DELETE FROM messages WHERE job_id = $1", job_id)
                    await conn.execute("DELETE FROM calls WHERE job_id = $1", job_id)
                    await conn.execute("DELETE FROM contacts WHERE job_id = $1", job_id)
                    await conn.execute("DELETE FROM ingested_files WHERE case_id = $1", case_id)
                
                async with db.neo4j_driver.session() as session:
                    await session.run(
                        "MATCH (c:Case {case_id: $case_id}) OPTIONAL MATCH (c)-[:OWNS]->(n) DETACH DELETE c, n", 
                        case_id=case_id
                    )
                
                # Await Qdrant delete because it uses AsyncQdrantClient
                await db.qdrant_client.delete(
                    collection_name="forensic_chunks",
                    points_selector=models.Filter(
                        must=[models.FieldCondition(key="job_id", match=models.MatchValue(value=job_id))]
                    )
                )
                logger.info(f"[SAGA COMPLETE] Job {job_id} successfully rolled back.")
            except Exception as rollback_err:
                logger.critical(f"SAGA ROLLBACK FAILED: {str(rollback_err)}")
            finally:
                await db.disconnect()
                
        asyncio.run(execute_rollback())
        raise e
        
    finally:
        # CLEANUP
        if os.path.exists(local_temp_path):
            os.remove(local_temp_path)