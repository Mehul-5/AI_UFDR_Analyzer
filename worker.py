from celery import Celery
import logging
import zipfile
import json
import csv
import io
import xml.etree.ElementTree as ET
import asyncio
import asyncpg
from datetime import datetime
from neo4j import AsyncGraphDatabase
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
            contact_tuples = [
                (
                    job_id,
                    c.get("contact_id"),
                    c.get("display_name"),
                    c.get("phone_numbers", []),
                    c.get("organization"),
                    c.get("is_foreign", False)
                )
                for c in parsed_data["contacts"]
            ]
            await conn.executemany("""
                INSERT INTO contacts (job_id, contact_id, display_name, phone_numbers, organization, is_foreign)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (job_id, contact_id) DO NOTHING
            """, contact_tuples)

        if parsed_data["calls"]:
            call_tuples = [
                (
                    job_id,
                    c.get("call_id"),
                    c.get("caller_phone"),
                    c.get("callee_phone"),
                    c.get("direction"),
                    c.get("call_type"),
                    int(c.get("duration_seconds", 0)),
                    parse_iso_date(c.get("started_at"))
                )
                for c in parsed_data["calls"]
            ]
            await conn.executemany("""
                INSERT INTO calls (job_id, call_id, caller_phone, callee_phone, direction, call_type, duration_seconds, started_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (job_id, call_id) DO NOTHING
            """, call_tuples)

        if parsed_data["messages"]:
            message_tuples = [
                (
                    job_id,
                    m.get("message_id"),
                    m.get("thread_id"),
                    m.get("platform"),
                    m.get("sender_phone"),
                    m.get("recipient_phones", []),
                    m.get("content_text"),
                    parse_iso_date(m.get("sent_at")),
                    m.get("is_deleted", False)
                )
                for m in parsed_data["messages"]
            ]
            await conn.executemany("""
                INSERT INTO messages (job_id, message_id, thread_id, platform, sender_phone, recipient_phones, content_text, sent_at, is_deleted)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (job_id, message_id) DO NOTHING
            """, message_tuples)

    finally:
        await conn.close()

async def build_graph_neo4j(job_id: str, parsed_data: dict):
    """
    Asynchronously builds the graph topology in Neo4j.
    Uses UNWIND for bulk processing and MERGE for idempotency.
    """
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    )
    
    try:
        async with driver.session() as session:
            # 1. Ingest Contacts & Phone Numbers
            if parsed_data["contacts"]:
                await session.run("""
                    UNWIND $contacts AS c
                    MERGE (ct:Contact {contact_id: c.contact_id})
                    SET ct.display_name = c.display_name, ct.organization = c.organization, ct.job_id = $job_id
                    WITH ct, c
                    UNWIND c.phone_numbers AS phone
                    MERGE (pn:PhoneNumber {e164: phone})
                    MERGE (pn)-[:ASSIGNED_TO]->(ct)
                """, contacts=parsed_data["contacts"], job_id=job_id)
                logger.info(f"Graph nodes merged for {len(parsed_data['contacts'])} contacts.")

            # 2. Ingest Messages & Communication Edges
            if parsed_data["messages"]:
                await session.run("""
                    UNWIND $messages AS m
                    MERGE (msg:Message {message_id: m.message_id})
                    SET msg.platform = m.platform, msg.sent_at = datetime(m.sent_at), msg.job_id = $job_id
                    WITH msg, m
                    MERGE (sender:PhoneNumber {e164: m.sender_phone})
                    MERGE (msg)-[:SENT_BY]->(sender)
                    WITH msg, m, sender
                    UNWIND m.recipient_phones AS recip_phone
                    MERGE (recip:PhoneNumber {e164: recip_phone})
                    MERGE (msg)-[:RECEIVED_BY]->(recip)
                    
                    // The core analytical edge: aggregate communications
                    MERGE (sender)-[rel:CONTACTED]->(recip)
                    ON CREATE SET rel.count = 1, rel.last_contact = datetime(m.sent_at)
                    ON MATCH SET 
                        rel.count = rel.count + 1, 
                        rel.last_contact = CASE WHEN datetime(m.sent_at) > coalesce(rel.last_contact, datetime("1970-01-01T00:00:00Z")) THEN datetime(m.sent_at) ELSE rel.last_contact END
                """, messages=parsed_data["messages"], job_id=job_id)
                logger.info(f"Graph edges merged for {len(parsed_data['messages'])} messages.")

            # 3. Ingest Calls (Creates a CALLED edge)
            if parsed_data["calls"]:
                await session.run("""
                    UNWIND $calls AS c
                    MERGE (caller:PhoneNumber {e164: c.caller_phone})
                    MERGE (callee:PhoneNumber {e164: c.callee_phone})
                    MERGE (caller)-[rel:CALLED]->(callee)
                    ON CREATE SET rel.count = 1, rel.total_duration = toInteger(c.duration_seconds)
                    ON MATCH SET 
                        rel.count = rel.count + 1, 
                        rel.total_duration = rel.total_duration + toInteger(c.duration_seconds)
                """, calls=parsed_data["calls"], job_id=job_id)
                logger.info(f"Graph edges merged for {len(parsed_data['calls'])} calls.")

    finally:
        await driver.close()


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
                    tree = ET.parse(f)
                    root = tree.getroot()
                    device_node = root.find('.//Device')
                    if device_node is not None:
                        parsed_data["manifest"]["imei"] = device_node.findtext('IMEI', 'Unknown')
                        parsed_data["manifest"]["model"] = device_node.findtext('Model', 'Unknown')
                        parsed_data["manifest"]["os"] = device_node.findtext('OSVersion', 'Unknown')
            
            if 'contacts.json' in file_list:
                with zf.open('contacts.json') as f:
                    parsed_data["contacts"] = json.load(f)
                    
            if 'messages.json' in file_list:
                with zf.open('messages.json') as f:
                    parsed_data["messages"] = json.load(f)
                    
            if 'calls.csv' in file_list:
                with zf.open('calls.csv') as f:
                    csv_data = f.read().decode('utf-8')
                    reader = csv.DictReader(io.StringIO(csv_data))
                    parsed_data["calls"] = list(reader)

        logger.info(f"[JOB: {job_id}] Phase 1 Complete.")

        # PHASE 2: POSTGRESQL BULK INSERT
        logger.info(f"[JOB: {job_id}] Phase 2: Inserting into PostgreSQL...")
        asyncio.run(bulk_insert_postgres(job_id, parsed_data))
        logger.info(f"[JOB: {job_id}] Phase 2 Complete.")
        
        # PHASE 3: NEO4J GRAPH CONSTRUCTION
        logger.info(f"[JOB: {job_id}] Phase 3: Building Neo4j Graph Topology...")
        asyncio.run(build_graph_neo4j(job_id, parsed_data))
        logger.info(f"[JOB: {job_id}] Phase 3 Complete.")

        return {
            "status": "success", 
            "job_id": job_id,
            "database_sync": "pg_and_neo4j_complete"
        }

    except zipfile.BadZipFile:
        logger.error(f"[JOB: {job_id}] Failed: File is not a valid archive.")
        raise
    except Exception as e:
        logger.error(f"[JOB: {job_id}] Failed: {str(e)}")
        raise self.retry(exc=e, countdown=2 ** self.request.retries)