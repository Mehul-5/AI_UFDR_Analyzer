import asyncpg
from neo4j import AsyncGraphDatabase
from qdrant_client import AsyncQdrantClient
import logging
import asyncio
from config import settings

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.pg_pool = None
        self.neo4j_driver = None
        self.qdrant_client = None

    async def init_models(self):
        """
        Creates the PostgreSQL schema if it does not exist.
        Uses UNIQUE constraints on (job_id, entity_id) to guarantee idempotency.
        """
        logger.info("Verifying PostgreSQL schema...")
        async with self.pg_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS contacts (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    job_id TEXT NOT NULL,
                    contact_id TEXT NOT NULL,
                    display_name TEXT,
                    phone_numbers TEXT[],
                    organization TEXT,
                    is_foreign BOOLEAN,
                    UNIQUE(job_id, contact_id)
                );

                CREATE TABLE IF NOT EXISTS calls (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    job_id TEXT NOT NULL,
                    call_id TEXT NOT NULL,
                    caller_phone TEXT,
                    callee_phone TEXT,
                    direction TEXT,
                    call_type TEXT,
                    duration_seconds INT,
                    started_at TIMESTAMPTZ,
                    UNIQUE(job_id, call_id)
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    job_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    thread_id TEXT,
                    platform TEXT,
                    sender_phone TEXT,
                    recipient_phones TEXT[],
                    content_text TEXT,
                    sent_at TIMESTAMPTZ,
                    is_deleted BOOLEAN,
                    UNIQUE(job_id, message_id)
                );
            """)
        logger.info(" PostgreSQL schema verified.")

    async def connect(self):
        logger.info("Initializing database connections...")
        try:
            # 1. PostgreSQL Connection Pool
            self.pg_pool = await asyncpg.create_pool(
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.POSTGRES_DB,
                host=settings.POSTGRES_SERVER,
                port=settings.POSTGRES_PORT,
                min_size=2,
                max_size=10
            )
            logger.info(" PostgreSQL pool established.")
            
            # --- NEW: Initialize the tables immediately after connecting ---
            await self.init_models()

            # 2. Neo4j Async Driver (With Retry Logic)
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    self.neo4j_driver = AsyncGraphDatabase.driver(
                        settings.NEO4J_URI,
                        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
                    )
                    await self.neo4j_driver.verify_connectivity()
                    logger.info(" Neo4j driver connected.")
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"⏳ Neo4j not ready yet. Retrying in 5 seconds...")
                        await asyncio.sleep(5)
                    else:
                        logger.error(" Neo4j failed to connect.")
                        raise e

            # 3. Qdrant Async Client
            self.qdrant_client = AsyncQdrantClient(
                host=settings.QDRANT_HOST,
                port=settings.QDRANT_PORT
            )
            logger.info(" Qdrant client initialized.")
            
        except Exception as e:
            logger.error(f" Failed to connect to databases: {str(e)}")
            raise e

    async def disconnect(self):
        logger.info("Closing database connections...")
        if self.pg_pool:
            await self.pg_pool.close()
        if self.neo4j_driver:
            await self.neo4j_driver.close()
        if self.qdrant_client:
            await self.qdrant_client.close()
        logger.info("All database connections cleanly closed.")

db = DatabaseManager()