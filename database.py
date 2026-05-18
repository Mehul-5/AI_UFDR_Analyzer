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
    """
    Manages connection pools and clients for all core databases.
    Ensures connections are opened securely on startup and closed cleanly on shutdown.
    """
    def __init__(self):
        self.pg_pool = None
        self.neo4j_driver = None
        self.qdrant_client = None

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

            # 2. Neo4j Async Driver (With Retry Logic for Cold Starts)
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    self.neo4j_driver = AsyncGraphDatabase.driver(
                        settings.NEO4J_URI,
                        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
                    )
                    await self.neo4j_driver.verify_connectivity()
                    logger.info(" Neo4j driver connected.")
                    break  # Success, exit the retry loop
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"⏳ Neo4j not ready yet (Attempt {attempt + 1}/{max_retries}). Retrying in 5 seconds...")
                        await asyncio.sleep(5)
                    else:
                        logger.error(" Neo4j failed to connect after multiple attempts.")
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

# Export a single instance to be used across the application
db = DatabaseManager()