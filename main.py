from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from config import settings
from database import db

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI Lifespan Context Manager.
    Code before the 'yield' runs on server startup.
    Code after the 'yield' runs on server shutdown.
    """
    # STARTUP: Connect to all databases
    await db.connect()
    
    yield  # Application runs here
    
    # SHUTDOWN: Close all database connections securely
    await db.disconnect()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="API for ingesting and querying forensic extraction reports.",
    lifespan=lifespan  # Attach the lifespan manager here
)

@app.get("/health", tags=["System"])
async def health_check():
    """
    Advanced health check endpoint.
    Verifies that the API is running AND that database objects exist.
    """
    return JSONResponse(
        status_code=200,
        content={
            "status": "online",
            "project": settings.PROJECT_NAME,
            "connections": {
                "postgres": db.pg_pool is not None,
                "neo4j": db.neo4j_driver is not None,
                "qdrant": db.qdrant_client is not None
            }
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main.py:app", host="0.0.0.0", port=8000, reload=True)