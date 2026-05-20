from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from config import settings
from database import db
import uuid
import os
import aiofiles
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from schemas import ExtractionResponse
from worker import parse_ufdr_archive

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

# Ensuring a directory exists to store the uploaded files securely
UPLOAD_DIR = "secure_store"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/api/v1/extract", tags=["Ingestion"])
async def upload_ufdr(
    file: UploadFile = File(...),
    case_id: str = "default-case-001"
):
    """
    Receives a UFDR zip file via multipart form-data.
    Streams it to disk and enqueues an asynchronous Celery parsing task.
    """
    if not file.filename.endswith('.zip') and not file.filename.endswith('.ufdr'):
        raise HTTPException(status_code=400, detail="Invalid file type. Must be .zip or .ufdr")

    job_id = str(uuid.uuid4())
    secure_file_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")

    # PHASE 0: Stream the file to disk (Does not block memory!)
    try:
        async with aiofiles.open(secure_file_path, 'wb') as out_file:
            # Read in chunks of 1MB to keep memory footprint flat, even for 400MB files
            while content := await file.read(1024 * 1024):  
                await out_file.write(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    # Enqueue the background task to Celery
    task = parse_ufdr_archive.apply_async(args=[job_id, secure_file_path])

    # Return HTTP 202 Accepted immediately
    return JSONResponse(
        status_code=202,
        content={
            "job_id": job_id,
            "case_id": case_id,
            "task_id": task.id,
            "status": "QUEUED",
            "message": "File received and queued for background processing."
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main.py:app", host="0.0.0.0", port=8000, reload=True)