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
import cohere
from fastembed import TextEmbedding
from qdrant_client import models
from schemas import QueryRequest

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI Lifespan Context Manager.
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

# Initialize AI Models Globally
embedding_model = TextEmbedding("BAAI/bge-small-en-v1.5")
cohere_client = cohere.AsyncClient(settings.COHERE_API_KEY)

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
    Receives a UFDR file via multipart form-data.
    Streams it to disk and enqueues an asynchronous Celery parsing task.
    """
    # STRICT ENFORCEMENT: Only allow .ufdr extensions
    if not file.filename.lower().endswith('.ufdr'):
        raise HTTPException(
            status_code=400, 
            detail="Invalid file type. Must be a .ufdr forensic extraction archive."
        )

    job_id = str(uuid.uuid4())
    secure_file_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")

    # PHASE 0: Stream the file to disk securely
    try:
        async with aiofiles.open(secure_file_path, 'wb') as out_file:
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
            "message": "UFDR file received and queued for background processing."
        }
    )

@app.post("/api/v1/query", tags=["AI Analysis"])
async def query_forensic_data(request: QueryRequest):
    """
    Executes a Retrieval-Augmented Generation (RAG) query.
    """
    # 1. Convert the text question into a vector array explicitly
    query_vector = list(embedding_model.embed([request.query]))[0].tolist()

    # 2. Build the Qdrant search filter
    search_filter = None
    if request.job_id:
        search_filter = models.Filter(
            must=[models.FieldCondition(key="job_id", match=models.MatchValue(value=request.job_id))]
        )

    # 3. Execute Vector Search with the explicit mathematical vector
    search_response = await db.qdrant_client.query_points(
        collection_name="forensic_chunks",
        query=query_vector,
        using="fast-bge-small-en-v1.5",
        query_filter=search_filter,
        limit=5 
    )

    search_results = search_response.points

    if not search_results:
        return JSONResponse(status_code=404, content={"message": "No relevant forensic data found."})

    # 4. Format the retrieved vectors into structured documents for Cohere
    documents = []
    for hit in search_results:
        payload = hit.payload
        documents.append({
            "id": str(hit.id),  # Cohere requires IDs to be strings
            "text": payload.get("document", ""),
            "thread": payload.get("thread_id", "Unknown"),
            "timeframe": f"{payload.get('start_time')} to {payload.get('end_time')}"
        })

    # 5. Execute Grounded AI Synthesis via Cohere
    system_prompt = """## ROLE
        You are an expert Digital Forensics and E-Discovery AI Analyst. Your mandate is to assist human investigators by analyzing extracted mobile device data (chat threads, call records, and forensic metadata).

        ## CORE DIRECTIVE
        Answer the investigator's query STRICTLY and EXCLUSIVELY using the provided retrieved documents. Treat the provided context as immutable digital evidence. 

        ## RULES OF ENGAGEMENT
        1. ZERO HALLUCINATION: Never invent, infer, or assume information outside the provided text. If the evidence required to answer the query is not present, state exactly: "The provided forensic extraction does not contain evidence to answer this query."
        2. OBJECTIVE TONE: Maintain a clinical, objective, and unbiased tone. Do not judge the morality or legality of the actions described. Report only the facts.
        3. FORENSIC PRECISION: When referencing technical identifiers (cryptocurrency addresses, phone numbers, IMEI, IP addresses, or email addresses), quote them EXACTLY as they appear. Never truncate or alter them.
        4. TEMPORAL ACCURACY: When summarizing events, present them in chronological order. Maintain the exact timestamps and timezones provided in the logs.
        5. IDENTITY RESOLUTION: Do not assume the identity of an unknown phone number unless it is explicitly linked to a name in the provided context. Refer to unknown participants by their raw identifier.
        6. ARTIFACT AWARENESS: If a document explicitly mentions that a message or record was "deleted", "recovered", or is part of an incomplete fragment, highlight this forensic status in your response.

        ## OUTPUT FORMAT
        - Be direct. Omit conversational filler (e.g., do not say "Here is the information you requested").
        - Use bullet points when listing multiple entities, transactions, or timelines.
        - When answering complex queries, briefly explain your reasoning based solely on the evidence before providing the final conclusion."""

    response = await cohere_client.chat(
        message=request.query,
        model="command-r-08-2024",
        documents=documents,
        preamble=system_prompt
    )

    # 6. Format the final output to include exact citations
    formatted_citations = []
    if response.citations:
        for citation in response.citations:
            formatted_citations.append({
                "text_generated": citation.text,
                "document_ids_referenced": citation.document_ids
            })

    return {
        "query": request.query,
        "answer": response.text,
        "citations": formatted_citations,
        "sources_scanned": documents
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main.py:app", host="0.0.0.0", port=8000, reload=True)