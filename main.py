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
from typing import List, Dict, Any, Optional
from fastembed import TextEmbedding
from qdrant_client import models
from schemas import QueryRequest
from pydantic import ValidationError
import json
from schemas import QueryIntent, GraphNodeResult, HydratedEntity, CompiledRetrievalContext
from telemetry import configure_telemetry


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
    lifespan=lifespan
)

# Initialize observability for the API layer
configure_telemetry(app=app)


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

class HybridRetrievalService:
    def __init__(self, database, cohere_cli, embedder):
        self.db = database
        self.cohere = cohere_cli
        self.embedder = embedder

    async def classify_intent(self, query: str) -> QueryIntent:
        """Phase 1: Determine what databases need to be queried."""
        prompt = f"""Analyze this forensic query: '{query}'
        Output a JSON object with:
        - requires_graph (bool): True if asking for frequency, relationships, 'most contacted', or counts.
        - requires_semantic (bool): True if asking for conversational context, tone, or specific topics.
        - extracted_identifiers (list of str): Any explicit phone numbers or names mentioned.
        Respond ONLY with valid JSON."""
        
        try:
            response = await self.cohere.chat(message=prompt, model="command-r-08-2024")
            raw_json = response.text.replace('```json', '').replace('```', '').strip()
            return QueryIntent(**json.loads(raw_json))
        except Exception:
            # Fallback to querying everything if classification fails (Graceful Degradation)
            return QueryIntent(requires_graph=True, requires_semantic=True, extracted_identifiers=[])

    async def fetch_graph_topology(self, job_id: str) -> List[GraphNodeResult]:
        """Phase 2: Calculate actual relationship math in Neo4j."""
        results = []
        cypher = """
        MATCH (n:PhoneNumber)-[r:CONTACTED|CALLED]->(m:PhoneNumber)
        WHERE ($job_id IS NULL) OR ($job_id IS NOT NULL)
        RETURN n.e164 AS source, m.e164 AS target, type(r) AS rel_type, sum(r.count) AS frequency
        ORDER BY frequency DESC LIMIT 5
        """
        try:
            async with self.db.neo4j_driver.session() as session:
                records = await session.run(cypher, job_id=job_id)
                for record in await records.data():
                    results.append(GraphNodeResult(
                        source_number=record["source"],
                        target_number=record["target"],
                        interaction_type=record["rel_type"],
                        frequency=record["frequency"] or 1
                    ))
        except Exception as e:
            print(f"Graph retrieval failed: {e}")
        return results

    async def hydrate_identities(self, graph_results: list[GraphNodeResult]) -> list[HydratedEntity]:
        """Phase 3: Batch Hydration (O(1) Network Calls)"""
        unique_numbers = set()
        for res in graph_results:
            unique_numbers.add(res.source_number)
            unique_numbers.add(res.target_number)

        if not unique_numbers:
            return []

        hydrated = []
        async with self.db.pg_pool.acquire() as conn:
            # ONE single query using the Array Overlap operator (&&)
            # Casting $1 to ::text[] to ensure Postgres understands the type being send
            query = """
                SELECT display_name, organization, phone_numbers 
                FROM contacts 
                WHERE phone_numbers && $1::text[]
            """
            
            # Execute the query ONCE.
            rows = await conn.fetch(query, list(unique_numbers))

            # In-Memory Mapping (CPU is infinitely faster than Network I/O)
            for row in rows:
                db_phones = set(row["phone_numbers"])
                
                # Find which of our requested numbers match this specific contact
                matched_numbers = unique_numbers.intersection(db_phones)
                
                for number in matched_numbers:
                    hydrated.append(HydratedEntity(
                        phone_number=number,
                        display_name=row["display_name"],
                        organization=row["organization"]
                    ))
                    
        return hydrated

    async def fetch_semantic_chunks(self, query: str, job_id: str) -> List[Dict]:
        """Phase 4: Fetch context from Qdrant."""
        query_vector = list(self.embedder.embed([query]))[0].tolist()
        search_filter = None
        if job_id:
            search_filter = models.Filter(
                must=[models.FieldCondition(key="job_id", match=models.MatchValue(value=job_id))]
            )

        search_response = await self.db.qdrant_client.query_points(
            collection_name="forensic_chunks",
            query=query_vector,
            using="fast-bge-small-en-v1.5",
            query_filter=search_filter,
            limit=5 
        )

        documents = []
        for hit in search_response.points:
            payload = hit.payload
            documents.append({
                "id": str(hit.id),
                "text": payload.get("document", ""),
                "thread": payload.get("thread_id", "Unknown"),
                "timeframe": f"{payload.get('start_time')} to {payload.get('end_time')}"
            })
        return documents

    async def execute(self, request: QueryRequest) -> dict:
        """Orchestrates the Hybrid Data Flow."""
        intent = await self.classify_intent(request.query)
        
        context = CompiledRetrievalContext(query_intent=intent)

        if intent.requires_graph:
            context.graph_facts = await self.fetch_graph_topology(request.job_id)
            if context.graph_facts:
                context.hydrated_entities = await self.hydrate_identities(context.graph_facts)
                
        if intent.requires_semantic:
            context.semantic_chunks = await self.fetch_semantic_chunks(request.query, request.job_id)

        return context


@app.post("/api/v1/query", tags=["AI Analysis"])
async def query_forensic_data(request: QueryRequest):
    """
    Executes a Hybrid Retrieval-Augmented Generation (RAG) query.
    """
    # 1. Initialize Hybrid Service
    retriever = HybridRetrievalService(db, cohere_client, embedding_model)
    
    # 2. Execute Orchestration
    context_data = await retriever.execute(request)
    
    if not context_data.graph_facts and not context_data.semantic_chunks:
        return JSONResponse(status_code=404, content={"message": "No relevant forensic data found in graph or vector indices."})

    # 3. Compile the strictly formatted Anti-Hallucination prompt
    system_prompt = f"""## ROLE
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
        - When answering complex queries, briefly explain your reasoning based solely on the evidence before providing the final conclusion.

{context_data.compile_system_prompt()}
"""

    # 4. Final LLM Synthesis
    response = await cohere_client.chat(
        message=request.query,
        model="command-r-08-2024",
        preamble=system_prompt,
        # Passing the semantic chunks as discrete documents for Cohere's citation engine
        documents=context_data.semantic_chunks if context_data.semantic_chunks else None
    )

    formatted_citations = []
    if response.citations:
        for citation in response.citations:
            formatted_citations.append({
                "text_generated": citation.text,
                "document_ids_referenced": citation.document_ids
            })

    return {
        "query": request.query,
        "intent_detected": context_data.query_intent.dict(),
        "answer": response.text,
        "citations": formatted_citations,
        "hydrated_identities": [e.dict() for e in context_data.hydrated_entities]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main.py:app", host="0.0.0.0", port=8000, reload=True)