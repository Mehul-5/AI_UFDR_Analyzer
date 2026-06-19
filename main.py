import asyncio
import hashlib
import os
import uuid
import aiofiles
from contextlib import asynccontextmanager
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import cohere
from fastembed import TextEmbedding
from qdrant_client import models
import json

from worker import celery_app, parse_ufdr_archive
from config import settings
from database import db
from schemas import QueryRequest, QueryIntent, GraphNodeResult, HydratedEntity, CompiledRetrievalContext
from telemetry import configure_telemetry
from llm_service import CohereStrategy, CircuitBreakerLLM

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    yield  
    await db.disconnect()

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="API for ingesting and querying forensic extraction reports.",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

configure_telemetry(app=app)

embedding_model = TextEmbedding("BAAI/bge-small-en-v1.5")
cohere_client = cohere.AsyncClient(settings.COHERE_API_KEY)
llm_gateway = CircuitBreakerLLM(strategy=CohereStrategy())

UPLOAD_DIR = "secure_store"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@app.get("/health", tags=["System"])
async def health_check():
    return JSONResponse(
        status_code=200,
        content={"status": "online", "project": settings.PROJECT_NAME}
    )


@app.post("/api/v1/extract", tags=["Ingestion"])
async def extract_ufdr(case_id: str, file: UploadFile = File(...)):
    if not file.filename.lower().endswith('.ufdr'):
        raise HTTPException(status_code=400, detail="Invalid file type. Must be a .ufdr archive.")

    job_id = str(uuid.uuid4())
    secure_file_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")
    hasher = hashlib.sha256()

    try:
        async with aiofiles.open(secure_file_path, 'wb') as out_file:
            while content := await file.read(1024 * 1024):  
                await asyncio.to_thread(hasher.update, content)
                await out_file.write(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    file_hash = hasher.hexdigest()

    async with db.pg_pool.acquire() as conn:
        existing_record = await conn.fetchval(
            "SELECT id FROM ingested_files WHERE case_id = $1 AND file_hash = $2",
            case_id, file_hash
        )
        
        if existing_record:
            os.remove(secure_file_path) 
            return JSONResponse(
                status_code=409, 
                content={
                    "status": "CONFLICT", "job_id": None, "case_id": case_id,
                    "message": "This exact file has already been ingested into this case."
                }
            )
            
        await conn.execute(
            "INSERT INTO ingested_files (case_id, file_hash, filename) VALUES ($1, $2, $3)",
            case_id, file_hash, file.filename
        )

    task = parse_ufdr_archive.apply_async(args=[job_id, secure_file_path, case_id], task_id=job_id)
    
    return JSONResponse(
        status_code=202,
        content={
            "status": "QUEUED", "job_id": job_id, "case_id": case_id, "task_id": task.id,
            "message": "UFDR extraction dispatched to background worker."
        }
    )


@app.get("/api/v1/jobs/{job_id}/status", tags=["Ingestion"])
async def get_job_status(job_id: str):
    """Fetch the real-time status of the Celery worker."""
    task = celery_app.AsyncResult(job_id)
    return JSONResponse(status_code=200, content={
        "job_id": job_id,
        "status": task.state,
        "error_message": str(task.info) if task.state == 'FAILURE' else None
    })


class HybridRetrievalService:
    def __init__(self, database, cohere_cli, embedder):
        self.db = database
        self.cohere = cohere_cli
        self.embedder = embedder

    async def classify_intent(self, query: str) -> QueryIntent:
        prompt = f"""Analyze this forensic query: '{query}'
        Output a JSON object with these boolean flags:
        - requires_graph: True ONLY if asking for frequency, relationships, or counts of calls/messages.
        - requires_sql_identity: True if asking about contact names, aliases, anomalies, or who a number is saved as.
        - requires_semantic: True if asking for conversational context, tone, or specific topics.
        - extracted_identifiers: Any explicit phone numbers or names mentioned.
        Respond ONLY with valid JSON."""
        
        try:
            response = await self.cohere.chat(message=prompt, model="command-r-08-2024")
            raw_json = response.text.replace('```json', '').replace('```', '').strip()
            return QueryIntent(**json.loads(raw_json))
        except Exception:
            return QueryIntent(requires_graph=True, requires_sql_identity=True, requires_semantic=True)

    async def fetch_identity_anomalies(self, case_id: str) -> list[dict]:
        """Phase 2A: Relational SQL Macro for Identity Resolution"""
        async with self.db.pg_pool.acquire() as conn:
            query = """
                SELECT phone, array_agg(DISTINCT display_name) as known_aliases
                FROM (
                    SELECT unnest(phone_numbers) as phone, display_name
                    FROM contacts
                    WHERE case_id = $1 AND display_name IS NOT NULL
                ) subquery
                GROUP BY phone
                HAVING count(DISTINCT display_name) > 1;
            """
            rows = await conn.fetch(query, case_id)
            return [{"phone": row["phone"], "known_aliases": row["known_aliases"]} for row in rows]

    async def fetch_graph_topology(self, case_id: str) -> list[GraphNodeResult]:
        """Phase 2B: Calculate actual relationship math in Neo4j bounded by Case RBAC."""
        results = []
        cypher = """
        MATCH (c:Case {case_id: $case_id})-[:OWNS]->(sender:PhoneNumber)-[:MADE_CALL|SENT_MESSAGE]->(event)-[:RECEIVED_BY]->(receiver:PhoneNumber)<-[:OWNS]-(c)
        RETURN sender.e164 AS source, receiver.e164 AS target, labels(event)[0] AS rel_type, count(event) AS frequency
        ORDER BY frequency DESC LIMIT 10
        """
        try:
            async with self.db.neo4j_driver.session() as session:
                records = await session.run(cypher, case_id=case_id)
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
            query = """
                SELECT display_name, organization, phone_numbers 
                FROM contacts 
                WHERE phone_numbers && $1::text[]
            """
            rows = await conn.fetch(query, list(unique_numbers))

            for row in rows:
                db_phones = set(row["phone_numbers"])
                matched_numbers = unique_numbers.intersection(db_phones)
                
                for number in matched_numbers:
                    hydrated.append(HydratedEntity(
                        phone_number=number,
                        display_name=row["display_name"],
                        organization=row["organization"]
                    ))
        return hydrated

    async def fetch_semantic_chunks(self, query: str, case_id: str) -> list[dict]:
        """Phase 4: Fetch context from Qdrant using case_id."""
        query_vector = list(self.embedder.embed([query]))[0].tolist()
        
        search_filter = None
        if case_id:
            search_filter = models.Filter(
                must=[models.FieldCondition(key="case_id", match=models.MatchValue(value=case_id))]
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

    async def execute(self, request: QueryRequest) -> CompiledRetrievalContext:
        """Orchestrates the Intent-Based Data Flow."""
        intent = await self.classify_intent(request.query)
        context = CompiledRetrievalContext(query_intent=intent)

        # 1. Fetch Identities (if needed)
        if intent.requires_sql_identity:
            context.sql_facts = await self.fetch_identity_anomalies(request.case_id)

        # 2. Fetch Graph Topology (if needed)
        if intent.requires_graph:
            context.graph_facts = await self.fetch_graph_topology(request.case_id)
            if context.graph_facts:
                context.hydrated_entities = await self.hydrate_identities(context.graph_facts)
                
        # 3. Fetch Semantic Texts (if needed)
        if intent.requires_semantic:
            context.semantic_chunks = await self.fetch_semantic_chunks(request.query, request.case_id)

        return context


@app.post("/api/v1/query", tags=["AI Analysis"])
async def query_forensic_data(request: QueryRequest):
    """
    Executes a Hybrid Retrieval-Augmented Generation (RAG) query.
    """
    retriever = HybridRetrievalService(db, cohere_client, embedding_model)
    context_data = await retriever.execute(request)
    
    if not context_data.graph_facts and not context_data.semantic_chunks and not context_data.sql_facts:
        return JSONResponse(
            status_code=404, 
            content={"message": "No relevant forensic data found in indices."}
        )

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

    try:
        response_payload = await llm_gateway.generate_response(
            prompt=request.query,
            system_message=system_prompt,
            documents=context_data.semantic_chunks if context_data.semantic_chunks else []
        )
        
        response_payload["query"] = request.query
        response_payload["intent_detected"] = context_data.query_intent.dict()
        response_payload["hydrated_identities"] = [e.dict() for e in context_data.hydrated_entities]
        response_payload["graph_facts"] = [f.dict() for f in context_data.graph_facts]
        response_payload["sql_facts"] = [s for s in context_data.sql_facts]
        
        return response_payload

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Synthesis Engine Failure: {str(e)}")
    

@app.get("/api/v1/cases", tags=["Case Management"])
async def list_cases():
    """Fetches all unique cases that have been ingested into Neo4j."""
    try:
        async with db.neo4j_driver.session() as session:
            records = await session.run("MATCH (c:Case) RETURN c.case_id AS case_id ORDER BY c.case_id LIMIT 50")
            cases = [record["case_id"] for record in await records.data()]
            return {"cases": cases}
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Failed to fetch cases: {str(e)}"})


@app.get("/api/v1/cases/{case_id}/graph", tags=["Case Management"])
async def get_full_graph(case_id: str):
    """Fetches the macro topology with full properties for the inspector panel."""
    cypher = """
    MATCH (c:Case {case_id: $case_id})-[:OWNS]->(n)
    OPTIONAL MATCH (n)-[r]->(m)<-[:OWNS]-(c)
    WHERE type(r) IS NOT NULL AND type(r) <> 'OWNS'
    RETURN 
        elementId(n) AS n_id, labels(n)[0] AS n_label, properties(n) AS n_props,
        type(r) AS rel_type, properties(r) AS rel_props,
        elementId(m) AS m_id
    LIMIT 1500
    """
    try:
        async with db.neo4j_driver.session() as session:
            records = await session.run(cypher, case_id=case_id)
            data = await records.data()
            
            nodes_dict = {}
            links = []
            
            for row in data:
                n_id = row["n_id"]
                if n_id not in nodes_dict:
                    nodes_dict[n_id] = {
                        "id": n_id, 
                        "label": row["n_label"], 
                        "name": row["n_props"].get("display_name") or row["n_props"].get("e164") or row["n_label"],
                        "properties": row["n_props"]
                    }
                
                if row["m_id"]:
                    links.append({
                        "source": n_id, 
                        "target": row["m_id"], 
                        "label": row["rel_type"],
                        "properties": row["rel_props"] or {}
                    })
            
            color_map = {"PhoneNumber": "#3b82f6", "Contact": "#10b981", "Message": "#f43f5e", "Call": "#f59e0b", "Device": "#8b5cf6"}
            for n in nodes_dict.values():
                n["color"] = color_map.get(n["label"], "#94a3b8")

            return {"nodes": list(nodes_dict.values()), "links": links}
            
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Graph retrieval failed: {str(e)}"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main.py:app", host="0.0.0.0", port=8000, reload=True)