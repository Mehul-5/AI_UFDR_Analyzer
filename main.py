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
import boto3
from sse_starlette.sse import EventSourceResponse
import redis.asyncio as aioredis
from fastapi import Request
import re
from opentelemetry.propagate import inject

s3_client = boto3.client(
    's3', 
    endpoint_url=settings.MINIO_ENDPOINT,
    aws_access_key_id=settings.MINIO_ACCESS_KEY,
    aws_secret_access_key=settings.MINIO_SECRET_KEY
)

# Ensure bucket exists on startup
try:
    s3_client.create_bucket(Bucket=settings.MINIO_BUCKET)
except Exception:
    pass # Bucket already exists

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
    object_key = f"{job_id}_{file.filename}"
    hasher = hashlib.sha256()

    try:
        #  ENTERPRISE UPGRADE: Offload heavy file processing & S3 network calls to a thread
        def process_and_upload(f_obj, key):
            # 1. Calculate Hash
            f_obj.seek(0)
            while chunk := f_obj.read(8192):
                hasher.update(chunk)
            
            # 2. Upload to S3/MinIO
            f_obj.seek(0)
            s3_client.upload_fileobj(f_obj, settings.MINIO_BUCKET, key)

        # Execute securely outside the main async event loop
        await asyncio.to_thread(process_and_upload, file.file, object_key)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")

    file_hash = hasher.hexdigest()

    #  POSTGRES DUPLICATE CHECK
    async with db.pg_pool.acquire() as conn:
        existing_record = await conn.fetchval(
            "SELECT id FROM ingested_files WHERE case_id = $1 AND file_hash = $2",
            case_id, file_hash
        )
        
        if existing_record:
            # Cleanup the orphaned S3 object if it's a duplicate
            await asyncio.to_thread(s3_client.delete_object, Bucket=settings.MINIO_BUCKET, Key=object_key)
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

    #  DISPATCH TO CELERY: Inject W3C Trace Context
    celery_headers = {}
    inject(celery_headers) 
    
    task = parse_ufdr_archive.apply_async(
        args=[job_id, object_key, case_id], 
        task_id=job_id,
        headers=celery_headers 
    )
    
    return JSONResponse(
        status_code=202,
        content={
            "status": "QUEUED", "job_id": job_id, "case_id": case_id, "task_id": task.id,
            "message": "UFDR extraction dispatched to background worker."
        }
    )


@app.get("/api/v1/jobs/{job_id}/stream", tags=["Ingestion"])
async def stream_job_status(job_id: str, request: Request):
    """Server-Sent Events (SSE) endpoint for real-time Celery status."""
    async def event_generator():
        redis_client = aioredis.from_url(settings.REDIS_URL)
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(f"job_{job_id}")
        try:
            while True:
                if await request.is_disconnected():
                    break
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    yield {"data": message["data"].decode("utf-8")}
        finally:
            await pubsub.unsubscribe()
            await redis_client.aclose()
            
    return EventSourceResponse(event_generator())


class HybridRetrievalService:
    def __init__(self, database, cohere_cli, embedder):
        self.db = database
        self.cohere = cohere_cli
        self.embedder = embedder

    async def classify_intent(self, query: str) -> QueryIntent:
        prompt = f"""Analyze this forensic query: '{query}'
        You are an AI query planner. Output a JSON object with these fields:
        - requires_graph: (boolean) True if asking for frequency, relationships, counts, or timelines of calls/messages.
        - requires_sql_identity: (boolean) True if asking about contact names, aliases, or anomalies.
        - requires_semantic: (boolean) True if asking for conversational context, tone, or specific topics.
        - extracted_identifiers: (list of strings) Any explicit phone numbers or names mentioned.
        - optimized_search_queries: (list of strings) If requires_semantic is True, generate 1-3 highly specific, keyword-dense search phrases to run against the Vector Database. Exclude conversational words. (e.g. for "Are they talking about hiding money?", output ["hiding money", "offshore accounts", "wire transfer"]).
        Respond ONLY with valid JSON."""
        
        try:
            response = await self.cohere.chat(message=prompt, model="command-r-08-2024")
            raw_json = response.text.replace('```json', '').replace('```', '').strip()
            data = json.loads(raw_json)
            
            # Dynamically attach the optimized queries to the intent object
            intent = QueryIntent(**data)
            intent.optimized_search_queries = data.get("optimized_search_queries", [])
            return intent
        except Exception as e:
            print(f"Agentic Planner Failed: {e}")
            intent = QueryIntent(requires_graph=True, requires_sql_identity=True, requires_semantic=True)
            intent.optimized_search_queries = [query]
            return intent

    async def fetch_dynamic_identities(self, case_id: str, identifiers: list[str]) -> list[dict]:
        """Phase 2A: Dynamic SQL Retrieval. Searches for specific entities or falls back to anomalies."""
        async with self.db.pg_pool.acquire() as conn:
            if not identifiers:
                # Fallback: Macro-level identity anomalies
                query = """
                    SELECT phone, array_agg(DISTINCT display_name) as known_aliases
                    FROM (SELECT unnest(phone_numbers) as phone, display_name FROM contacts WHERE case_id = $1) sub
                    GROUP BY phone HAVING count(DISTINCT display_name) > 1;
                """
                rows = await conn.fetch(query, case_id)
                return [{"phone": row["phone"], "known_aliases": row["known_aliases"]} for row in rows]

            # Dynamic Target Search
            results = []
            for ident in identifiers:
                query = """
                    SELECT unnest(phone_numbers) as phone, array_agg(DISTINCT display_name) as known_aliases
                    FROM contacts
                    WHERE case_id = $1 AND (display_name ILIKE $2 OR $3 = ANY(phone_numbers))
                    GROUP BY phone
                """
                rows = await conn.fetch(query, case_id, f"%{ident}%", ident)
                for r in rows:
                    results.append({"phone": r["phone"], "known_aliases": r["known_aliases"]})
            return results

    async def fetch_dynamic_topology(self, case_id: str, target_phones: list[str]) -> list[GraphNodeResult]:
        """Phase 2B: Temporal Cypher Retrieval."""
        results = []
        if not target_phones:
            cypher = """
            MATCH (c:Case {case_id: $case_id})-[:OWNS]->(sender:PhoneNumber)-[:MADE_CALL|SENT_MESSAGE]->(event)-[:RECEIVED_BY]->(receiver:PhoneNumber)<-[:OWNS]-(c)
            RETURN sender.e164 AS source, receiver.e164 AS target, labels(event)[0] AS rel_type, 
                   count(event) AS frequency, 
                   collect(toString(coalesce(event.started_at, event.sent_at)))[0..15] AS interaction_times
            ORDER BY frequency DESC LIMIT 10
            """
            params = {"case_id": case_id}
        else:
            cypher = """
            MATCH (c:Case {case_id: $case_id})-[:OWNS]->(sender:PhoneNumber)-[:MADE_CALL|SENT_MESSAGE]->(event)-[:RECEIVED_BY]->(receiver:PhoneNumber)<-[:OWNS]-(c)
            WHERE sender.e164 IN $targets OR receiver.e164 IN $targets
            RETURN sender.e164 AS source, receiver.e164 AS target, labels(event)[0] AS rel_type, 
                   count(event) AS frequency, 
                   collect(toString(coalesce(event.started_at, event.sent_at)))[0..15] AS interaction_times
            ORDER BY frequency DESC LIMIT 25
            """
            params = {"case_id": case_id, "targets": target_phones}

        try:
            async with self.db.neo4j_driver.session() as session:
                records = await session.run(cypher, **params)
                for record in await records.data():
                    results.append(GraphNodeResult(
                        source_number=record["source"],
                        target_number=record["target"],
                        interaction_type=record["rel_type"],
                        frequency=record["frequency"] or 1,
                        interaction_times=record.get("interaction_times", []) # 🚀 Now passing times!
                    ))
        except Exception as e:
            print(f"Graph retrieval failed: {e}")
        return results

    async def hydrate_identities(self, graph_results: list[GraphNodeResult]) -> list[HydratedEntity]:
        unique_numbers = set()
        for res in graph_results:
            unique_numbers.add(res.source_number)
            unique_numbers.add(res.target_number)

        if not unique_numbers: return []

        hydrated = []
        async with self.db.pg_pool.acquire() as conn:
            query = "SELECT display_name, organization, phone_numbers FROM contacts WHERE phone_numbers && $1::text[]"
            rows = await conn.fetch(query, list(unique_numbers))
            for row in rows:
                matched_numbers = unique_numbers.intersection(set(row["phone_numbers"]))
                for number in matched_numbers:
                    hydrated.append(HydratedEntity(
                        phone_number=number, display_name=row["display_name"], organization=row["organization"]
                    ))
        return hydrated

    async def fetch_semantic_chunks(self, optimized_queries: list[str], case_id: str) -> list[dict]:
        if not optimized_queries:
            return []
            
        # Embed all optimized queries generated by the AI planner
        query_vectors = await asyncio.to_thread(lambda: list(self.embedder.embed(optimized_queries)))
        search_filter = models.Filter(must=[models.FieldCondition(key="case_id", match=models.MatchValue(value=case_id))]) if case_id else None

        all_points = []
        # Search Qdrant for each optimized query
        for vector in query_vectors:
            search_response = await self.db.qdrant_client.query_points(
                collection_name="forensic_chunks", query=vector.tolist(), using="fast-bge-small-en-v1.5", query_filter=search_filter, limit=4 
            )
            all_points.extend(search_response.points)

        # Deduplicate results based on chunk ID
        unique_chunks = {str(hit.id): hit for hit in all_points}

        return [{
            "id": chunk_id, "text": hit.payload.get("document", ""),
            "thread": hit.payload.get("thread_id", "Unknown"),
            "timeframe": f"{hit.payload.get('start_time')} to {hit.payload.get('end_time')}"
        } for chunk_id, hit in unique_chunks.items()]

    async def execute(self, request: QueryRequest) -> CompiledRetrievalContext:
        """The Orchestrator - Upgraded for Semantic-to-Graph Cross-Pollination"""
        import re # Required for cross-pollination regex
        
        intent = await self.classify_intent(request.query)
        context = CompiledRetrievalContext(query_intent=intent)
        resolved_phones = []

        # 1. SQL Resolution: Turn Names into Phone Numbers
        if intent.requires_sql_identity or intent.extracted_identifiers:
            context.sql_facts = await self.fetch_dynamic_identities(request.case_id, intent.extracted_identifiers)
            resolved_phones.extend([fact["phone"] for fact in context.sql_facts])
            
            # Normalize explicit phone numbers (strip dashes/spaces so +1-555 becomes +1555)
            for ident in intent.extracted_identifiers:
                clean_phone = ''.join(c for c in ident if c.isdigit() or c == '+')
                if len(clean_phone) >= 7:
                    resolved_phones.append(clean_phone)

        # 2. Semantic Retrieval 
        if intent.requires_semantic:
            search_queries = intent.optimized_search_queries if intent.optimized_search_queries else [request.query]
            
            context.semantic_chunks = await self.fetch_semantic_chunks(search_queries, request.case_id)
            
            # CROSS-POLLINATION: Extract phone numbers from the semantic text
            if context.semantic_chunks:
                for chunk in context.semantic_chunks:
                    phones = re.findall(r'\+?\d{7,15}', chunk["text"])
                    resolved_phones.extend(phones)

        # Deduplicate the phone numbers
        resolved_phones = list(set(resolved_phones))

        # 3. Graph Retrieval 
        if intent.requires_graph or resolved_phones:
            context.graph_facts = await self.fetch_dynamic_topology(request.case_id, resolved_phones)
            if context.graph_facts:
                context.hydrated_entities = await self.hydrate_identities(context.graph_facts)

        return context


@app.post("/api/v1/query", tags=["AI Analysis"])
async def query_forensic_data(request: QueryRequest):
    """
    Executes a Hybrid Retrieval-Augmented Generation (RAG) query.
    """
    retriever = HybridRetrievalService(db, cohere_client, embedding_model)
    context_data = await retriever.execute(request)

    if not context_data.graph_facts and not context_data.semantic_chunks and not context_data.sql_facts:
        return {
            "query": request.query,
            "intent_detected": context_data.query_intent.dict(),
            "answer": "No relevant forensic data was found in the database to answer this query. The specific identifiers or topics mentioned do not exist in this extraction.",
            "citations": [],
            "hydrated_identities": [],
            "graph_facts": [],
            "sql_facts": []
        }

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
    
@app.delete("/api/v1/cases/{case_id}", tags=["Case Management"])
async def delete_case(case_id: str):
    """Surgically deletes a case and all its data across all 3 databases AND Cloud Storage."""
    try:
        async with db.pg_pool.acquire() as conn:
            # 1. Fetch all associated Job IDs for this case to locate the raw files in MinIO
            jobs = await conn.fetch("""
                SELECT DISTINCT job_id FROM contacts WHERE case_id = $1 
                UNION 
                SELECT DISTINCT job_id FROM messages WHERE case_id = $1 
                UNION 
                SELECT DISTINCT job_id FROM calls WHERE case_id = $1
            """, case_id)
            
            job_ids = [job['job_id'] for job in jobs]

            # 2. Delete raw .ufdr files from MinIO (Cloud Storage)
            for j_id in job_ids:
                # List objects matching the job_id prefix
                response = await asyncio.to_thread(
                    s3_client.list_objects_v2, 
                    Bucket=settings.MINIO_BUCKET, 
                    Prefix=j_id
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        await asyncio.to_thread(
                            s3_client.delete_object, 
                            Bucket=settings.MINIO_BUCKET, 
                            Key=obj['Key']
                        )

            # 3. Delete structured data from PostgreSQL
            await conn.execute("DELETE FROM messages WHERE case_id = $1", case_id)
            await conn.execute("DELETE FROM calls WHERE case_id = $1", case_id)
            await conn.execute("DELETE FROM contacts WHERE case_id = $1", case_id)
            await conn.execute("DELETE FROM ingested_files WHERE case_id = $1", case_id)

        # 4. Delete topological data from Neo4j
        async with db.neo4j_driver.session() as session:
            await session.run(
                "MATCH (c:Case {case_id: $case_id}) OPTIONAL MATCH (c)-[:OWNS]->(n) DETACH DELETE c, n", 
                case_id=case_id
            )

        # 5. Delete semantic vectors from Qdrant
        await db.qdrant_client.delete(
            collection_name="forensic_chunks",
            points_selector=models.Filter(
                must=[models.FieldCondition(key="case_id", match=models.MatchValue(value=case_id))]
            )
        )

        return JSONResponse(
            status_code=200, 
            content={"message": f"Case {case_id} and all associated raw cloud files successfully purged."}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete case: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main.py:app", host="0.0.0.0", port=8000, reload=True)