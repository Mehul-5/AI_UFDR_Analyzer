*   **A. Problem Statement:** During digital forensic investigations, UFDR reports obtained from seized digital devices contain massive amounts of fragmented data (chats, calls, contacts). Going through this manually takes an immense amount of time and delays finding critical evidence. Investigating Officers need a tool that can instantly correlate connections across multi-device data streams and surface insights via an intuitive interface.

*   **B. Functional Requirements:** 
    *   Ingestion pipeline: Forensic files uploaded via a React frontend, handled by a FastAPI stream/chunk parser, and written sequentially to a unified PostgreSQL source of truth.
    *   Derived Sync: Data must propagate from PostgreSQL to Neo4j (for entity/relationship topology) and the Vector DB (for semantic chunking and indexing).
    *   The RAG query engine must execute natural-language forensic queries and provide deterministic source-context alongside a structured "chain-of-thought" reasoning process.
    *   It must reliably answer multi-modal targeted queries, specifically:
        1. "Show me chat records containing cryptocurrency wallet addresses."
        2. "List all communications with foreign phone numbers."
        3. "Identify the most frequently contacted entity."
        4. "Isolate and slice all incoming calls during a specific temporal window."

*   **C. Non-Functional Requirements:**
    *   **Latency targets:** Query pipeline (Vector search + Graph retrieval + LLM context compilation) must resolve within 5–7 seconds end-to-end. File parsing must stream asynchronously to prevent HTTP timeouts.
    *   **Accuracy:** Absolute semantic accuracy. The system must prevent hallucinations by enforcing strict, hard-filtered context boundaries inside the LLM prompt. Every response must contain a clear, immutable audit trail back to the raw source data in PostgreSQL.

*   **D. Constraints:**
    *   Demo Scale: Maximum demonstration file size is constrained to 300–400 MB to safely execute within boundary limits of cloud LLM context windows and chunking limits.
    *   Data Truncation/Privacy Layer: We are explicitly choosing a "Synthetic Data Prototype" approach. Since a public cloud LLM API is used, NO real-world seized PII data can ever touch this pipeline. We are creating a fully mapped synthetic forensic dataset that simulates the payload types for the proof-of-concept.
    
*   **E. Success Metrics:** Complete programmatic alignment between relational, graph, and vector stores, providing a verifiable query audit trail with zero un-grounded LLM output.

## 1. System Topology & Data Flow Lifecycle

### 1.1 Macro Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              REACT SPA (Frontend)                               │
│  ┌─────────────┐  ┌──────────────────┐  ┌────────────────────────────────────┐ │
│  │  Upload UI  │  │  Case Dashboard  │  │    Query Interface (Investigator)   │ │
│  │ (Chunked    │  │  (Job Status     │  │  [Hybrid Search Panel + LLM Output] │ │
│  │  Multipart) │  │   Polling)       │  │                                    │ │
│  └──────┬──────┘  └────────┬─────────┘  └────────────────┬───────────────────┘ │
└─────────┼──────────────────┼──────────────────────────────┼─────────────────────┘
          │ POST /api/v1/    │ GET /api/v1/jobs/{id}/status │ POST /api/v1/query
          │ extract          │                              │
          ▼                  ▼                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       FastAPI ASGI Application Layer                            │
│                      (Python 3.11+ | Uvicorn Workers)                           │
│                                                                                 │
│  ┌──────────────────┐  ┌────────────────────┐  ┌────────────────────────────┐  │
│  │  Ingest Router   │  │  Job Status Router  │  │      Query Router          │  │
│  │  /api/v1/extract │  │  /api/v1/jobs/{id} │  │  /api/v1/query             │  │
│  │                  │  │                    │  │                            │  │
│  │  1. Stream to    │  │  → Redis job hash  │  │  1. Classify query intent  │  │
│  │     MinIO/S3     │  │    lookup          │  │  2. Route to retrieval     │  │
│  │  2. Register job │  │                    │  │     engine                 │  │
│  │     in Redis     │  │                    │  │  3. Compile LLM context    │  │
│  │  3. Enqueue      │  │                    │  │  4. Stream LLM response    │  │
│  │     Celery task  │  │                    │  │                            │  │
│  └──────┬───────────┘  └────────────────────┘  └────────────────────────────┘  │
│         │                                                                       │
│  ┌──────▼────────────────────────────────────────────────────────────────────┐  │
│  │                    OpenTelemetry Instrumentation Middleware                │  │
│  │       (Trace Context Propagation · Span Injection · Metric Emission)      │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└─────────┬───────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Redis Cluster (v7.x)                                    │
│                                                                                 │
│  ┌─────────────────────────────┐    ┌──────────────────────────────────────┐   │
│  │     Celery Broker           │    │           Application Cache           │   │
│  │  (Task Queue: FIFO)         │    │                                      │   │
│  │                             │    │  ·  Job status hashes                │   │
│  │  Queues:                    │    │     KEY: job:{uuid}                  │   │
│  │  · ingestion.high           │    │     TTL: 86400s                      │   │
│  │  · ingestion.bulk           │    │  ·  Query result cache               │   │
│  │  · reindex.semantic         │    │     KEY: qcache:{sha256(query)}      │   │
│  │                             │    │     TTL: 300s                        │   │
│  │  Result Backend:            │    │  ·  Rate-limit counters              │   │
│  │  · Redis (Celery results)   │    │     KEY: ratelimit:{user_id}         │   │
│  └─────────────────────────────┘    └──────────────────────────────────────┘   │
└─────────┬───────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       Celery Worker Fleet (Async Task Execution)                │
│                                                                                 │
│  Worker Pool A (ingestion.high — concurrency=4, prefetch=1)                    │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  Task: parse_ufdr_archive                                                 │  │
│  │  ├── Phase 1: Decompress & Validate UFDR (XML manifest + checksum)        │  │
│  │  ├── Phase 2: Extract entities → structured dicts (Chat, Call, Contact)   │  │
│  │  ├── Phase 3: Bulk write to PostgreSQL (COPY protocol)                    │  │
│  │  ├── Phase 4: Graph mutation to Neo4j (MERGE + relationship binding)       │  │
│  │  └── Phase 5: Semantic chunking + embedding → Vector DB index             │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
│  Worker Pool B (reindex.semantic — concurrency=2, prefetch=1)                  │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  Task: reindex_semantic_chunk                                              │  │
│  │  └── Re-embeds failed or delta-updated chunks                             │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└────────┬──────────────────────┬──────────────────────────┬──────────────────────┘
         │                      │                          │
         ▼                      ▼                          ▼
┌─────────────────┐  ┌────────────────────┐  ┌──────────────────────────────────┐
│  PostgreSQL 16  │  │     Neo4j 5.x      │  │    Qdrant / pgvector             │
│  (Primary RDB)  │  │   (Graph Engine)   │  │    (Vector Index, HNSW+PQ)       │
│                 │  │                    │  │                                  │
│  · Messages     │  │  · Device nodes    │  │  · Collection: forensic_chunks   │
│  · CallRecords  │  │  · Contact nodes   │  │  · dim: 1536 (text-embed-3-s)    │
│  · Contacts     │  │  · Message nodes   │  │  · metric: Cosine                │
│  · IngestionLog │  │  · SENT_TO edges   │  │  · payload: chunk_meta           │
│                 │  │  · USES_DEVICE     │  │                                  │
└────────┬────────┘  └──────────┬─────────┘  └───────────────┬──────────────────┘
         │                      │                             │
         └──────────────────────┴─────────────────────────────┘
                                │
                                ▼ (at query time)
                   ┌────────────────────────────┐
                   │     Retrieval Engine        │
                   │  (Hybrid: Graph + Semantic) │
                   └────────────┬───────────────┘
                                │
                                ▼
                   ┌────────────────────────────┐
                   │       LLM API Gateway       │
                   │   (cohere)                  │
                   │   Prompt: context + docs    │
                   │   Response: streamed JSON   │
                   └────────────────────────────┘
```

---

## 2. Architectural Decision Records (ADRs)

### ADR-001: Modular Monolith over Microservices

**Status:** Accepted

**Context:** The system is a PoC with a small team and defined data locality requirements.

**Decision:** All Celery workers share the same codebase and update all three datastores directly. No service mesh, no inter-service HTTP calls for the ingestion path.

**Consequences:** Simpler deployment (single Docker Compose / K8s Deployment), lower latency, easier debugging. Tradeoff: horizontal scaling of individual components requires extracting workers into separate deployable units — a straightforward refactor when scale demands it.

---

### ADR-002: Qdrant over pgvector for Vector Storage

**Status:** Accepted

**Context:** pgvector offers operational simplicity (same PostgreSQL instance), but its HNSW implementation lacks native payload filtering and quantization support in early versions.

**Decision:** Qdrant as a separate service. Native HNSW+INT8 quantization, payload indexes, and filtered ANN search are first-class features critical for case-scoped queries.

**Consequences:** Additional operational component. Mitigated by: Qdrant has a clean Docker image, minimal config, and a Python SDK with async support.

---

### ADR-003: `acks_late=True` for All Ingestion Tasks

**Status:** Accepted

**Context:** Default Celery behavior acknowledges tasks on receipt, meaning a worker crash loses the task permanently.

**Decision:** `acks_late=True` on all ingestion tasks. Task remains in queue until `task.acknowledge()` is called implicitly on successful return or explicit on permanent failure (after max_retries exhausted).

**Consequences:** At-least-once delivery. All write operations must be idempotent (enforced via ON CONFLICT DO NOTHING, MERGE, and upsert). This is a hard architectural constraint propagated to all storage writers.

---

### ADR-004: Query Latency Budget Allocation

**Status:** Accepted

```
Total Budget: 5000ms (P95 target, within stated 5-7s window)

Component                  │ Budget  │ Rationale
───────────────────────────┼─────────┼──────────────────────────────────
Query classification       │   50ms  │ Cached in Redis (hit rate > 80%)
Neo4j graph traversal      │  200ms  │ 2-3 hop, indexed nodes
Qdrant semantic search     │  100ms  │ HNSW IN8Q with payload filter
PostgreSQL hydration       │  100ms  │ PK lookup, small result set
Context compilation        │   20ms  │ In-process, pure Python
Network + serialization    │   30ms  │ Internal LAN between containers
LLM API synthesis          │ 4500ms  │ Dominant cost; first-token ~800ms
─────────────────────────────────────────────────────────────────────
TOTAL                      │ 5000ms  │

Streaming optimization: Use SSE/WebSocket to stream LLM tokens to client.
First token appears at ~800ms; investigator sees progressive output.
Perceived latency: < 1s to first meaningful content.
```

---

## 3. End-to-End Lifecycle of a 400MB UFDR File

```
T+0.000s  [CLIENT] React UI dispatches POST /api/v1/extract
          - Multipart form-data, streaming upload
          - Content-Type: multipart/form-data; boundary=...
          - Header: X-Extraction-Label: "Case-Synth-001"
          - OTel trace context header injected: traceparent: 00-{trace_id}-{span_id}-01

T+0.050s  [FASTAPI] HTTP worker receives first chunk
          - Initiates UploadFile.read() in async streaming loop
          - Writes chunks to /secure-store/{job_id}.ufdr (or S3 via aiobotocore)
          - NO parsing happens here — pure I/O offload

T+~8.0s   [FASTAPI] File fully written to secure storage (~400MB at ~50MB/s)
          - Computes SHA-256 checksum of written file (streaming hash)
          - INSERT INTO ingestion_jobs (id, status, file_path, checksum, created_at)
            VALUES (:job_id, 'QUEUED', :path, :sha256, NOW())
          - PUBLISH task to Redis: celery.send_task('tasks.parse_ufdr', args=[job_id])
          - RETURN HTTP 202 Accepted: {"job_id": "...", "status_url": "/api/v1/jobs/{id}/status"}

T+~8.1s   [REDIS] Task message lands in celery:parse queue
          - Task payload: {"job_id": "...", "file_path": "...", "trace_context": {...}}

T+~8.2s   [CELERY WORKER — Phase 1: PARSE]
          - Worker prefetch_multiplier=1 (prevents HOL blocking)
          - task_parse_ufdr(job_id) dequeued by first available worker
          - UPDATE ingestion_jobs SET status='PARSING', phase='decompress' WHERE id=:job_id
          - HSET job:{job_id}:status '{"phase":"PARSING","pct":5}'  (Redis)
          - Decompress ZIP archive (Python zipfile streaming, no full RAM load)
          - Parse XML manifest (lxml iterparse — SAX-style, memory-efficient)
          - Build intermediate Python dataclass list:
              parsed_data = {
                  "messages": [...],        # ~50k–200k rows
                  "call_records": [...],    # ~5k–20k rows
                  "contacts": [...],        # ~1k–5k rows
                  "media_meta": [...],
              }
          - HSET job:{job_id}:status '{"phase":"PARSED","pct":25}'
          - Chain: task_persist_sql.si(job_id).delay()

T+~30s    [CELERY WORKER — Phase 2: RELATIONAL PERSIST]
          - task_persist_sql(job_id) picks up parsed data (passed via Redis or re-loaded from temp file)
          - Opens asyncpg connection pool (or psycopg3 pipeline mode)
          - Uses PostgreSQL COPY protocol for bulk inserts:
              COPY messages (id, extraction_id, sender_id, ...) FROM STDIN (FORMAT BINARY)
          - Transaction boundary: single COPY per table inside BEGIN/COMMIT
          - Isolation: READ COMMITTED (default) — acceptable; no concurrent reader-writer conflict on fresh insert
          - UPDATE ingestion_jobs SET status='SQL_DONE', phase='relational' WHERE id=:job_id
          - HSET job:{job_id}:status '{"phase":"SQL_PERSIST","pct":50}'
          - Chain: task_build_graph.si(job_id).delay()

T+~55s    [CELERY WORKER — Phase 3: GRAPH BUILD]
          - task_build_graph(job_id) loads contact + message edges from PG (re-query, not re-parse)
          - Batches Neo4j writes in transactions of 500 nodes per UNWIND
          - Uses MERGE (idempotent, prevents duplicates on retry)
          - Relationship creation with edge properties (timestamp, platform, weight)
          - UPDATE ingestion_jobs SET status='GRAPH_DONE' WHERE id=:job_id
          - HSET job:{job_id}:status '{"phase":"GRAPH_BUILD","pct":75}'
          - Chain: task_embed_and_index.si(job_id).delay()

T+~90s    [CELERY WORKER — Phase 4: EMBED & INDEX]
          - task_embed_and_index(job_id) queries PG for message text
          - Chunks text (512 tokens, 64-token overlap, sliding window)
          - Calls embedding API in batches of 100 chunks
          - Upserts to Vector DB with full metadata payload
          - UPDATE ingestion_jobs SET status='COMPLETE', completed_at=NOW() WHERE id=:job_id
          - HSET job:{job_id}:status '{"phase":"COMPLETE","pct":100}' EX 3600
          - PUBLISH event to Redis pub/sub: "job_complete:{job_id}"

T+~90s    [REACT UI — Polling /api/v1/jobs/{id}/status]
          - Polling interval: 3s via TanStack Query with exponential backoff
          - On COMPLETE: navigation to Investigator Dashboard
          - Alternatively: WebSocket subscription to "job_complete" channel
```

---

## 4. Consistency Model: Outbox Pattern vs. Saga vs. Modular Monolith

**Assessment and Recommendation:**

Given the architecture (a single-process modular monolith where all four Celery task phases run as chained tasks in the same codebase, with shared library access to all three datastores), **neither the full Transactional Outbox Pattern nor the Saga Pattern is strictly necessary.** Here is the reasoning and the pragmatic alternative:

| Pattern | Applicable When | Verdict for UFDR |
|---|---|---|
| **Transactional Outbox** | Microservices where the producing service's DB and the message broker must stay in sync atomically (dual-write problem) | **Not needed.** FastAPI writes the job row and Celery task publish is a fire-and-forget with Redis ACK. A failure between the two is recoverable by re-checking `QUEUED` jobs on startup. |
| **Saga (Choreography)** | Long-running cross-service transactions that must rollback via compensating transactions | **Not needed.** Celery's chain is deterministic and co-located. Celery itself provides the saga-like `link_error` callback chain. |
| **Modular Monolith + Task Chain + Idempotent Writes** | Single codebase, shared DB connections, deterministic phases | **Correct choice here.** |

**Eventual Consistency Guarantee in the Monolith:**

```python
# Every Celery task is written to be IDEMPOTENT and CHECKPOINTED:

@app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=30,
    acks_late=True,           # ACK only after successful execution
    reject_on_worker_lost=True  # Re-queue if worker dies mid-execution
)
def task_persist_sql(self, job_id: str):
    """
    Idempotency: Check if already persisted before bulk insert.
    Phase checkpoint: job status column acts as a phase lock.
    """
    job = db.query("SELECT status FROM ingestion_jobs WHERE id = %s", [job_id])
    if job.status in ('SQL_DONE', 'GRAPH_DONE', 'COMPLETE'):
        logger.info("Phase already complete, skipping.", job_id=job_id)
        return  # Safe no-op on retry

    with db.transaction(isolation_level="READ COMMITTED"):
        # Bulk COPY — idempotent because we DELETE + INSERT or use ON CONFLICT DO NOTHING
        db.execute("DELETE FROM messages WHERE extraction_id = %s", [job_id])
        db.copy_from_stdin(messages_csv_stream, table="messages")
        db.execute(
            "UPDATE ingestion_jobs SET status='SQL_DONE' WHERE id = %s", [job_id]
        )
    # Status commit is INSIDE the same transaction — atomicity guaranteed
```

**The `acks_late=True` + `reject_on_worker_lost=True` combination is the crucial consistency lever**: the task message stays in Redis until the worker commits its own ACK, which only happens after the transaction commits. A crashed worker releases the task back to the queue. Combined with idempotent, checkpointed phases (guarded by the `ingestion_jobs.status` column), the system achieves strong eventual consistency without any distributed transaction coordinator.

---

## Section 5 — Database Schema & Index Specifications

### 5.1 PostgreSQL Relational Schema

```sql
-- ============================================================
-- SCHEMA: forensics
-- Engine: PostgreSQL 16, extensions: pgcrypto, pg_partman
-- ============================================================

CREATE SCHEMA IF NOT EXISTS forensics;

-- ─────────────────────────────────────────────────────────────
-- TABLE: ingestion_jobs
-- Source of truth for pipeline state machine
-- ─────────────────────────────────────────────────────────────
CREATE TABLE forensics.ingestion_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    label               TEXT NOT NULL,
    file_path           TEXT NOT NULL,                        -- Secure storage path
    file_checksum       CHAR(64) NOT NULL,                    -- SHA-256
    file_size_bytes     BIGINT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'QUEUED'        -- QUEUED | PARSING | SQL_DONE | GRAPH_DONE | COMPLETE | FAILED
                        CHECK (status IN ('QUEUED','PARSING','SQL_DONE','GRAPH_DONE','EMBED_DONE','COMPLETE','FAILED')),
    phase_detail        JSONB,                                -- {"current": "decompress", "pct": 12, "error": null}
    worker_id           TEXT,                                 -- Celery worker hostname
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    error_message       TEXT
);

CREATE INDEX idx_jobs_status ON forensics.ingestion_jobs (status);
CREATE INDEX idx_jobs_created ON forensics.ingestion_jobs (created_at DESC);


-- ─────────────────────────────────────────────────────────────
-- TABLE: contacts
-- ─────────────────────────────────────────────────────────────
CREATE TABLE forensics.contacts (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    extraction_id       UUID NOT NULL REFERENCES forensics.ingestion_jobs(id) ON DELETE CASCADE,
    raw_contact_id      TEXT NOT NULL,                        -- Original ID from UFDR file
    display_name        TEXT,
    phone_numbers       TEXT[],                               -- Array; a contact may have N numbers
    email_addresses     TEXT[],
    organization        TEXT,
    source_app          TEXT,                                 -- e.g., 'com.android.contacts'
    raw_payload         JSONB,                                -- Full original object for audit
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (extraction_id, raw_contact_id)
);

-- Composite index for extraction-scoped lookups
CREATE INDEX idx_contacts_extraction ON forensics.contacts (extraction_id);
-- GIN index on array fields for containment queries: WHERE phone_numbers @> '{+1234567890}'
CREATE INDEX idx_contacts_phones_gin ON forensics.contacts USING GIN (phone_numbers);
CREATE INDEX idx_contacts_emails_gin ON forensics.contacts USING GIN (email_addresses);
-- Full-text on display name
CREATE INDEX idx_contacts_name_fts ON forensics.contacts USING GIN (to_tsvector('english', coalesce(display_name, '')));


-- ─────────────────────────────────────────────────────────────
-- TABLE: messages
-- Partitioned by extraction_id for query isolation
-- ─────────────────────────────────────────────────────────────
CREATE TABLE forensics.messages (
    id                  UUID NOT NULL DEFAULT gen_random_uuid(),
    extraction_id       UUID NOT NULL REFERENCES forensics.ingestion_jobs(id) ON DELETE CASCADE,
    raw_message_id      TEXT NOT NULL,
    platform            TEXT NOT NULL,                        -- 'whatsapp' | 'sms' | 'telegram' | 'imessage'
    sender_phone        TEXT,
    recipient_phones    TEXT[],
    direction           TEXT CHECK (direction IN ('INCOMING','OUTGOING','UNKNOWN')),
    content_text        TEXT,
    content_type        TEXT DEFAULT 'text',                  -- 'text' | 'media' | 'location' | 'contact_card'
    media_ref           TEXT,                                 -- Path or hash of media item
    sent_at             TIMESTAMPTZ,
    delivered_at        TIMESTAMPTZ,
    read_at             TIMESTAMPTZ,
    is_deleted          BOOLEAN NOT NULL DEFAULT FALSE,       -- Forensic artifact: deleted but recovered
    thread_id           TEXT,                                 -- Group thread or conversation ID
    raw_payload         JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, extraction_id),                          -- Required for declarative partitioning
    UNIQUE (extraction_id, raw_message_id)
) PARTITION BY LIST (extraction_id);

-- Partition template — created dynamically per ingestion job by the Celery worker:
-- CREATE TABLE forensics.messages_<job_id_short>
--   PARTITION OF forensics.messages FOR VALUES IN ('<job_id>');

-- Indexes defined on parent (inherited by all partitions automatically in PG16):
CREATE INDEX idx_messages_extraction  ON forensics.messages (extraction_id);
CREATE INDEX idx_messages_sender      ON forensics.messages (sender_phone);
CREATE INDEX idx_messages_sent_at     ON forensics.messages (sent_at DESC);
CREATE INDEX idx_messages_platform    ON forensics.messages (platform);
CREATE INDEX idx_messages_thread      ON forensics.messages (extraction_id, thread_id);
-- Full-text content search
CREATE INDEX idx_messages_content_fts ON forensics.messages
    USING GIN (to_tsvector('english', coalesce(content_text, '')));
-- JSONB index for raw payload field access
CREATE INDEX idx_messages_raw_payload ON forensics.messages USING GIN (raw_payload jsonb_path_ops);


-- ─────────────────────────────────────────────────────────────
-- TABLE: call_records
-- ─────────────────────────────────────────────────────────────
CREATE TABLE forensics.call_records (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    extraction_id       UUID NOT NULL REFERENCES forensics.ingestion_jobs(id) ON DELETE CASCADE,
    raw_call_id         TEXT NOT NULL,
    caller_phone        TEXT,
    callee_phone        TEXT,
    direction           TEXT CHECK (direction IN ('INCOMING','OUTGOING','MISSED','BLOCKED','UNKNOWN')),
    call_type           TEXT DEFAULT 'VOICE',                 -- 'VOICE' | 'VIDEO' | 'VOIP'
    platform            TEXT,
    started_at          TIMESTAMPTZ,
    duration_seconds    INTEGER,
    is_international    BOOLEAN GENERATED ALWAYS AS (
                            callee_phone LIKE '+%' AND
                            LEFT(callee_phone, 3) != '+1 '   -- Simplified; use libphonenumber in app layer
                        ) STORED,
    raw_payload         JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (extraction_id, raw_call_id)
);

CREATE INDEX idx_calls_extraction   ON forensics.call_records (extraction_id);
CREATE INDEX idx_calls_caller       ON forensics.call_records (caller_phone);
CREATE INDEX idx_calls_callee       ON forensics.call_records (callee_phone);
CREATE INDEX idx_calls_started_at   ON forensics.call_records (started_at DESC);
CREATE INDEX idx_calls_international ON forensics.call_records (is_international)
    WHERE is_international = TRUE;
CREATE INDEX idx_calls_direction    ON forensics.call_records (extraction_id, direction);
```

**Isolation Level Analysis — Heavy Bulk Inserts:**

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                  PostgreSQL Isolation Level Decision Matrix                          │
│                  During Celery Worker Bulk Insert Operations                        │
├────────────────────────┬───────────────────────────┬────────────────────────────────┤
│ Scenario               │ READ COMMITTED (default)  │ REPEATABLE READ                │
├────────────────────────┼───────────────────────────┼────────────────────────────────┤
│ Worker inserts 200k    │ RECOMMENDED. Each stmt    │ Unnecessary overhead. The       │
│ messages in a single   │ sees latest committed     │ transaction snapshot is taken   │
│ COPY command           │ data. COPY is atomic at   │ at BEGIN. For pure inserts of  │
│                        │ the statement level.      │ new rows, no phantom read risk. │
├────────────────────────┼───────────────────────────┼────────────────────────────────┤
│ Status UPDATE inside   │ Safe. The UPDATE reads    │ Could cause serialization       │
│ same transaction as    │ its own in-progress rows  │ errors if another session       │
│ COPY (job checkpoint)  │ correctly.               │ updated the job row between     │
│                        │                           │ BEGIN and UPDATE here.          │
├────────────────────────┼───────────────────────────┼────────────────────────────────┤
│ Concurrent API read    │ Reader sees only fully    │ Identical behavior for this     │
│ while worker inserts   │ committed rows. No dirty  │ use case; adds no benefit over  │
│ (investigator query)   │ reads. Acceptable.        │ READ COMMITTED here.            │
├────────────────────────┼───────────────────────────┼────────────────────────────────┤
│ Re-check status before │ ADVISORY LOCK recommended:│ Would prevent phantom reads     │
│ retry (idempotency     │ SELECT pg_advisory_xact_  │ across the status check and     │
│ guard in worker)       │ lock(hashtext(job_id))    │ insert but at serialization     │
│                        │ prevents dual execution.  │ retry cost. Use advisory lock   │
│                        │                           │ instead.                        │
└────────────────────────┴───────────────────────────┴────────────────────────────────┘

DECISION: Use READ COMMITTED + pg_advisory_xact_lock(hashtext(job_id)) on all 
          write-phase tasks. This is simpler, lower overhead, and eliminates 
          the dual-execution race condition without serialization overhead.
```

---

### 5.2 Neo4j Graph Schema

```cypher
// ============================================================
// NEO4J GRAPH SCHEMA v1.0
// Nodes, constraints, indexes, and relationship definitions
// ============================================================

// ─────────────────────────────────────────────────────────────
// NODE: Device
// Represents a physical mobile device from the extraction
// ─────────────────────────────────────────────────────────────
CREATE CONSTRAINT device_imei_unique IF NOT EXISTS
    FOR (d:Device) REQUIRE d.imei IS UNIQUE;

CREATE INDEX device_extraction_idx IF NOT EXISTS
    FOR (d:Device) ON (d.extraction_id);

// Properties on :Device node:
// {
//   imei:           "355490069843657",
//   model:          "Samsung Galaxy S22",
//   os:             "Android 14",
//   os_version:     "14.0.0",
//   extraction_id:  "uuid-of-ingestion-job",
//   extracted_at:   datetime("2025-01-15T10:00:00Z"),
//   owner_phone:    "+14155550100"
// }


// ─────────────────────────────────────────────────────────────
// NODE: PhoneNumber
// Normalized phone number — the central connecting entity
// Kept separate from Contact to handle unresolved numbers
// ─────────────────────────────────────────────────────────────
CREATE CONSTRAINT phone_number_unique IF NOT EXISTS
    FOR (p:PhoneNumber) REQUIRE p.e164 IS UNIQUE;

// Properties on :PhoneNumber node:
// {
//   e164:           "+14155550199",     // E.164 normalized
//   country_code:   "1",
//   national:       "4155550199",
//   is_international: false,
//   carrier:        "Verizon",          // optional enrichment
// }


// ─────────────────────────────────────────────────────────────
// NODE: Contact
// Named entity from the device address book
// ─────────────────────────────────────────────────────────────
CREATE CONSTRAINT contact_id_unique IF NOT EXISTS
    FOR (c:Contact) REQUIRE c.contact_id IS UNIQUE;

CREATE FULLTEXT INDEX contact_name_ft IF NOT EXISTS
    FOR (c:Contact) ON EACH [c.display_name, c.organization];

// Properties on :Contact node:
// {
//   contact_id:     "uuid-from-pg-contacts-table",
//   display_name:   "John Doe",
//   organization:   "Acme Corp",
//   extraction_id:  "uuid-of-ingestion-job"
// }


// ─────────────────────────────────────────────────────────────
// NODE: Message
// Individual message artifact
// ─────────────────────────────────────────────────────────────
CREATE CONSTRAINT message_id_unique IF NOT EXISTS
    FOR (m:Message) REQUIRE m.message_id IS UNIQUE;

CREATE INDEX message_sent_at_idx IF NOT EXISTS
    FOR (m:Message) ON (m.sent_at);

CREATE INDEX message_platform_idx IF NOT EXISTS
    FOR (m:Message) ON (m.platform);

// Properties on :Message node:
// {
//   message_id:     "uuid-from-pg-messages-table",
//   platform:       "whatsapp",
//   content_text:   "Meet me at the usual spot",
//   content_type:   "text",
//   direction:      "OUTGOING",
//   sent_at:        datetime("2025-03-10T14:32:00Z"),
//   thread_id:      "thread-abc123",
//   is_deleted:     false,
//   extraction_id:  "uuid-of-ingestion-job"
// }


// ─────────────────────────────────────────────────────────────
// RELATIONSHIPS & THEIR PROPERTIES
// ─────────────────────────────────────────────────────────────

// (Device)-[:BELONGS_TO]->(Contact)
//   Properties: { since: datetime, confidence: float }
//   Meaning: Device is primarily associated with this contact/owner

// (Message)-[:SENT_FROM]->(Device)
//   Properties: { platform: "whatsapp", thread_id: "..." }

// (Message)-[:SENT_BY]->(PhoneNumber)
//   Properties: { at: datetime, platform: "whatsapp" }

// (Message)-[:RECEIVED_BY]->(PhoneNumber)
//   Properties: { at: datetime, delivery_status: "READ" }

// (PhoneNumber)-[:ASSIGNED_TO]->(Contact)
//   Properties: { label: "mobile", is_primary: bool }
//   NOTE: A PhoneNumber can be UNRESOLVED (no Contact node linked)

// (PhoneNumber)-[:CONTACTED { count: int, last_at: datetime, platforms: [str] }]->(PhoneNumber)
//   NOTE: Aggregated communication edge; updated via MERGE+SET increment
//   This is the KEY relationship for "most frequently contacted" queries


// ─────────────────────────────────────────────────────────────
// OPTIMAL CYPHER DATA MUTATION PATTERN
// Using UNWIND + MERGE to avoid graph fragmentation
// ─────────────────────────────────────────────────────────────
// Pattern: NEVER use individual CREATE; always MERGE on constraint fields.
// Batch via UNWIND for transaction efficiency.

// --- Upsert Devices (run once per ingestion) ---
UNWIND $devices AS dev
MERGE (d:Device {imei: dev.imei})
SET d += {
    model:          dev.model,
    os:             dev.os,
    os_version:     dev.os_version,
    extraction_id:  dev.extraction_id,
    extracted_at:   datetime(dev.extracted_at),
    owner_phone:    dev.owner_phone
};

// --- Upsert PhoneNumbers + Contacts + ASSIGNED_TO edge ---
UNWIND $contacts AS c
MERGE (ct:Contact {contact_id: c.contact_id})
SET ct += {display_name: c.display_name, organization: c.organization}
WITH ct, c
UNWIND c.phone_numbers AS phone_e164
MERGE (pn:PhoneNumber {e164: phone_e164})
ON CREATE SET pn.is_international = (left(phone_e164, 3) <> '+1-')
MERGE (pn)-[rel:ASSIGNED_TO]->(ct)
ON CREATE SET rel.label = 'mobile', rel.is_primary = (phone_e164 = c.phone_numbers[0]);

// --- Upsert Messages + Communication Edges (batch of 500) ---
UNWIND $messages AS msg
MERGE (m:Message {message_id: msg.message_id})
SET m += {
    platform:     msg.platform,
    content_text: msg.content_text,
    direction:    msg.direction,
    sent_at:      datetime(msg.sent_at),
    thread_id:    msg.thread_id,
    is_deleted:   msg.is_deleted,
    extraction_id: msg.extraction_id
}
WITH m, msg
MERGE (sender:PhoneNumber {e164: msg.sender_phone})
MERGE (m)-[:SENT_BY {at: datetime(msg.sent_at), platform: msg.platform}]->(sender)
WITH m, msg, sender
UNWIND msg.recipient_phones AS rphone
MERGE (recip:PhoneNumber {e164: rphone})
MERGE (m)-[:RECEIVED_BY {at: datetime(msg.sent_at)}]->(recip)
// Increment the aggregated CONTACTED edge (core analytics relationship)
MERGE (sender)-[comm:CONTACTED]->(recip)
ON CREATE SET comm.count = 1, comm.last_at = datetime(msg.sent_at), comm.platforms = [msg.platform]
ON MATCH SET
    comm.count   = comm.count + 1,
    comm.last_at = CASE WHEN datetime(msg.sent_at) > comm.last_at
                        THEN datetime(msg.sent_at) ELSE comm.last_at END,
    comm.platforms = CASE WHEN NOT msg.platform IN comm.platforms
                          THEN comm.platforms + msg.platform ELSE comm.platforms END;
```

**Why this avoids graph fragmentation:**

- **`MERGE` over `CREATE`**: prevents duplicate nodes for the same phone number appearing from different message records
- **`UNWIND` batching**: keeps Neo4j transaction size bounded (500 rows = ~1–2ms per batch write)
- **`ON CREATE SET` vs `ON MATCH SET`**: atomic conditional property initialization prevents race conditions on first-write
- **Aggregated `CONTACTED` edge**: instead of one relationship per message (graph explosion), a single edge carries count/last_at/platforms — dramatically reduces graph density for high-volume conversations

---

### 5.3 Vector Database Index Configuration

**Chosen Implementation: Qdrant (self-hosted or Qdrant Cloud)**
*(Pgvector is acceptable for PoC; Qdrant preferred for dedicated HNSW tuning)*

```python
# ─────────────────────────────────────────────────────────────
# COLLECTION CREATION — forensic_chunks
# ─────────────────────────────────────────────────────────────
from qdrant_client import QdrantClient
from qdrant_client.models import (
    VectorParams, Distance, HnswConfigDiff,
    OptimizersConfigDiff, QuantizationConfig,
    ProductQuantization, PqConfig, CompressionRatio
)

client = QdrantClient(url="http://qdrant:6333")

client.create_collection(
    collection_name="forensic_chunks",
    vectors_config=VectorParams(
        size=1536,              # OpenAI text-embedding-3-small dims
        # size=768,             # Alternative: sentence-transformers/all-mpnet-base-v2
        distance=Distance.COSINE,  # Normalized embeddings; cosine = dot product equivalent
        on_disk=True,           # Large forensic datasets; mmap from disk to manage RAM
    ),
    hnsw_config=HnswConfigDiff(
        m=16,                   # Number of bi-directional links per node
                                # Higher m = better recall, more memory
                                # m=16 is optimal for recall@10 > 95%
        ef_construct=128,       # Construction-time beam width
                                # Higher = better graph quality, slower index build
                                # 128 is sweet spot for PoC
        full_scan_threshold=10_000,  # Fallback to brute-force if collection < 10k vectors
        on_disk=False,          # Keep HNSW graph in RAM for low-latency traversal
    ),
    quantization_config=QuantizationConfig(
        product=ProductQuantization(
            compression=CompressionRatio.X16,  # 32-bit float -> 2-bit; ~16x size reduction
            always_ram=True,    # Keep quantized vectors in RAM; raw on disk
        )
    ),
    optimizers_config=OptimizersConfigDiff(
        indexing_threshold=20_000,   # Build HNSW index after 20k vectors
        memmap_threshold=50_000,     # Switch to mmap above 50k raw vectors
    ),
)
```

**Semantic Chunking Strategy:**

```python
# ─────────────────────────────────────────────────────────────
# CHUNKING STRATEGY FOR LONG CHAT THREADS
# ─────────────────────────────────────────────────────────────

from dataclasses import dataclass
from typing import List, Generator
import tiktoken

CHUNK_SIZE_TOKENS    = 512   # Max tokens per chunk
CHUNK_OVERLAP_TOKENS = 64    # Overlap for context continuity across chunk boundaries
ENCODING             = tiktoken.get_encoding("cl100k_base")


@dataclass
class ForensicChunk:
    text:           str
    token_count:    int
    # Metadata payload — stored alongside vector in Qdrant payload
    extraction_id:  str
    message_ids:    List[str]    # All message IDs whose content is in this chunk
    thread_id:      str
    platform:       str
    start_time:     str          # ISO8601 of earliest message in chunk
    end_time:       str          # ISO8601 of latest message in chunk
    chunk_index:    int          # Position within the thread
    total_chunks:   int          # Total chunks in this thread (set in second pass)
    is_deleted_content: bool     # True if any message in chunk was a forensic recovery
    sender_phones:  List[str]    # Distinct senders in this chunk


def chunk_thread(
    thread_messages: List[dict],  # Sorted ascending by sent_at
    extraction_id: str,
    thread_id: str,
    platform: str,
) -> Generator[ForensicChunk, None, None]:
    """
    Sliding window chunker for chat threads.

    Format of each line before tokenization:
        [TIMESTAMP] SENDER_PHONE: message text
        e.g.: [2025-03-10T14:32Z] +14155550100: Meet me at the corner.

    This format preserves temporal and sender context inside the chunk embedding,
    which massively improves semantic retrieval for investigator queries like
    "what did number X say about Y around March 10th."
    """
    buffer_tokens: List[int]  = []
    buffer_mids:   List[str]  = []
    buffer_times:  List[str]  = []
    buffer_phones: List[str]  = []

    chunk_index = 0

    for msg in thread_messages:
        formatted_line = (
            f"[{msg['sent_at']}] {msg['sender_phone'] or 'UNKNOWN'}: "
            f"{msg['content_text'] or '[media]'}\n"
        )
        line_tokens = ENCODING.encode(formatted_line)

        # If adding this message would exceed chunk size, emit current buffer
        if len(buffer_tokens) + len(line_tokens) > CHUNK_SIZE_TOKENS and buffer_tokens:
            yield _build_chunk(
                buffer_tokens, buffer_mids, buffer_times, buffer_phones,
                extraction_id, thread_id, platform, chunk_index
            )
            chunk_index += 1

            # Retain overlap: keep last CHUNK_OVERLAP_TOKENS tokens in buffer
            # Find which messages contributed those tokens (walk backward)
            overlap_tokens, overlap_mids, overlap_times, overlap_phones = \
                _compute_overlap(buffer_tokens, buffer_mids, buffer_times, buffer_phones)

            buffer_tokens = overlap_tokens
            buffer_mids   = overlap_mids
            buffer_times  = overlap_times
            buffer_phones = overlap_phones

        buffer_tokens.extend(line_tokens)
        buffer_mids.append(msg['id'])
        buffer_times.append(msg['sent_at'])
        if msg['sender_phone'] not in buffer_phones:
            buffer_phones.append(msg['sender_phone'])

    # Emit final chunk
    if buffer_tokens:
        yield _build_chunk(
            buffer_tokens, buffer_mids, buffer_times, buffer_phones,
            extraction_id, thread_id, platform, chunk_index
        )


# ─────────────────────────────────────────────────────────────
# METADATA / CALL RECORD CHUNKING
# ─────────────────────────────────────────────────────────────
# Call records are structured, not freeform text.
# Strategy: embed a rich text representation of a WINDOW of N records:
#   "Call log [2025-01-01 to 2025-01-07]: 
#    OUTGOING to +447911123456 (London, UK) on 2025-01-03 duration 342s via VOICE.
#    INCOMING from +447911123456 on 2025-01-05 duration 120s.
#    MISSED from +33612345678 (Paris, France) on 2025-01-06."
# Window: 20 call records per chunk — keeps semantic context while remaining compact.

def chunk_call_records(
    calls: List[dict],            # Sorted ascending by started_at
    extraction_id: str,
    window_size: int = 20,
) -> Generator[ForensicChunk, None, None]:
    for i in range(0, len(calls), window_size):
        window = calls[i : i + window_size]
        lines = []
        for c in window:
            direction = c['direction']
            other     = c['callee_phone'] if direction == 'OUTGOING' else c['caller_phone']
            intl      = "INTERNATIONAL" if c.get('is_international') else "DOMESTIC"
            lines.append(
                f"{direction} call {'to' if direction=='OUTGOING' else 'from'} "
                f"{other or 'UNKNOWN'} [{intl}] on {c['started_at']} "
                f"duration {c['duration_seconds'] or '?'}s via {c['call_type']}."
            )
        text = f"Call Records [{window[0]['started_at']} to {window[-1]['started_at']}]:\n"
        text += "\n".join(lines)
        yield ForensicChunk(
            text           = text,
            token_count    = len(ENCODING.encode(text)),
            extraction_id  = extraction_id,
            message_ids    = [c['id'] for c in window],
            thread_id      = "call_log",
            platform       = "telephony",
            start_time     = window[0]['started_at'],
            end_time       = window[-1]['started_at'],
            chunk_index    = i // window_size,
            total_chunks   = -1,    # Set in second pass
            is_deleted_content = False,
            sender_phones  = list({c['caller_phone'] for c in window if c['caller_phone']}),
        )
```

**Qdrant Payload Schema (Metadata stored per vector):**

```json
{
  "extraction_id":      "3f7a1b2c-...",
  "chunk_index":        4,
  "total_chunks":       12,
  "thread_id":          "whatsapp-group-abc123",
  "platform":           "whatsapp",
  "start_time":         "2025-03-10T14:00:00Z",
  "end_time":           "2025-03-10T14:45:00Z",
  "sender_phones":      ["+14155550100", "+14155550199"],
  "message_ids":        ["uuid-1", "uuid-2", "..."],
  "is_deleted_content": false,
  "content_type":       "chat",
  "token_count":        487
}
```

---

## 6. API Contract & Payload Specifications

### 6.1 `POST /api/v1/extract` — File Ingestion

**Request:**
```
POST /api/v1/extract
Content-Type: multipart/form-data
X-Case-ID: {uuid}
X-Analyst-ID: {string}
Authorization: Bearer {token}

Body:
  file: <binary UFDR file>
  metadata: {"device_make": "Apple", "examiner_notes": "..."}
```

**Response 202:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "case_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "status": "QUEUED",
  "poll_url": "/api/v1/jobs/550e8400-e29b-41d4-a716-446655440000/status",
  "estimated_duration_secs": 90,
  "submitted_at": "2024-01-15T14:32:01.000Z"
}
```

---

### 6.2 `POST /api/v1/query` — Hybrid Investigator Query

#### Request Schema

```json
{
  "query": "Show me chat records mentioning crypto wallet addresses from foreign contacts",
  "case_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "retrieval_config": {
    "max_semantic_chunks": 10,
    "max_graph_hops": 3,
    "time_filter": {
      "from": "2023-01-01T00:00:00Z",
      "to":   "2023-12-31T23:59:59Z"
    },
    "source_type_filter": ["chat", "call"],
    "enable_thinking_trace": true
  },
  "llm_config": {
    "model": "claude-sonnet-4-20250514",
    "max_tokens": 2048,
    "stream": false
  },
  "request_id": "client-req-abc123",
  "analyst_id": "analyst-007"
}
```

#### Response Schema

```json
{
  "request_id": "client-req-abc123",
  "query":      "Show me chat records mentioning crypto wallet addresses from foreign contacts",
  "case_id":    "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "latency_ms": 4720,

  "answer": {
    "text": "Based on the extracted forensic data for this case, I identified 3 chat threads containing cryptocurrency wallet addresses, all from contacts flagged as foreign (non-domestic) numbers...",
    "confidence": "high",
    "answer_basis": "hybrid_retrieval"
  },

  "thinking_trace": {
    "query_classification": {
      "primary_type": "semantic",
      "secondary_type": "structural",
      "routing_reason": "Query contains semantic intent (crypto addresses) AND structural filter (foreign contacts). Dual-path retrieval activated.",
      "detected_entities": ["crypto_wallet_address", "foreign_contact"],
      "detected_operators": ["filter_by_contact_attribute"]
    },
    "retrieval_plan": [
      {
        "step": 1,
        "executor": "neo4j",
        "operation": "graph_traversal",
        "query_issued": "MATCH (c:Contact {case_id: $case_id, is_foreign: true})-[:SENT]->(m:Message) WHERE 'contains_crypto_addr' IN m.semantic_flags RETURN c, m LIMIT 100",
        "duration_ms": 87,
        "records_returned": 12
      },
      {
        "step": 2,
        "executor": "qdrant",
        "operation": "filtered_ann_search",
        "query_vector_preview": "[0.021, -0.347, ..., 0.118]",
        "filter_applied": {"case_id": "7c9e...", "source_type": "chat"},
        "top_k": 10,
        "score_threshold": 0.72,
        "duration_ms": 43,
        "chunks_returned": 8
      },
      {
        "step": 3,
        "executor": "postgresql",
        "operation": "entity_hydration",
        "purpose": "Fetch full message bodies for graph-matched message IDs",
        "duration_ms": 29,
        "records_returned": 12
      },
      {
        "step": 4,
        "executor": "llm_api",
        "operation": "synthesis",
        "model": "claude-sonnet-4-20250514",
        "prompt_tokens": 3847,
        "completion_tokens": 412,
        "duration_ms": 4561
      }
    ],
    "context_compilation_strategy": "graph_primary_semantic_augment",
    "deduplication_applied": true,
    "total_context_tokens": 3847
  },

  "retrieved_sources": [
    {
      "source_id": "src-001",
      "source_type": "chat",
      "retrieval_method": "graph_traversal",
      "relevance_score": null,
      "message_id": "a1b2c3d4-...",
      "thread_id": "thread-882",
      "platform": "whatsapp",
      "sender": "+44 7911 123456",
      "recipient": "+1 555 0100",
      "sent_at": "2023-07-14T09:21:00Z",
      "body": "Send the payment to bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq",
      "is_deleted": false,
      "contact_display_name": "Marcus T.",
      "contact_is_foreign": true,
      "contact_country": "GB"
    },
    {
      "source_id": "src-002",
      "source_type": "chat",
      "retrieval_method": "semantic_search",
      "relevance_score": 0.891,
      "chunk_id": "ck-9f3a...",
      "chunk_text": "[WhatsApp Thread · +44 7911 123456 · 2023-07-14]\nMarcus T.: ...",
      "message_ids_in_chunk": ["a1b2c3d4-...", "a1b2c3d5-..."],
      "time_range": {"from": "2023-07-14T09:18:00Z", "to": "2023-07-14T09:25:00Z"}
    }
  ],

  "anti_hallucination_controls": {
    "grounding_strategy": "source_citation_forced",
    "llm_instruction": "Answer ONLY from the RETRIEVED_DOCUMENTS block. If information is not present, say 'Not found in extracted data.' Do not infer or fabricate.",
    "sources_passed_to_llm": 18,
    "answer_cites_sources": true
  },

  "trace_context": {
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "span_id":  "00f067aa0ba902b7",
    "otel_baggage": {"case_id": "7c9e...", "analyst_id": "analyst-007"}
  }
}
```

---

### 6.3 Query Routing Logic (Retrieval Engine)

```
Query Classification Matrix:

Query Pattern                            │ Primary Executor │ Secondary Executor
─────────────────────────────────────────┼──────────────────┼────────────────────
"most frequently contacted number"       │ Neo4j (degree)   │ PostgreSQL (count)
"calls to foreign numbers in July"       │ PostgreSQL (SQL)  │ Neo4j (is_foreign)
"messages mentioning crypto addresses"   │ Vector (semantic) │ PostgreSQL (hydrate)
"who did X contact after event Y"        │ Neo4j (traverse)  │ PostgreSQL (time)
"summarize communication pattern of X"  │ Neo4j + Vector   │ LLM synthesis
"show deleted messages from thread Z"    │ PostgreSQL (flag) │ None

Classification via lightweight intent classifier (zero-shot prompt to LLM):
  - Extracts: entities, temporal filters, relationship operators, semantic keywords
  - Returns: {"structural": 0.8, "semantic": 0.6, "routing": "dual_path"}
  - Cached in Redis for 60s (identical query within session)
```

---

## Section 7 — Failure Modes & Observability Matrix

### 7.1 Failure Mode Analysis

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                        FAILURE MODE & EFFECTS ANALYSIS (FMEA)                      ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                      ║
║  FAILURE A: Celery Worker Crashes Mid-Parse                                          ║
║  ─────────────────────────────────────────────────────────────────────────────────  ║
║                                                                                      ║
║  Trigger Scenarios:                                                                  ║
║    - OOMKill during XML parse of large media manifest (~2–4GB RSS at peak)           ║
║    - Unhandled exception in custom UFDR parser (malformed node)                      ║
║    - Container eviction (Kubernetes preemption in spot/preemptible config)           ║
║    - Network partition between worker and Redis (ACK never sent)                     ║
║                                                                                      ║
║  Immediate Effect:                                                                   ║
║    - Worker process dies; OS kernel sends SIGKILL                                    ║
║    - Redis message was NOT ACK'd (acks_late=True config critical here)               ║
║    - Message visibility timeout expires (default: 1 hour for Celery)                 ║
║    - Redis re-queues the task automatically                                          ║
║                                                                                      ║
║  Recovery Mechanism:                                                                 ║
║    1. [AUTO] Redis re-delivers task to next available worker (within visibility_     ║
║       timeout). No human intervention required.                                      ║
║    2. [IDEMPOTENCY GUARD] Worker calls pg_advisory_xact_lock(hashtext(job_id))      ║
║       before any write. If prior partial data exists (extraction_id rows in          ║
║       messages table), the DELETE + re-INSERT pattern via COPY is triggered.         ║
║    3. [CHECKPOINT] Worker reads ingestion_jobs.status on startup:                   ║
║       - If status = 'QUEUED' or 'PARSING': restart from Phase 1                    ║
║       - If status = 'SQL_DONE': skip to Phase 3 (graph build)                      ║
║       - If status = 'GRAPH_DONE': skip to Phase 4 (embed)                          ║
║    4. [MAX_RETRIES=3] After 3 failed attempts: task goes to Dead Letter Queue       ║
║       (celery:dlq). Alert fires via OpenTelemetry metric:                           ║
║       forensic.ingestion.task.dlq_count (counter, extraction_id label)              ║
║    5. [HUMAN RECOVERY] Investigator sees FAILED status in UI with error_message.    ║
║       Admin can re-trigger via: POST /api/v1/jobs/{id}/retry                        ║
║                                                                                      ║
║  Observability Signals:                                                              ║
║    - OTel Metric: forensic.celery.task.failure (counter, task_name label)           ║
║    - OTel Metric: forensic.celery.task.retry_count (histogram)                      ║
║    - OTel Log: level=ERROR, job_id, phase, exception.type, exception.stacktrace     ║
║    - Alerting Rule: task_failure_rate > 0.1 over 5m -> PagerDuty/Slack             ║
║                                                                                      ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                      ║
║  FAILURE B: Vector DB Timeout During Query                                           ║
║  ─────────────────────────────────────────────────────────────────────────────────  ║
║                                                                                      ║
║  Trigger Scenarios:                                                                  ║
║    - Qdrant node under heavy concurrent indexing (embedding phase) and query         ║
║    - Network partition between FastAPI and Qdrant container                         ║
║    - HNSW index build blocking read queries (indexing_threshold hit)                ║
║    - Cold start: index not loaded into RAM (on_disk=True mmap slow first access)    ║
║                                                                                      ║
║  Immediate Effect:                                                                   ║
║    - Vector search asyncio.gather() branch raises TimeoutError after 2.0s           ║
║    - asyncio.wait_for(vector_task, timeout=2.0) catches exception                   ║
║                                                                                      ║
║  Recovery Mechanism (GRACEFUL DEGRADATION):                                          ║
║    1. HybridRetrievalEngine catches TimeoutError in the semantic branch             ║
║    2. Continues with structural results ONLY (Neo4j + PostgreSQL)                   ║
║    3. Sets response.status = "partial"                                              ║
║    4. Adds to thinking_process: "vector_search_degraded": true,                    ║
║       "vector_error": "Timeout after 2000ms"                                       ║
║    5. LLM prompt is modified: adds disclaimer "Semantic search was unavailable.     ║
║       Results are based on structural database queries only."                        ║
║    6. Response is still returned to investigator within SLA (structural-only         ║
║       query is ~300ms, well within 5-7s target)                                     ║
║                                                                                      ║
║  Fallback Chain:                                                                     ║
║    Vector DB Timeout                                                                 ║
║    --> Try PostgreSQL full-text search (tsvector GIN index) as fallback semantic    ║
║    --> If PG FTS also fails: return structural results with degradation notice       ║
║                                                                                      ║
║  Observability Signals:                                                              ║
║    - OTel Metric: forensic.vector_db.timeout_count (counter)                        ║
║    - OTel Metric: forensic.query.degraded_mode (counter, mode="no_semantic")        ║
║    - OTel Span: db.qdrant.search with status=ERROR, attribute: error.type=Timeout   ║
║    - Alerting Rule: vector_timeout_rate > 0.05 over 2m -> investigate Qdrant        ║
║                                                                                      ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                      ║
║  FAILURE C: LLM API Rate Limiting / Unavailability                                  ║
║  ─────────────────────────────────────────────────────────────────────────────────  ║
║                                                                                      ║
║  Trigger Scenarios:                                                                  ║
║    - HTTP 429 Too Many Requests from OpenAI (TPM/RPM quota exhausted)               ║
║    - HTTP 503 / 524 (Gateway Timeout) from LLM provider                             ║
║    - Network timeout >30s on LLM streaming connection                               ║
║                                                                                      ║
║  Immediate Effect:                                                                   ║
║    - LLM API call in HybridRetrievalEngine raises RateLimitError / APIError         ║
║                                                                                      ║
║  Recovery Mechanism:                                                                 ║
║    1. [RETRY WITH BACKOFF] openai library built-in exponential retry:               ║
║       max_retries=3, initial_delay=1s, max_delay=20s                                ║
║    2. [CIRCUIT BREAKER] After 5 consecutive 429s in 60s window:                    ║
║       circuit breaker OPENS. All subsequent LLM calls short-circuit immediately.   ║
║       Response returns status="partial" with raw citations but no LLM synthesis.   ║
║       Message to investigator: "AI synthesis temporarily unavailable. Raw          ║
║       forensic evidence blocks are shown below for manual review."                  ║
║    3. [FALLBACK RESPONSE] Return the structured citations + thinking_process         ║
║       to the investigator as-is. A skilled investigator can work from raw evidence. ║
║    4. [REDIS CACHE] SHA256(query + extraction_id) cache lookup runs BEFORE          ║
║       the LLM call. Cached responses bypass LLM entirely (TTL=300s). This is        ║
║       particularly useful for repeated identical investigator queries.               ║
║    5. [ALERT] OTel metric fires; on-call engineer checks API quota dashboard.       ║
═══════════════════════════════════════════════════════════════════════════════════════
```


### 7.2 OpenTelemetry Instrumentation Map

```
Span Hierarchy for POST /api/v1/query:

http.server [FastAPI middleware — root span]
│  Attributes: http.method, http.route, http.status_code,
│              case_id, analyst_id, request_id
│
├── query.classification [FastAPI handler]
│   │  Attributes: query_type, routing_decision
│   │  Duration target: < 50ms (cached in Redis)
│   │
├── retrieval.graph [Neo4j executor]
│   │  Attributes: neo4j.query_hash, neo4j.records_returned,
│   │              neo4j.db.name="neo4j", db.system="neo4j"
│   │  Span events: "query_executed", "results_received"
│   │  Duration target: < 200ms
│   │
├── retrieval.semantic [Qdrant executor]
│   │  Attributes: qdrant.collection, qdrant.top_k,
│   │              qdrant.filter_applied, qdrant.chunks_returned
│   │  Duration target: < 100ms
│   │
├── retrieval.hydration [PostgreSQL executor]
│   │  Attributes: db.system="postgresql", db.rows_returned
│   │  Duration target: < 100ms
│   │
├── context.compilation [Python — in-process]
│   │  Attributes: total_tokens, dedup_removed, sources_count
│   │  Duration target: < 20ms
│   │
└── llm.synthesis [LLM API client]
    Attributes: llm.model, llm.prompt_tokens, llm.completion_tokens,
                llm.provider, llm.request_id
    Span events: "request_sent", "first_token_received", "stream_complete"
    Duration target: < 4500ms


Span Hierarchy for Celery Task (parse_ufdr_archive):

celery.task [Worker root span — propagated from FastAPI via trace context in Redis]
│  Attributes: celery.task_name, celery.task_id, job_id, case_id
│
├── ingestion.download [Object storage]
│   Duration target: < 10s for 400MB
│
├── ingestion.parse [CPU-bound parsing]
│   Attributes: records_parsed_messages, records_parsed_calls
│   Duration target: < 30s
│
├── ingestion.postgresql [Bulk COPY]
│   Attributes: db.system, rows_copied, copy_duration_ms
│
├── ingestion.neo4j [Graph MERGE]
│   Attributes: nodes_created, nodes_merged, relationships_created
│
└── ingestion.vector [Embedding + upsert]
    Attributes: chunks_created, chunks_failed, embedding_model


Metric Definitions (OpenTelemetry Semantic Conventions):

Name                                Type        Unit    Labels
────────────────────────────────────────────────────────────────────────────
http.server.duration                Histogram   ms      route, status_code
celery.task.duration                Histogram   ms      task_name, status
ufdr.ingestion.records_total        Counter     records case_id, entity_type
ufdr.ingestion.bytes_processed      Counter     bytes   case_id
retrieval.neo4j.query_duration      Histogram   ms      operation
retrieval.qdrant.search_duration    Histogram   ms      collection
retrieval.qdrant.score_p50          Gauge       float   —
llm.api.duration                    Histogram   ms      provider, model
llm.api.tokens_total                Counter     tokens  provider, direction
llm.api.429_total                   Counter     —       provider


Log Schema (Structured JSON — all components):
{
  "timestamp":    "2024-01-15T14:32:05.123Z",
  "level":        "INFO|WARN|ERROR",
  "service":      "ufdr-api|ufdr-worker",
  "trace_id":     "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id":      "00f067aa0ba902b7",
  "job_id":       "550e8400-...",
  "case_id":      "7c9e6679-...",
  "event":        "ingestion.phase_complete",
  "phase":        "RELATIONAL_COMPLETE",
  "duration_ms":  14230,
  "records":      47892,
  "message":      "PostgreSQL bulk insert complete"
}
```

---