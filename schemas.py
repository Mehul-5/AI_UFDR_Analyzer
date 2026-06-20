from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime
from typing import Optional, List, Dict, Any

class ExtractionResponse(BaseModel):
    job_id: UUID = Field(..., description="The unique identifier for this ingestion job.")
    case_id: str = Field(..., description="The case this extraction belongs to.")
    status: str = Field(..., description="Current status of the job.")
    poll_url: str = Field(..., description="The endpoint to poll for status updates.")
    submitted_at: datetime = Field(default_factory=datetime.utcnow)

class JobStatusResponse(BaseModel):
    job_id: UUID
    status: str
    phase: Optional[str] = None
    error_message: Optional[str] = None

class QueryRequest(BaseModel):
    query: str
    case_id: Optional[str] = "default-case-001"

# --- HYBRID RAG SCHEMAS ---

class QueryIntent(BaseModel):
    requires_graph: bool
    requires_sql_identity: bool = False
    requires_semantic: bool
    extracted_identifiers: list[str] = []
    optimized_search_queries: list[str] = []

class SQLIdentityAnomaly(BaseModel):
    phone: str
    known_aliases: list[str]

class GraphNodeResult(BaseModel):
    source_number: str
    target_number: str
    interaction_type: str
    frequency: int
    interaction_times: list[str] = []

class HydratedEntity(BaseModel):
    phone_number: str
    display_name: str
    organization: Optional[str]

class CompiledRetrievalContext(BaseModel):
    query_intent: QueryIntent
    graph_facts: list = []
    sql_facts: list[SQLIdentityAnomaly] = [] # NEW
    hydrated_entities: list = []
    semantic_chunks: list = []

    def compile_system_prompt(self) -> str:
        prompt = ""
        if self.sql_facts:
            prompt += f"\n[IDENTITY ANOMALIES FOUND]\n{self.sql_facts}"
        if self.graph_facts:
            prompt += f"\n[COMMUNICATION TOPOLOGY]\n{self.graph_facts}"
        if self.semantic_chunks:
            prompt += f"\n[SEMANTIC CHUNKS]\n{self.semantic_chunks}"
        return prompt