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
    query: str = Field(..., description="The natural language question to ask the AI.")
    job_id: Optional[str] = Field(None, description="Optional job ID to isolate the search.")

# --- HYBRID RAG SCHEMAS ---

class QueryIntent(BaseModel):
    requires_graph: bool = Field(default=True, description="True if query asks for relationships, frequencies, or counts.")
    requires_semantic: bool = Field(default=True, description="True if query asks for semantic conversation context.")
    extracted_identifiers: List[str] = Field(default_factory=list)

class GraphNodeResult(BaseModel):
    source_number: str
    target_number: str
    interaction_type: str
    frequency: int

class HydratedEntity(BaseModel):
    phone_number: str
    display_name: str
    organization: Optional[str]

class CompiledRetrievalContext(BaseModel):
    query_intent: QueryIntent
    graph_facts: List[GraphNodeResult] = []
    hydrated_entities: List[HydratedEntity] = []
    semantic_chunks: List[Dict[str, Any]] = []

    def compile_system_prompt(self) -> str:
        """
        This is the anti-hallucination compiler. 
        It forces the LLM to read the relational truth before the raw vectors.
        """
        prompt = "## RETRIEVED FORENSIC CONTEXT\n\n"
        
        if self.hydrated_entities:
            prompt += "### KNOWN IDENTITIES (RELATIONAL TRUTH)\n"
            for entity in self.hydrated_entities:
                name = entity.display_name or "Unknown Name"
                prompt += f"- Number: {entity.phone_number} | Identity: {name} | Org: {entity.organization}\n"
            prompt += "\n"
        
        if self.graph_facts:
            prompt += "### COMMUNICATION TOPOLOGY (GRAPH FREQUENCIES)\n"
            for fact in self.graph_facts:
                prompt += f"- {fact.source_number} {fact.interaction_type} {fact.target_number} ({fact.frequency} times)\n"
            prompt += "\n"

        if self.semantic_chunks:
            prompt += "### RELEVANT CHAT TRANSCRIPTS (VECTOR SEARCH)\n"
            for chunk in self.semantic_chunks:
                prompt += f"--- Thread: {chunk['thread']} | Time: {chunk['timeframe']} ---\n{chunk['text']}\n\n"
        
        return prompt