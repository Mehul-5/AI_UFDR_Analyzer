from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime
from typing import Optional

class ExtractionResponse(BaseModel):
    """
    The response payload when an investigator uploads a forensic file.
    """
    job_id: UUID = Field(..., description="The unique identifier for this ingestion job.")
    case_id: str = Field(..., description="The case this extraction belongs to.")
    status: str = Field(..., description="Current status of the job (e.g., QUEUED).")
    poll_url: str = Field(..., description="The endpoint to poll for status updates.")
    submitted_at: datetime = Field(default_factory=datetime.utcnow)

class JobStatusResponse(BaseModel):
    """
    The response payload when checking a job's status.
    """
    job_id: UUID
    status: str
    phase: Optional[str] = None
    error_message: Optional[str] = None

class QueryRequest(BaseModel):
    """
    The payload sent by the investigator to ask the AI a question.
    """
    query: str = Field(..., description="The natural language question to ask the AI.")
    job_id: Optional[str] = Field(None, description="Optional job ID to isolate the search to a specific case.")