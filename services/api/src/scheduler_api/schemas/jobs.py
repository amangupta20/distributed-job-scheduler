from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID
from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self


class JobCreate(BaseModel):
    project_id: UUID
    queue_id: UUID
    type: str = Field(min_length=1, max_length=100)
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=-100, le=100)
    scheduled_at: Optional[datetime] = None
    timeout_seconds: int = Field(default=300, ge=1, le=86400)
    idempotency_key: Optional[str] = Field(default=None, max_length=255)
    retry_policy_id: Optional[UUID] = None

    @model_validator(mode="after")
    def validate_job_type_and_payload(self) -> Self:
        allowed_types = {"noop", "http", "chaos"}
        if self.type not in allowed_types:
            raise ValueError(f"Job type must be one of {allowed_types}")

        if self.type == "http":
            url = self.payload.get("url")
            method = self.payload.get("method")
            if not url or not isinstance(url, str) or not (url.startswith("http://") or url.startswith("https://")):
                raise ValueError("HTTP jobs require a valid 'url' payload parameter starting with http:// or https://")
            
            allowed_methods = {"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"}
            if not method or not isinstance(method, str) or method.upper() not in allowed_methods:
                raise ValueError(f"HTTP jobs require a valid 'method' payload parameter from {allowed_methods}")
            
            # Normalize method to uppercase
            self.payload["method"] = method.upper()
            
        return self


class JobResponse(BaseModel):
    id: UUID
    project_id: UUID
    queue_id: UUID
    retry_policy_id: Optional[UUID] = None
    batch_id: Optional[UUID] = None
    idempotency_key: Optional[str] = None
    type: str
    payload: Dict[str, Any]
    priority: int
    status: str
    scheduled_at: datetime
    timeout_seconds: int
    attempt_count: int
    max_attempts: int
    claimed_by_worker_id: Optional[UUID] = None
    lease_token: Optional[str] = None
    lease_expires_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class JobListResponse(BaseModel):
    items: List[JobResponse]
    next_cursor: Optional[str] = None

