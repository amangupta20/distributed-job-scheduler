from datetime import datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel, Field


class QueueCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9_-]*$")
    project_id: UUID
    priority: int = Field(default=0, ge=-100, le=100)
    concurrency_limit: int = Field(default=10, ge=1, le=1000)
    rate_limit_per_minute: Optional[int] = Field(default=None, ge=1, le=1000000)
    retry_policy_id: Optional[UUID] = None


class QueueResponse(BaseModel):
    id: UUID
    project_id: UUID
    name: str
    priority: int
    concurrency_limit: int
    rate_limit_per_minute: Optional[int]
    retry_policy_id: Optional[UUID]
    pause_state: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
