from datetime import datetime
from typing import List, Optional
from uuid import UUID
from pydantic import BaseModel, Field


class WorkerRegister(BaseModel):
    worker_id: UUID
    name: str = Field(min_length=1, max_length=255)
    version: str = Field(default="1.0.0", max_length=100)
    concurrency_limit: int = Field(ge=1, le=1000)


class WorkerHeartbeat(BaseModel):
    running_job_ids: List[UUID] = Field(default_factory=list)


class HeartbeatResponse(BaseModel):
    status: str = "ok"
    cancelled_job_ids: List[UUID] = Field(default_factory=list)


class WorkerResponse(BaseModel):
    id: UUID
    name: str
    version: str
    status: str
    concurrency_limit: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
