from datetime import datetime
from typing import Any
from uuid import UUID

from croniter import croniter
from pydantic import BaseModel, Field, field_validator


class ScheduledJobCreate(BaseModel):
    project_id: UUID
    queue_id: UUID
    name: str = Field(min_length=1, max_length=255)
    expression: str = Field(min_length=1, max_length=100)
    job_type: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    next_run_at: datetime

    @field_validator("expression")
    @classmethod
    def validate_cron_expression(cls, value: str) -> str:
        if not croniter.is_valid(value):
            raise ValueError("expression must be a valid cron expression")
        return value


class ScheduledJobResponse(BaseModel):
    id: UUID
    project_id: UUID
    queue_id: UUID
    name: str
    expression: str
    job_type: str
    payload: dict[str, Any]
    next_run_at: datetime
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ScheduledJobListResponse(BaseModel):
    items: list[ScheduledJobResponse]
