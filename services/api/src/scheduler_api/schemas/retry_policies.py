from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self

from scheduler_api.enums import RetryStrategy


class RetryPolicyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    project_id: UUID
    strategy: RetryStrategy
    base_delay_seconds: int = Field(default=10, ge=1)
    max_delay_seconds: int = Field(default=3600, ge=1)
    max_attempts: int = Field(default=3, ge=1)

    @model_validator(mode="after")
    def validate_delays(self) -> Self:
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds must be greater than or equal to base_delay_seconds")
        return self


class RetryPolicyResponse(BaseModel):
    id: UUID
    project_id: UUID
    name: str
    strategy: RetryStrategy
    base_delay_seconds: int
    max_delay_seconds: int
    max_attempts: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
