from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field


class ProjectCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class ProjectResponse(BaseModel):
    id: UUID
    organization_id: UUID
    name: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
