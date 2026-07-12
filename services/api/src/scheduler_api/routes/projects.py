from uuid import UUID
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from scheduler_api.db import get_session
from scheduler_api.models import Project, OrganizationMembership
from scheduler_api.deps import get_current_membership
from scheduler_api.errors import DomainError
from scheduler_api.schemas.projects import ProjectCreate, ProjectResponse

router = APIRouter(prefix="/projects", tags=["Projects"])


@router.post("", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
async def create_project(
    payload: ProjectCreate,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> ProjectResponse:
    project = Project(
        organization_id=membership.organization_id,
        name=payload.name
    )
    session.add(project)
    await session.commit()
    return project


@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project(
    project_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> ProjectResponse:
    result = await session.execute(
        select(Project).where(
            Project.id == project_id,
            Project.organization_id == membership.organization_id
        )
    )
    project = result.scalar_one_or_none()
    if not project:
        raise DomainError(
            code="not_found",
            message="Project not found",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return project
