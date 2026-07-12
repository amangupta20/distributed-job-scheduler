from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scheduler_api.db import get_session
from scheduler_api.deps import get_current_membership
from scheduler_api.errors import DomainError
from scheduler_api.models import OrganizationMembership, Project, Queue, ScheduledJob
from scheduler_api.schemas.scheduled_jobs import (
    ScheduledJobCreate,
    ScheduledJobListResponse,
    ScheduledJobResponse,
)

router = APIRouter(prefix="/scheduled-jobs", tags=["Scheduled Jobs"])


async def get_organization_project(
    session: AsyncSession, project_id: UUID, organization_id: UUID
) -> Project:
    result = await session.execute(
        select(Project).where(Project.id == project_id, Project.organization_id == organization_id)
    )
    project = result.scalar_one_or_none()
    if project is None:
        raise DomainError(code="not_found", message="Project not found", status_code=status.HTTP_404_NOT_FOUND)
    return project


@router.post("", response_model=ScheduledJobResponse, status_code=status.HTTP_201_CREATED)
async def create_scheduled_job(
    payload: ScheduledJobCreate,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership),
) -> ScheduledJob:
    await get_organization_project(session, payload.project_id, membership.organization_id)
    queue_result = await session.execute(
        select(Queue).where(Queue.id == payload.queue_id, Queue.project_id == payload.project_id)
    )
    if queue_result.scalar_one_or_none() is None:
        raise DomainError(code="not_found", message="Queue not found", status_code=status.HTTP_404_NOT_FOUND)

    scheduled_job = ScheduledJob(**payload.model_dump())
    session.add(scheduled_job)
    await session.commit()
    await session.refresh(scheduled_job)
    return scheduled_job


@router.get("", response_model=ScheduledJobListResponse)
async def list_scheduled_jobs(
    project_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership),
) -> dict[str, list[ScheduledJob]]:
    await get_organization_project(session, project_id, membership.organization_id)
    result = await session.execute(
        select(ScheduledJob)
        .where(ScheduledJob.project_id == project_id)
        .order_by(ScheduledJob.next_run_at.asc(), ScheduledJob.id.asc())
    )
    return {"items": list(result.scalars())}


@router.delete("/{scheduled_job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_scheduled_job(
    scheduled_job_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership),
) -> Response:
    result = await session.execute(
        select(ScheduledJob)
        .join(Project, Project.id == ScheduledJob.project_id)
        .where(ScheduledJob.id == scheduled_job_id, Project.organization_id == membership.organization_id)
    )
    scheduled_job = result.scalar_one_or_none()
    if scheduled_job is None:
        raise DomainError(code="not_found", message="Scheduled job not found", status_code=status.HTTP_404_NOT_FOUND)

    await session.delete(scheduled_job)
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
