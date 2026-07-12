from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from scheduler_api.db import get_session
from scheduler_api.deps import get_current_membership
from scheduler_api.errors import DomainError
from scheduler_api.models import OrganizationMembership, ScheduledJob
from scheduler_api.repositories import scheduling
from scheduler_api.schemas.scheduled_jobs import (
    ScheduledJobCreate,
    ScheduledJobListResponse,
    ScheduledJobResponse,
)

router = APIRouter(prefix="/scheduled-jobs", tags=["Scheduled Jobs"])


@router.post("", response_model=ScheduledJobResponse, status_code=status.HTTP_201_CREATED)
async def create_scheduled_job(
    payload: ScheduledJobCreate,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership),
) -> ScheduledJob:
    project = await scheduling.get_organization_project(session, payload.project_id, membership.organization_id)
    if project is None:
        raise DomainError(code="not_found", message="Project not found", status_code=status.HTTP_404_NOT_FOUND)
    queue = await scheduling.get_project_queue(session, payload.queue_id, payload.project_id)
    if queue is None:
        raise DomainError(code="not_found", message="Queue not found", status_code=status.HTTP_404_NOT_FOUND)

    try:
        return await scheduling.create_scheduled_job(session, payload)
    except scheduling.ScheduledJobNameConflict:
        raise DomainError(
            code="conflict",
            message="A scheduled job with this name already exists in the project",
            status_code=status.HTTP_409_CONFLICT,
        )


@router.get("", response_model=ScheduledJobListResponse)
async def list_scheduled_jobs(
    project_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership),
) -> dict[str, list[ScheduledJob]]:
    project = await scheduling.get_organization_project(session, project_id, membership.organization_id)
    if project is None:
        raise DomainError(code="not_found", message="Project not found", status_code=status.HTTP_404_NOT_FOUND)
    return {"items": await scheduling.list_project_scheduled_jobs(session, project_id)}


@router.delete("/{scheduled_job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_scheduled_job(
    scheduled_job_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership),
) -> Response:
    scheduled_job = await scheduling.get_organization_scheduled_job(
        session, scheduled_job_id, membership.organization_id
    )
    if scheduled_job is None:
        raise DomainError(code="not_found", message="Scheduled job not found", status_code=status.HTTP_404_NOT_FOUND)

    await scheduling.delete_scheduled_job(session, scheduled_job)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
