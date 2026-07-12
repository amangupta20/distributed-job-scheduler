import base64
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, and_, or_

from scheduler_api.db import get_session
from scheduler_api.models import Project, Queue, Job, JobStateEvent, DeadLetterEntry, OrganizationMembership
from scheduler_api.deps import get_current_membership
from scheduler_api.enums import JobStatus
from scheduler_api.errors import DomainError
from scheduler_api.schemas.jobs import JobCreate, JobResponse, JobListResponse
from scheduler_api.services.jobs import create_job as service_create_job

logger = logging.getLogger("scheduler_api.jobs")
router = APIRouter(prefix="/jobs", tags=["Jobs"])


def encode_cursor(created_at: datetime, job_id: UUID) -> str:
    data = {"created_at": created_at.isoformat(), "id": str(job_id)}
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()


def decode_cursor(cursor_str: str) -> tuple:
    try:
        decoded = base64.urlsafe_b64decode(cursor_str.encode()).decode()
        data = json.loads(decoded)
        dt = datetime.fromisoformat(data["created_at"])
        return dt, UUID(data["id"])
    except Exception:
        raise DomainError(code="validation_failed", message="Invalid cursor format", status_code=422)


@router.post("", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_job_endpoint(
    payload: JobCreate,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> Job:
    return await service_create_job(session, payload, membership.organization_id)


@router.get("", response_model=JobListResponse)
async def list_jobs(
    project_id: Optional[UUID] = None,
    queue_id: Optional[UUID] = None,
    status: Optional[str] = None,
    worker_id: Optional[UUID] = None,
    created_after: Optional[datetime] = None,
    created_before: Optional[datetime] = None,
    cursor: Optional[str] = None,
    limit: int = Query(default=20, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> Dict[str, Any]:
    # Enforce organization isolation
    # Find all projects belonging to organization
    proj_stmt = select(Project.id).where(Project.organization_id == membership.organization_id)
    proj_result = await session.execute(proj_stmt)
    allowed_project_ids = [r[0] for r in proj_result.all()]

    if not allowed_project_ids:
        return {"items": [], "next_cursor": None}

    # Base query filters
    filters = [Job.project_id.in_(allowed_project_ids)]

    if project_id:
        if project_id not in allowed_project_ids:
            return {"items": [], "next_cursor": None}
        filters.append(Job.project_id == project_id)

    if queue_id:
        filters.append(Job.queue_id == queue_id)

    if status:
        filters.append(Job.status == status)

    if worker_id:
        filters.append(Job.claimed_by_worker_id == worker_id)

    if created_after:
        filters.append(Job.created_at >= created_after)

    if created_before:
        filters.append(Job.created_at <= created_before)

    # Handle cursor
    if cursor:
        cursor_created_at, cursor_id = decode_cursor(cursor)
        # We sort by created_at DESC, id DESC. So:
        # created_at < cursor_created_at OR (created_at == cursor_created_at AND id < cursor_id)
        filters.append(
            or_(
                Job.created_at < cursor_created_at,
                and_(
                    Job.created_at == cursor_created_at,
                    Job.id < cursor_id
                )
            )
        )

    # Fetch limit + 1 items to determine if there is a next page
    stmt = (
        select(Job)
        .where(and_(*filters))
        .order_by(Job.created_at.desc(), Job.id.desc())
        .limit(limit + 1)
    )
    result = await session.execute(stmt)
    jobs = result.scalars().all()

    has_next = len(jobs) > limit
    items = jobs[:limit]

    next_cursor = None
    if has_next and items:
        last_item = items[-1]
        next_cursor = encode_cursor(last_item.created_at, last_item.id)

    return {
        "items": items,
        "next_cursor": next_cursor
    }


@router.post("/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> Job:
    # Verify job belongs to project in organization
    stmt = (
        select(Job)
        .join(Project, Project.id == Job.project_id)
        .where(
            Job.id == job_id,
            Project.organization_id == membership.organization_id
        )
    )
    result = await session.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise DomainError(
            code="not_found",
            message="Job not found",
            status_code=status.HTTP_404_NOT_FOUND
        )

    # Check terminal state
    terminal_states = {JobStatus.COMPLETED, JobStatus.CANCELLED, JobStatus.DEAD_LETTERED}
    if job.status in terminal_states:
        raise DomainError(
            code="invalid_state",
            message=f"Job in terminal state '{job.status}' cannot be cancelled",
            status_code=status.HTTP_409_CONFLICT
        )

    old_status = job.status
    job.status = JobStatus.CANCELLED
    job.completed_at = datetime.now(timezone.utc)

    # State Event
    event = JobStateEvent(
        job_id=job.id,
        from_status=old_status,
        to_status=JobStatus.CANCELLED
    )
    session.add(event)
    await session.commit()

    logger.info("Job cancelled", extra={"job_id": str(job.id), "org_id": str(membership.organization_id)})
    return job


@router.post("/{job_id}/replay-dlq", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def replay_dlq(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> Job:
    # Fetch original job and check tenancy
    stmt = (
        select(Job)
        .join(Project, Project.id == Job.project_id)
        .where(
            Job.id == job_id,
            Project.organization_id == membership.organization_id
        )
    )
    result = await session.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise DomainError(
            code="not_found",
            message="Job not found",
            status_code=status.HTTP_404_NOT_FOUND
        )

    if job.status != JobStatus.DEAD_LETTERED:
        raise DomainError(
            code="invalid_state",
            message=f"Only dead-lettered jobs can be replayed, current status: {job.status}",
            status_code=status.HTTP_409_CONFLICT
        )

    # Check DLQ entry
    dlq_stmt = select(DeadLetterEntry).where(
        DeadLetterEntry.job_id == job.id,
        DeadLetterEntry.replayed_at.is_(None)
    )
    dlq_result = await session.execute(dlq_stmt)
    dlq_entry = dlq_result.scalar_one_or_none()
    if not dlq_entry:
        raise DomainError(
            code="invalid_state",
            message="Job has no active unplayed dead-letter entry",
            status_code=status.HTTP_409_CONFLICT
        )

    now = datetime.now(timezone.utc)

    # Create new queued job (without original idempotency key to avoid uniqueness violation)
    new_job = Job(
        project_id=job.project_id,
        queue_id=job.queue_id,
        retry_policy_id=job.retry_policy_id,
        idempotency_key=None,
        type=job.type,
        payload=job.payload,
        priority=job.priority,
        status=JobStatus.QUEUED,
        scheduled_at=now,
        timeout_seconds=job.timeout_seconds,
        max_attempts=job.max_attempts,
        attempt_count=0
    )
    session.add(new_job)
    await session.flush()

    # Create state event for new job
    new_event = JobStateEvent(
        job_id=new_job.id,
        from_status=None,
        to_status=JobStatus.QUEUED
    )
    session.add(new_event)

    # Mark original DLQ entry as replayed
    dlq_entry.replayed_at = now
    dlq_entry.replacement_job_id = new_job.id

    # Notify workers
    await session.execute(
        text("SELECT pg_notify('jobs_available', :queue_id)"),
        {"queue_id": str(job.queue_id)}
    )

    await session.commit()
    logger.info("DLQ job replayed", extra={"old_job_id": str(job.id), "new_job_id": str(new_job.id)})
    return new_job
