import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text

from scheduler_api.models import Project, Queue, RetryPolicy, Job, JobStateEvent
from scheduler_api.enums import JobStatus
from scheduler_api.errors import DomainError
from scheduler_api.schemas.jobs import JobCreate


def canonical_request_hash(job_type: str, payload: dict, queue_id: UUID, scheduled_at: Optional[datetime]) -> str:
    sched_str = scheduled_at.isoformat() if scheduled_at else ""
    value = json.dumps(
        {"type": job_type, "payload": payload, "queue_id": str(queue_id), "scheduled_at": sched_str},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(value.encode()).hexdigest()


async def create_job(
    session: AsyncSession,
    payload: JobCreate,
    org_id: UUID
) -> Job:
    # 1. Verify project exists and belongs to org
    proj_result = await session.execute(
        select(Project).where(
            Project.id == payload.project_id,
            Project.organization_id == org_id
        )
    )
    project = proj_result.scalar_one_or_none()
    if not project:
        raise DomainError(
            code="not_found",
            message="Project not found",
            status_code=404
        )

    # 2. Verify queue exists and belongs to project
    queue_result = await session.execute(
        select(Queue).where(
            Queue.id == payload.queue_id,
            Queue.project_id == payload.project_id
        )
    )
    queue = queue_result.scalar_one_or_none()
    if not queue:
        raise DomainError(
            code="not_found",
            message="Queue not found",
            status_code=404
        )

    # 3. Handle Retry Policy resolution
    policy_id = payload.retry_policy_id or queue.retry_policy_id
    max_attempts = 3
    if policy_id:
        policy_result = await session.execute(
            select(RetryPolicy).where(
                RetryPolicy.id == policy_id,
                RetryPolicy.project_id == payload.project_id
            )
        )
        policy = policy_result.scalar_one_or_none()
        if not policy:
            raise DomainError(
                code="not_found",
                message="Retry policy not found",
                status_code=404
            )
        max_attempts = policy.max_attempts

    # 4. Check Idempotency Key
    if payload.idempotency_key:
        result = await session.execute(
            select(Job).where(
                Job.project_id == payload.project_id,
                Job.idempotency_key == payload.idempotency_key
            )
        )
        existing_job = result.scalar_one_or_none()
        if existing_job:
            # Direct equivalence checks
            now = datetime.now(timezone.utc)
            core_match = (
                existing_job.type == payload.type
                and existing_job.payload == payload.payload
                and existing_job.queue_id == payload.queue_id
                and existing_job.priority == payload.priority
                and existing_job.timeout_seconds == payload.timeout_seconds
            )
            
            incoming_sched = payload.scheduled_at
            existing_sched = existing_job.scheduled_at
            
            is_incoming_future = incoming_sched and incoming_sched > now
            is_existing_future = existing_sched and existing_sched > now
            
            sched_match = False
            if not is_incoming_future and not is_existing_future:
                sched_match = True
            elif is_incoming_future and is_existing_future:
                sched_match = abs((incoming_sched - existing_sched).total_seconds()) < 1.0

            if core_match and sched_match:
                return existing_job
            else:
                raise DomainError(
                    code="conflict",
                    message="Job with this idempotency key already exists with a different payload",
                    status_code=409
                )

    # 5. Determine initial status and scheduled_at
    now = datetime.now(timezone.utc)
    target_sched = payload.scheduled_at
    if not target_sched or target_sched <= now:
        target_sched = now
        status = JobStatus.QUEUED
    else:
        status = JobStatus.SCHEDULED

    # 6. Create Job
    job = Job(
        project_id=payload.project_id,
        queue_id=payload.queue_id,
        retry_policy_id=policy_id,
        idempotency_key=payload.idempotency_key,
        type=payload.type,
        payload=payload.payload,
        priority=payload.priority,
        status=status,
        scheduled_at=target_sched,
        timeout_seconds=payload.timeout_seconds,
        max_attempts=max_attempts,
        attempt_count=0
    )
    session.add(job)
    await session.flush()  # Populates job.id

    # 7. Create Job State Event
    event = JobStateEvent(
        job_id=job.id,
        from_status=None,
        to_status=status
    )
    session.add(event)

    # 8. PostgreSQL Notify for immediately runnable jobs
    if status == JobStatus.QUEUED:
        await session.execute(
            text("SELECT pg_notify('jobs_available', :queue_id)"),
            {"queue_id": str(payload.queue_id)}
        )

    await session.commit()
    return job
