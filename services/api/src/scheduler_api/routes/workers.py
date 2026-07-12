import logging
from datetime import datetime, timezone
from uuid import UUID
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from scheduler_api.db import get_session
from scheduler_api.models import Worker, WorkerHeartbeat, Job, OrganizationMembership
from scheduler_api.deps import get_current_membership
from scheduler_api.enums import JobStatus, WorkerState
from scheduler_api.errors import DomainError
from scheduler_api.schemas.workers import WorkerRegister, WorkerHeartbeat as SchemaHeartbeat, HeartbeatResponse, WorkerResponse

logger = logging.getLogger("scheduler_api.workers")
router = APIRouter(prefix="/workers", tags=["Workers"])


@router.post("/register", response_model=WorkerResponse, status_code=status.HTTP_201_CREATED)
async def register_worker(
    payload: WorkerRegister,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> WorkerResponse:
    # Check if worker already exists
    stmt = select(Worker).where(Worker.id == payload.worker_id)
    res = await session.execute(stmt)
    worker = res.scalar_one_or_none()

    if worker:
        worker.name = payload.name
        worker.version = payload.version
        worker.capacity = payload.concurrency_limit
        worker.status = WorkerState.READY
    else:
        worker = Worker(
            id=payload.worker_id,
            name=payload.name,
            version=payload.version,
            capacity=payload.concurrency_limit,
            status=WorkerState.READY
        )
        session.add(worker)

    await session.commit()
    logger.info("Worker registered/updated", extra={"worker_id": str(worker.id), "name": worker.name})
    return WorkerResponse(
        id=worker.id,
        name=worker.name,
        version=worker.version,
        status=worker.status,
        concurrency_limit=worker.capacity,
        created_at=worker.created_at,
        updated_at=worker.updated_at
    )


@router.post("/{worker_id}/heartbeat", response_model=HeartbeatResponse)
async def worker_heartbeat(
    worker_id: UUID,
    payload: SchemaHeartbeat,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> HeartbeatResponse:
    now = datetime.now(timezone.utc)

    # Verify worker exists
    stmt = select(Worker).where(Worker.id == worker_id)
    res = await session.execute(stmt)
    worker = res.scalar_one_or_none()
    if not worker:
        raise DomainError(
            code="not_found",
            message=f"Worker with ID '{worker_id}' not found",
            status_code=status.HTTP_404_NOT_FOUND
        )

    # Update worker activity
    worker.status = WorkerState.READY

    # Log heartbeat record
    hb = WorkerHeartbeat(
        worker_id=worker_id,
        status=WorkerState.READY,
        active_jobs=len(payload.running_job_ids)
    )
    session.add(hb)

    # Query for cancelled jobs among reported running jobs
    cancelled_ids = []
    if payload.running_job_ids:
        job_stmt = select(Job.id).where(
            Job.id.in_(payload.running_job_ids),
            Job.status == JobStatus.CANCELLED
        )
        job_res = await session.execute(job_stmt)
        cancelled_ids = [r[0] for r in job_res.all()]

    await session.commit()
    return HeartbeatResponse(status="ok", cancelled_job_ids=cancelled_ids)

