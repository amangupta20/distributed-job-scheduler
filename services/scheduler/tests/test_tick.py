import pytest
import uuid
import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from scheduler_api.models import Organization, Project, Queue, Job, ScheduledJob, DeadLetterEntry, JobStateEvent, Worker
from scheduler_api.enums import JobStatus, RetryStrategy
from scheduler_service.tick import run_tick


@pytest.fixture
async def seed_env(db_session: AsyncSession):
    org = Organization(id=uuid.uuid4(), name="Sched Org")
    project = Project(id=uuid.uuid4(), organization_id=org.id, name="Sched Project")
    queue = Queue(id=uuid.uuid4(), project_id=project.id, name="default", priority=0, concurrency_limit=10)
    db_session.add_all([org, project, queue])
    await db_session.commit()
    return project, queue



@pytest.mark.asyncio
async def test_two_ticks_materialize_one_cron_occurrence(db_session: AsyncSession, seed_env) -> None:
    project, queue = seed_env
    cron_job = ScheduledJob(
        id=uuid.uuid4(),
        project_id=project.id,
        queue_id=queue.id,
        name="test-cron",
        expression="* * * * *",
        job_type="noop",
        payload={},
        next_run_at=datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)
    )
    db_session.add(cron_job)
    await db_session.commit()

    # Create session factory
    from scheduler_api.db import AsyncSessionLocal
    now = datetime(2026, 7, 12, 12, 0, 5, tzinfo=timezone.utc)

    # Run tick twice concurrently
    await asyncio.gather(
        run_tick(AsyncSessionLocal, now, 100),
        run_tick(AsyncSessionLocal, now, 100),
    )

    # Verify only one job materialized
    stmt = select(Job).where(Job.project_id == project.id)
    res = await db_session.execute(stmt)
    jobs = res.scalars().all()
    assert len(jobs) == 1
    assert jobs[0].status == JobStatus.QUEUED
    assert jobs[0].idempotency_key == f"cron:{cron_job.id}:2026-07-12T12:00:00+00:00"

    # Verify scheduled_job is updated
    await db_session.refresh(cron_job)
    assert cron_job.next_run_at == datetime(2026, 7, 12, 12, 1, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_due_delayed_promotion(db_session: AsyncSession, seed_env) -> None:
    project, queue = seed_env
    # Job scheduled in past
    job = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        queue_id=queue.id,
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.SCHEDULED,
        scheduled_at=datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc),
        timeout_seconds=60,
        max_attempts=3,
        attempt_count=0
    )
    db_session.add(job)
    await db_session.commit()

    from scheduler_api.db import AsyncSessionLocal
    now = datetime(2026, 7, 12, 12, 0, 5, tzinfo=timezone.utc)

    await run_tick(AsyncSessionLocal, now, 100)

    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED


@pytest.mark.asyncio
async def test_expired_lease_recovery_retry(db_session: AsyncSession, seed_env) -> None:
    project, queue = seed_env
    worker_id = uuid.uuid4()
    worker = Worker(id=worker_id, name="worker-retry", version="1.0.0", capacity=5, status="ready")
    db_session.add(worker)
    await db_session.flush()

    # Job claimed/running but lease expired
    job = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        queue_id=queue.id,
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.RUNNING,
        scheduled_at=datetime(2026, 7, 12, 11, 50, 0, tzinfo=timezone.utc),
        timeout_seconds=60,
        max_attempts=3,
        attempt_count=1,
        claimed_by_worker_id=worker_id,
        lease_token="lease-abc",
        lease_expires_at=datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)
    )
    db_session.add(job)
    await db_session.commit()

    from scheduler_api.db import AsyncSessionLocal
    now = datetime(2026, 7, 12, 12, 0, 5, tzinfo=timezone.utc)

    await run_tick(AsyncSessionLocal, now, 100)

    await db_session.refresh(job)
    # Retry scheduled with cleared lease
    assert job.status == JobStatus.RETRY_SCHEDULED
    assert job.lease_token is None
    assert job.claimed_by_worker_id is None
    assert job.scheduled_at > now


@pytest.mark.asyncio
async def test_expired_lease_to_dlq(db_session: AsyncSession, seed_env) -> None:
    project, queue = seed_env
    worker_id = uuid.uuid4()
    worker = Worker(id=worker_id, name="worker-dlq", version="1.0.0", capacity=5, status="ready")
    db_session.add(worker)
    await db_session.flush()

    # Job claimed/running but lease expired, max attempts reached
    job = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        queue_id=queue.id,
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.RUNNING,
        scheduled_at=datetime(2026, 7, 12, 11, 50, 0, tzinfo=timezone.utc),
        timeout_seconds=60,
        max_attempts=3,
        attempt_count=3,
        claimed_by_worker_id=worker_id,
        lease_token="lease-abc",
        lease_expires_at=datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)
    )
    db_session.add(job)
    await db_session.commit()

    from scheduler_api.db import AsyncSessionLocal
    now = datetime(2026, 7, 12, 12, 0, 5, tzinfo=timezone.utc)

    await run_tick(AsyncSessionLocal, now, 100)

    await db_session.refresh(job)
    assert job.status == JobStatus.DEAD_LETTERED
    assert job.lease_token is None

    # Check dead letter entry
    stmt = select(DeadLetterEntry).where(DeadLetterEntry.job_id == job.id)
    res = await db_session.execute(stmt)
    dlq = res.scalar_one()
    assert "Lease expired" in dlq.reason
