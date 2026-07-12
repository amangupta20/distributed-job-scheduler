import logging
import croniter
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from scheduler_api.models import ScheduledJob, Job, JobStateEvent, DeadLetterEntry, RetryPolicy, Queue
from scheduler_api.enums import JobStatus, RetryStrategy
from scheduler_api.services.retries import calculate_retry_at

logger = logging.getLogger("scheduler_service.tick")


@dataclass
class TickResult:
    cron: int
    promoted: int
    recovered: int


async def run_tick(session_factory: async_sessionmaker, now: datetime, batch_size: int) -> TickResult:
    cron_count = await materialize_cron(session_factory, now, batch_size)
    promoted_count = await promote_due_jobs(session_factory, now, batch_size)
    recovered_count = await recover_expired_leases(session_factory, now, batch_size)
    return TickResult(cron=cron_count, promoted=promoted_count, recovered=recovered_count)


async def materialize_cron(session_factory: async_sessionmaker, now: datetime, batch_size: int) -> int:
    count = 0
    async with session_factory() as session:
        # Find scheduled jobs that are due (next_run_at <= now)
        stmt = (
            select(ScheduledJob)
            .where(ScheduledJob.next_run_at <= now)
            .order_by(ScheduledJob.next_run_at.asc())
            .with_for_update(skip_locked=True)
            .limit(batch_size)
        )
        res = await session.execute(stmt)
        scheduled_jobs = res.scalars().all()

        if not scheduled_jobs:
            return 0

        for sj in scheduled_jobs:
            # occurrences starting from sj.next_run_at up to now
            start_time = sj.next_run_at - timedelta(seconds=1)
            iter_occ = croniter.croniter(sj.expression, start_time)
            occurrences = []

            # Find all occurrences <= now
            while len(occurrences) < 1000:
                nxt = iter_occ.get_next(datetime)
                if nxt.tzinfo is None:
                    nxt = nxt.replace(tzinfo=timezone.utc)
                if nxt <= now:
                    occurrences.append(nxt)
                else:
                    break

            for occ in occurrences:
                idem_key = f"cron:{sj.id}:{occ.isoformat()}"

                # Check if job already exists (idempotency safety)
                exist_check = await session.execute(
                    select(Job.id).where(Job.project_id == sj.project_id, Job.idempotency_key == idem_key)
                )
                if exist_check.scalar_one_or_none():
                    continue

                job = Job(
                    project_id=sj.project_id,
                    queue_id=sj.queue_id,
                    idempotency_key=idem_key,
                    type=sj.job_type,
                    payload=sj.payload,
                    priority=0,  # Default
                    status=JobStatus.QUEUED,
                    scheduled_at=occ,
                    timeout_seconds=300,  # Default
                    max_attempts=3,  # Default
                    attempt_count=0
                )
                session.add(job)
                await session.flush()

                event = JobStateEvent(
                    job_id=job.id,
                    from_status=None,
                    to_status=JobStatus.QUEUED
                )
                session.add(event)

                await session.execute(
                    text("SELECT pg_notify('jobs_available', :queue_id)"),
                    {"queue_id": str(sj.queue_id)}
                )
                count += 1

            if occurrences:
                iter_next = croniter.croniter(sj.expression, occurrences[-1])
                nxt_run = iter_next.get_next(datetime)
                if nxt_run.tzinfo is None:
                    nxt_run = nxt_run.replace(tzinfo=timezone.utc)
                sj.next_run_at = nxt_run
            else:
                iter_next = croniter.croniter(sj.expression, sj.next_run_at)
                nxt_run = iter_next.get_next(datetime)
                if nxt_run.tzinfo is None:
                    nxt_run = nxt_run.replace(tzinfo=timezone.utc)
                sj.next_run_at = nxt_run

        await session.commit()
    return count


async def promote_due_jobs(session_factory: async_sessionmaker, now: datetime, batch_size: int) -> int:
    count = 0
    async with session_factory() as session:
        stmt = (
            select(Job)
            .where(
                Job.status.in_([JobStatus.SCHEDULED, JobStatus.RETRY_SCHEDULED]),
                Job.scheduled_at <= now
            )
            .order_by(Job.scheduled_at.asc())
            .with_for_update(skip_locked=True)
            .limit(batch_size)
        )
        res = await session.execute(stmt)
        jobs = res.scalars().all()

        if not jobs:
            return 0

        for job in jobs:
            old_status = job.status
            job.status = JobStatus.QUEUED

            event = JobStateEvent(
                job_id=job.id,
                from_status=old_status,
                to_status=JobStatus.QUEUED
            )
            session.add(event)

            await session.execute(
                text("SELECT pg_notify('jobs_available', :queue_id)"),
                {"queue_id": str(job.queue_id)}
            )
            count += 1

        await session.commit()
    return count


async def recover_expired_leases(session_factory: async_sessionmaker, now: datetime, batch_size: int) -> int:
    count = 0
    async with session_factory() as session:
        stmt = (
            select(Job)
            .where(
                Job.status.in_([JobStatus.CLAIMED, JobStatus.RUNNING]),
                Job.lease_expires_at <= now
            )
            .order_by(Job.lease_expires_at.asc())
            .with_for_update(skip_locked=True)
            .limit(batch_size)
        )
        res = await session.execute(stmt)
        jobs = res.scalars().all()

        if not jobs:
            return 0

        for job in jobs:
            old_status = job.status

            if job.attempt_count < job.max_attempts:
                # Resolve policy
                policy_id = job.retry_policy_id
                if not policy_id:
                    q_res = await session.execute(select(Queue.retry_policy_id).where(Queue.id == job.queue_id))
                    policy_id = q_res.scalar_one_or_none()

                strategy = RetryStrategy.FIXED
                base_delay = 10
                max_delay = 60

                if policy_id:
                    p_res = await session.execute(select(RetryPolicy).where(RetryPolicy.id == policy_id))
                    policy = p_res.scalar_one_or_none()
                    if policy:
                        strategy = policy.strategy
                        base_delay = policy.base_delay_seconds
                        max_delay = policy.max_delay_seconds

                next_retry = calculate_retry_at(
                    strategy=strategy,
                    base_delay_seconds=base_delay,
                    attempt=job.attempt_count,
                    now=now
                )

                delay_sec = (next_retry - now).total_seconds()
                if delay_sec > max_delay:
                    next_retry = now + timedelta(seconds=max_delay)

                job.status = JobStatus.RETRY_SCHEDULED
                job.scheduled_at = next_retry
                job.claimed_by_worker_id = None
                job.lease_token = None
                job.lease_expires_at = None

                event = JobStateEvent(
                    job_id=job.id,
                    from_status=old_status,
                    to_status=JobStatus.RETRY_SCHEDULED
                )
                session.add(event)
            else:
                job.status = JobStatus.DEAD_LETTERED
                job.claimed_by_worker_id = None
                job.lease_token = None
                job.lease_expires_at = None

                event = JobStateEvent(
                    job_id=job.id,
                    from_status=old_status,
                    to_status=JobStatus.DEAD_LETTERED
                )
                session.add(event)

                dlq = DeadLetterEntry(
                    job_id=job.id,
                    reason=f"Lease expired and max attempts reached ({job.max_attempts})"
                )
                session.add(dlq)

            count += 1

        await session.commit()
    return count
