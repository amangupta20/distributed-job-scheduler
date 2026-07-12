from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from scheduler_api.models import Project, Queue, ScheduledJob
from scheduler_api.schemas.scheduled_jobs import ScheduledJobCreate


class ScheduledJobNameConflict(Exception):
    """Raised when a schedule name already exists within a project."""


async def get_organization_project(
    session: AsyncSession, project_id: UUID, organization_id: UUID
) -> Project | None:
    result = await session.execute(
        select(Project).where(Project.id == project_id, Project.organization_id == organization_id)
    )
    return result.scalar_one_or_none()


async def get_project_queue(session: AsyncSession, queue_id: UUID, project_id: UUID) -> Queue | None:
    result = await session.execute(select(Queue).where(Queue.id == queue_id, Queue.project_id == project_id))
    return result.scalar_one_or_none()


async def create_scheduled_job(session: AsyncSession, payload: ScheduledJobCreate) -> ScheduledJob:
    existing = await session.execute(
        select(ScheduledJob.id).where(
            ScheduledJob.project_id == payload.project_id,
            ScheduledJob.name == payload.name,
        )
    )
    if existing.scalar_one_or_none() is not None:
        raise ScheduledJobNameConflict

    scheduled_job = ScheduledJob(**payload.model_dump())
    session.add(scheduled_job)
    try:
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        if getattr(error.orig, "diag", None) and error.orig.diag.constraint_name == "uq_scheduled_job_project_name":
            raise ScheduledJobNameConflict from error
        raise
    await session.refresh(scheduled_job)
    return scheduled_job


async def list_project_scheduled_jobs(session: AsyncSession, project_id: UUID) -> list[ScheduledJob]:
    result = await session.execute(
        select(ScheduledJob)
        .where(ScheduledJob.project_id == project_id)
        .order_by(ScheduledJob.next_run_at.asc(), ScheduledJob.id.asc())
    )
    return list(result.scalars())


async def get_organization_scheduled_job(
    session: AsyncSession, scheduled_job_id: UUID, organization_id: UUID
) -> ScheduledJob | None:
    result = await session.execute(
        select(ScheduledJob)
        .join(Project, Project.id == ScheduledJob.project_id)
        .where(ScheduledJob.id == scheduled_job_id, Project.organization_id == organization_id)
    )
    return result.scalar_one_or_none()


async def delete_scheduled_job(session: AsyncSession, scheduled_job: ScheduledJob) -> None:
    await session.delete(scheduled_job)
    await session.commit()
