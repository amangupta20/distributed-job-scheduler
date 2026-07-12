import logging
from uuid import UUID
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from scheduler_api.db import get_session
from scheduler_api.models import Project, Queue, RetryPolicy, OrganizationMembership
from scheduler_api.deps import get_current_membership
from scheduler_api.errors import DomainError
from scheduler_api.schemas.queues import QueueCreate, QueueResponse

logger = logging.getLogger("scheduler_api.queues")
router = APIRouter(prefix="/queues", tags=["Queues"])


@router.post("", response_model=QueueResponse, status_code=status.HTTP_201_CREATED)
async def create_queue(
    payload: QueueCreate,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> QueueResponse:
    # Verify project belongs to organization
    proj_result = await session.execute(
        select(Project).where(
            Project.id == payload.project_id,
            Project.organization_id == membership.organization_id
        )
    )
    if not proj_result.scalar_one_or_none():
        raise DomainError(
            code="not_found",
            message="Project not found",
            status_code=status.HTTP_404_NOT_FOUND
        )

    # Verify retry policy belongs to organization
    if payload.retry_policy_id:
        policy_result = await session.execute(
            select(RetryPolicy)
            .join(Project, Project.id == RetryPolicy.project_id)
            .where(
                RetryPolicy.id == payload.retry_policy_id,
                Project.organization_id == membership.organization_id
            )
        )
        if not policy_result.scalar_one_or_none():
            raise DomainError(
                code="not_found",
                message="Retry policy not found",
                status_code=status.HTTP_404_NOT_FOUND
            )

    queue = Queue(
        project_id=payload.project_id,
        name=payload.name,
        priority=payload.priority,
        concurrency_limit=payload.concurrency_limit,
        rate_limit_per_minute=payload.rate_limit_per_minute,
        rate_tokens=payload.rate_limit_per_minute,  # Init tokens to capacity
        retry_policy_id=payload.retry_policy_id
    )
    session.add(queue)
    await session.commit()
    return queue


@router.get("/{queue_id}", response_model=QueueResponse)
async def get_queue(
    queue_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> QueueResponse:
    result = await session.execute(
        select(Queue)
        .join(Project, Project.id == Queue.project_id)
        .where(
            Queue.id == queue_id,
            Project.organization_id == membership.organization_id
        )
    )
    queue = result.scalar_one_or_none()
    if not queue:
        raise DomainError(
            code="not_found",
            message="Queue not found",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return queue


@router.post("/{queue_id}/pause", response_model=QueueResponse)
async def pause_queue(
    queue_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> QueueResponse:
    result = await session.execute(
        select(Queue)
        .join(Project, Project.id == Queue.project_id)
        .where(
            Queue.id == queue_id,
            Project.organization_id == membership.organization_id
        )
    )
    queue = result.scalar_one_or_none()
    if not queue:
        raise DomainError(
            code="not_found",
            message="Queue not found",
            status_code=status.HTTP_404_NOT_FOUND
        )

    queue.pause_state = True
    await session.commit()

    logger.info(
        "Queue paused",
        extra={
            "queue_id": str(queue.id),
            "project_id": str(queue.project_id),
            "org_id": str(membership.organization_id)
        }
    )
    return queue


@router.post("/{queue_id}/resume", response_model=QueueResponse)
async def resume_queue(
    queue_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> QueueResponse:
    result = await session.execute(
        select(Queue)
        .join(Project, Project.id == Queue.project_id)
        .where(
            Queue.id == queue_id,
            Project.organization_id == membership.organization_id
        )
    )
    queue = result.scalar_one_or_none()
    if not queue:
        raise DomainError(
            code="not_found",
            message="Queue not found",
            status_code=status.HTTP_404_NOT_FOUND
        )

    queue.pause_state = False
    await session.commit()

    logger.info(
        "Queue resumed",
        extra={
            "queue_id": str(queue.id),
            "project_id": str(queue.project_id),
            "org_id": str(membership.organization_id)
        }
    )
    return queue
