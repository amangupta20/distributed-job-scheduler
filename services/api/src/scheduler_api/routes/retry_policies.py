from uuid import UUID
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from scheduler_api.db import get_session
from scheduler_api.models import Project, RetryPolicy, OrganizationMembership
from scheduler_api.deps import get_current_membership
from scheduler_api.errors import DomainError
from scheduler_api.schemas.retry_policies import RetryPolicyCreate, RetryPolicyResponse

router = APIRouter(prefix="/retry-policies", tags=["Retry Policies"])


@router.post("", response_model=RetryPolicyResponse, status_code=status.HTTP_201_CREATED)
async def create_retry_policy(
    payload: RetryPolicyCreate,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> RetryPolicyResponse:
    # Verify project exists and belongs to organization
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

    policy = RetryPolicy(
        project_id=payload.project_id,
        name=payload.name,
        strategy=payload.strategy,
        base_delay_seconds=payload.base_delay_seconds,
        max_delay_seconds=payload.max_delay_seconds,
        max_attempts=payload.max_attempts
    )
    session.add(policy)
    await session.commit()
    return policy


@router.get("/{policy_id}", response_model=RetryPolicyResponse)
async def get_retry_policy(
    policy_id: UUID,
    session: AsyncSession = Depends(get_session),
    membership: OrganizationMembership = Depends(get_current_membership)
) -> RetryPolicyResponse:
    result = await session.execute(
        select(RetryPolicy)
        .join(Project, Project.id == RetryPolicy.project_id)
        .where(
            RetryPolicy.id == policy_id,
            Project.organization_id == membership.organization_id
        )
    )
    policy = result.scalar_one_or_none()
    if not policy:
        raise DomainError(
            code="not_found",
            message="Retry policy not found",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return policy
