from typing import Any, Dict
from uuid import UUID
from fastapi import Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from scheduler_api.config import settings
from scheduler_api.db import get_session
from scheduler_api.models import User, OrganizationMembership
from scheduler_api.security import decode_access_token
from scheduler_api.errors import DomainError

security = HTTPBearer()


async def get_token_payload(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    try:
        payload = decode_access_token(credentials.credentials, settings)
        return payload
    except Exception:
        raise DomainError(
            code="unauthorized",
            message="Invalid or expired access token",
            status_code=status.HTTP_401_UNAUTHORIZED
        )


async def get_current_user(
    payload: Dict[str, Any] = Depends(get_token_payload),
    session: AsyncSession = Depends(get_session)
) -> User:
    user_id = payload.get("sub")
    if not user_id:
        raise DomainError(code="unauthorized", message="Invalid token claims", status_code=status.HTTP_401_UNAUTHORIZED)

    result = await session.execute(select(User).where(User.id == UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise DomainError(code="unauthorized", message="User not found", status_code=status.HTTP_401_UNAUTHORIZED)

    return user


async def get_current_membership(
    payload: Dict[str, Any] = Depends(get_token_payload),
    session: AsyncSession = Depends(get_session)
) -> OrganizationMembership:
    user_id = payload.get("sub")
    org_id = payload.get("org_id")
    if not user_id or not org_id:
        raise DomainError(code="unauthorized", message="Invalid token claims", status_code=status.HTTP_401_UNAUTHORIZED)

    result = await session.execute(
        select(OrganizationMembership).where(
            OrganizationMembership.user_id == UUID(user_id),
            OrganizationMembership.organization_id == UUID(org_id)
        )
    )
    membership = result.scalar_one_or_none()
    if not membership:
        raise DomainError(code="unauthorized", message="User is not a member of this organization", status_code=status.HTTP_401_UNAUTHORIZED)

    return membership
