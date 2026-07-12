from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from scheduler_api.db import get_session
from scheduler_api.models import User, Organization, OrganizationMembership
from scheduler_api.enums import Role
from scheduler_api.security import get_password_hash, verify_password, create_access_token
from scheduler_api.config import settings
from scheduler_api.errors import DomainError
from scheduler_api.schemas.auth import UserRegister, UserLogin, Token

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/register", response_model=Token, status_code=status.HTTP_201_CREATED)
async def register(payload: UserRegister, session: AsyncSession = Depends(get_session)) -> Token:
    # Check if user already exists
    result = await session.execute(select(User).where(User.email == payload.email))
    if result.scalar_one_or_none():
        raise DomainError(
            code="registration_failed",
            message="Email already registered",
            status_code=status.HTTP_409_CONFLICT
        )

    # Create Org
    org = Organization(name=payload.organization_name)
    session.add(org)
    await session.flush()  # Populates org.id

    # Create User
    hashed_pwd = get_password_hash(payload.password)
    user = User(email=payload.email, password_hash=hashed_pwd)
    session.add(user)
    await session.flush()  # Populates user.id

    # Create Membership
    membership = OrganizationMembership(
        organization_id=org.id,
        user_id=user.id,
        role=Role.OWNER
    )
    session.add(membership)
    await session.commit()

    token = create_access_token(user_id=user.id, org_id=org.id, role=Role.OWNER, settings=settings)
    return Token(access_token=token)


@router.post("/login", response_model=Token)
async def login(payload: UserLogin, session: AsyncSession = Depends(get_session)) -> Token:
    result = await session.execute(select(User).where(User.email == payload.email))
    user = result.scalar_one_or_none()
    if not user or not verify_password(payload.password, user.password_hash):
        raise DomainError(
            code="unauthorized",
            message="Invalid email or password",
            status_code=status.HTTP_401_UNAUTHORIZED
        )

    # Fetch membership
    result = await session.execute(
        select(OrganizationMembership).where(OrganizationMembership.user_id == user.id)
    )
    membership = result.scalar_one()
    if not membership:
        raise DomainError(
            code="unauthorized",
            message="User has no associated organizations",
            status_code=status.HTTP_401_UNAUTHORIZED
        )

    token = create_access_token(
        user_id=user.id,
        org_id=membership.organization_id,
        role=membership.role,
        settings=settings
    )
    return Token(access_token=token)
