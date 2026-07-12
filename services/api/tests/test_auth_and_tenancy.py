import pytest
import uuid
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from scheduler_api.models import User, Organization, OrganizationMembership, Project
from scheduler_api.enums import Role
from scheduler_api.security import get_password_hash, create_access_token


@pytest.mark.asyncio
async def test_auth_register_creates_user_org_membership(api_client: AsyncClient, db_session: AsyncSession) -> None:
    payload = {
        "email": "test@example.com",
        "password": "securepassword123",
        "organization_name": "Test Org"
    }
    response = await api_client.post("/api/v1/auth/register", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"

    # Verify database state
    result = await db_session.execute(select(User).where(User.email == "test@example.com"))
    user = result.scalar_one_or_none()
    assert user is not None
    assert user.email == "test@example.com"

    result = await db_session.execute(select(Organization).where(Organization.name == "Test Org"))
    org = result.scalar_one_or_none()
    assert org is not None

    result = await db_session.execute(
        select(OrganizationMembership).where(
            OrganizationMembership.user_id == user.id,
            OrganizationMembership.organization_id == org.id
        )
    )
    membership = result.scalar_one_or_none()
    assert membership is not None
    assert membership.role == Role.OWNER


@pytest.mark.asyncio
async def test_auth_login_validates_credentials(api_client: AsyncClient, db_session: AsyncSession) -> None:
    # Seed user and org
    org = Organization(id=uuid.uuid4(), name="Login Org")
    hashed = get_password_hash("loginpassword")
    user = User(id=uuid.uuid4(), email="login@example.com", password_hash=hashed)
    membership = OrganizationMembership(organization_id=org.id, user_id=user.id, role=Role.MEMBER)
    db_session.add_all([org, user, membership])
    await db_session.commit()

    # Success login
    response = await api_client.post(
        "/api/v1/auth/login",
        json={"email": "login@example.com", "password": "loginpassword"}
    )
    assert response.status_code == 200
    assert "access_token" in response.json()

    # Failed login (wrong password)
    response = await api_client.post(
        "/api/v1/auth/login",
        json={"email": "login@example.com", "password": "wrongpassword"}
    )
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


@pytest.mark.asyncio
async def test_user_cannot_read_another_organizations_project(api_client: AsyncClient, db_session: AsyncSession) -> None:
    # Org A setup
    org_a = Organization(id=uuid.uuid4(), name="Org A")
    user_a = User(id=uuid.uuid4(), email="user_a@example.com", password_hash=get_password_hash("password"))
    member_a = OrganizationMembership(organization_id=org_a.id, user_id=user_a.id, role=Role.ADMIN)
    proj_a = Project(id=uuid.uuid4(), organization_id=org_a.id, name="Project A")
    
    # Org B setup
    org_b = Organization(id=uuid.uuid4(), name="Org B")
    user_b = User(id=uuid.uuid4(), email="user_b@example.com", password_hash=get_password_hash("password"))
    member_b = OrganizationMembership(organization_id=org_b.id, user_id=user_b.id, role=Role.ADMIN)
    proj_b = Project(id=uuid.uuid4(), organization_id=org_b.id, name="Project B")

    db_session.add_all([org_a, user_a, member_a, proj_a, org_b, user_b, member_b, proj_b])
    await db_session.commit()

    # Generate tokens (manually, or we can use settings dependency)
    from scheduler_api.config import settings
    token_a = create_access_token(user_id=user_a.id, org_id=org_a.id, role=Role.ADMIN, settings=settings)
    token_b = create_access_token(user_id=user_b.id, org_id=org_b.id, role=Role.ADMIN, settings=settings)

    headers_a = {"Authorization": f"Bearer {token_a}"}
    headers_b = {"Authorization": f"Bearer {token_b}"}

    # Fetch Project A using User A token -> 200
    response = await api_client.get(f"/api/v1/projects/{proj_a.id}", headers=headers_a)
    assert response.status_code == 200
    assert response.json()["name"] == "Project A"

    # Fetch Project A using User B token -> 404 (isolation check)
    response = await api_client.get(f"/api/v1/projects/{proj_a.id}", headers=headers_b)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"
