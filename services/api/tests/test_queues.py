import pytest
import uuid
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from scheduler_api.models import User, Organization, OrganizationMembership, Project, Queue, RetryPolicy
from scheduler_api.enums import Role, RetryStrategy
from scheduler_api.security import create_access_token


@pytest.fixture
async def seeded_user_org_proj_token(db_session: AsyncSession):
    from scheduler_api.config import settings
    org = Organization(id=uuid.uuid4(), name="Queue Org")
    user = User(id=uuid.uuid4(), email="queue_user@example.com", password_hash="hash")
    membership = OrganizationMembership(organization_id=org.id, user_id=user.id, role=Role.ADMIN)
    project = Project(id=uuid.uuid4(), organization_id=org.id, name="Queue Project")
    db_session.add_all([org, user, membership, project])
    await db_session.commit()

    token = create_access_token(user_id=user.id, org_id=org.id, role=Role.ADMIN, settings=settings)
    return project, token


@pytest.mark.asyncio
async def test_queue_creation_validation(api_client: AsyncClient, seeded_user_org_proj_token) -> None:
    project, token = seeded_user_org_proj_token
    headers = {"Authorization": f"Bearer {token}"}

    # Invalid name (must start with letter/number, contain only alphanumeric/dash/underscore)
    payload = {
        "name": "-invalid-name",
        "project_id": str(project.id),
        "priority": 0,
        "concurrency_limit": 10
    }
    response = await api_client.post("/api/v1/queues", json=payload, headers=headers)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_failed"

    # Invalid priority
    payload = {
        "name": "valid-name",
        "project_id": str(project.id),
        "priority": 200,
        "concurrency_limit": 10
    }
    response = await api_client.post("/api/v1/queues", json=payload, headers=headers)
    assert response.status_code == 422

    # Success queue creation
    payload = {
        "name": "valid-name",
        "project_id": str(project.id),
        "priority": 10,
        "concurrency_limit": 50,
        "rate_limit_per_minute": 100
    }
    response = await api_client.post("/api/v1/queues", json=payload, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "valid-name"
    assert data["priority"] == 10
    assert data["concurrency_limit"] == 50
    assert data["rate_limit_per_minute"] == 100


@pytest.mark.asyncio
async def test_queue_pause_and_resume(api_client: AsyncClient, db_session: AsyncSession, seeded_user_org_proj_token) -> None:
    project, token = seeded_user_org_proj_token
    headers = {"Authorization": f"Bearer {token}"}

    queue = Queue(id=uuid.uuid4(), project_id=project.id, name="pause-test-queue", priority=0, concurrency_limit=10)
    db_session.add(queue)
    await db_session.commit()

    # Pause
    response = await api_client.post(f"/api/v1/queues/{queue.id}/pause", headers=headers)
    assert response.status_code == 200
    assert response.json()["pause_state"] is True

    # Resume
    response = await api_client.post(f"/api/v1/queues/{queue.id}/resume", headers=headers)
    assert response.status_code == 200
    assert response.json()["pause_state"] is False
