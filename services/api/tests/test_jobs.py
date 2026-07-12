import pytest
import uuid
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from scheduler_api.models import User, Organization, OrganizationMembership, Project, Queue, Job, DeadLetterEntry
from scheduler_api.enums import Role, JobStatus
from scheduler_api.security import create_access_token


@pytest.fixture
async def seed_env(db_session: AsyncSession):
    from scheduler_api.config import settings
    org = Organization(id=uuid.uuid4(), name="Job Org")
    user = User(id=uuid.uuid4(), email="job_user@example.com", password_hash="hash")
    membership = OrganizationMembership(organization_id=org.id, user_id=user.id, role=Role.ADMIN)
    project = Project(id=uuid.uuid4(), organization_id=org.id, name="Job Project")
    queue = Queue(id=uuid.uuid4(), project_id=project.id, name="default", priority=0, concurrency_limit=10)
    db_session.add_all([org, user, membership, project, queue])
    await db_session.commit()

    token = create_access_token(user_id=user.id, org_id=org.id, role=Role.ADMIN, settings=settings)
    return project, queue, token


@pytest.mark.asyncio
async def test_job_creation_payload_validation(api_client: AsyncClient, seed_env) -> None:
    project, queue, token = seed_env
    headers = {"Authorization": f"Bearer {token}"}

    # Invalid type
    payload = {
        "project_id": str(project.id),
        "queue_id": str(queue.id),
        "type": "shell",
        "payload": {"cmd": "rm -rf /"}
    }
    response = await api_client.post("/api/v1/jobs", json=payload, headers=headers)
    assert response.status_code == 422

    # Invalid http URL
    payload = {
        "project_id": str(project.id),
        "queue_id": str(queue.id),
        "type": "http",
        "payload": {"method": "GET"}  # missing url
    }
    response = await api_client.post("/api/v1/jobs", json=payload, headers=headers)
    assert response.status_code == 422

    # Success noop job
    payload = {
        "project_id": str(project.id),
        "queue_id": str(queue.id),
        "type": "noop",
        "payload": {}
    }
    response = await api_client.post("/api/v1/jobs", json=payload, headers=headers)
    assert response.status_code == 201
    assert response.json()["type"] == "noop"


@pytest.mark.asyncio
async def test_job_creation_idempotency(api_client: AsyncClient, seed_env) -> None:
    project, queue, token = seed_env
    headers = {"Authorization": f"Bearer {token}"}
    key = "idem-key-1"

    payload = {
        "project_id": str(project.id),
        "queue_id": str(queue.id),
        "type": "noop",
        "payload": {},
        "idempotency_key": key
    }
    # Create first time
    response1 = await api_client.post("/api/v1/jobs", json=payload, headers=headers)
    assert response1.status_code in (200, 201)
    job1_id = response1.json()["id"]

    # Create second time with exact same parameters
    response2 = await api_client.post("/api/v1/jobs", json=payload, headers=headers)
    assert response2.status_code in (200, 201)
    assert response2.json()["id"] == job1_id

    # Create with conflicting payload
    payload_conflicting = payload.copy()
    payload_conflicting["payload"] = {"conflict": True}
    response3 = await api_client.post("/api/v1/jobs", json=payload_conflicting, headers=headers)
    assert response3.status_code == 409
    assert response3.json()["error"]["code"] == "conflict"


@pytest.mark.asyncio
async def test_job_cancellation(api_client: AsyncClient, db_session: AsyncSession, seed_env) -> None:
    project, queue, token = seed_env
    headers = {"Authorization": f"Bearer {token}"}

    job = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        queue_id=queue.id,
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.QUEUED,
        timeout_seconds=300,
        max_attempts=3,
        attempt_count=0
    )
    db_session.add(job)
    await db_session.commit()

    response = await api_client.post(f"/api/v1/jobs/{job.id}/cancel", headers=headers)
    assert response.status_code == 200
    assert response.json()["status"] == JobStatus.CANCELLED


@pytest.mark.asyncio
async def test_job_dlq_replay(api_client: AsyncClient, db_session: AsyncSession, seed_env) -> None:
    project, queue, token = seed_env
    headers = {"Authorization": f"Bearer {token}"}

    job = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        queue_id=queue.id,
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.DEAD_LETTERED,
        timeout_seconds=300,
        max_attempts=3,
        attempt_count=3
    )
    dlq_entry = DeadLetterEntry(
        id=uuid.uuid4(),
        job_id=job.id,
        reason="failed permanently"
    )
    db_session.add_all([job, dlq_entry])
    await db_session.commit()

    response = await api_client.post(f"/api/v1/jobs/{job.id}/replay-dlq", headers=headers)
    assert response.status_code == 201
    new_job_data = response.json()
    assert new_job_data["status"] == JobStatus.QUEUED
    assert uuid.UUID(new_job_data["id"]) != job.id

    # Verify DLQ entry update
    await db_session.refresh(dlq_entry)
    assert dlq_entry.replayed_at is not None
    assert dlq_entry.replacement_job_id == uuid.UUID(new_job_data["id"])


@pytest.mark.asyncio
async def test_job_pagination(api_client: AsyncClient, db_session: AsyncSession, seed_env) -> None:
    project, queue, token = seed_env
    headers = {"Authorization": f"Bearer {token}"}

    # Create 3 jobs
    jobs = []
    for i in range(3):
        job = Job(
            id=uuid.uuid4(),
            project_id=project.id,
            queue_id=queue.id,
            type="noop",
            payload={"index": i},
            priority=0,
            status=JobStatus.QUEUED,
            timeout_seconds=300,
            max_attempts=3,
            attempt_count=0
        )
        jobs.append(job)
        db_session.add(job)
    await db_session.commit()

    # Get page 1 with limit=2
    response1 = await api_client.get(f"/api/v1/jobs?limit=2&project_id={project.id}", headers=headers)
    assert response1.status_code == 200
    data1 = response1.json()
    assert len(data1["items"]) == 2
    assert data1["next_cursor"] is not None

    # Get page 2 with cursor
    cursor = data1["next_cursor"]
    response2 = await api_client.get(f"/api/v1/jobs?limit=2&cursor={cursor}&project_id={project.id}", headers=headers)
    assert response2.status_code == 200
    data2 = response2.json()
    assert len(data2["items"]) == 1
    assert data2["next_cursor"] is None

