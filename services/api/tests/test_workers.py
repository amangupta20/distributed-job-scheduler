import pytest
import uuid
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from scheduler_api.models import User, Organization, OrganizationMembership, Project, Queue, Job, Worker, WorkerHeartbeat
from scheduler_api.enums import Role, JobStatus, WorkerState, WorkerState as WS
from scheduler_api.security import create_access_token


@pytest.fixture
async def seed_env(db_session: AsyncSession):
    from scheduler_api.config import settings
    org = Organization(id=uuid.uuid4(), name="Worker Org")
    user = User(id=uuid.uuid4(), email="worker_user@example.com", password_hash="hash")
    membership = OrganizationMembership(organization_id=org.id, user_id=user.id, role=Role.ADMIN)
    project = Project(id=uuid.uuid4(), organization_id=org.id, name="Worker Project")
    queue = Queue(id=uuid.uuid4(), project_id=project.id, name="default", priority=0, concurrency_limit=10)
    db_session.add_all([org, user, membership, project, queue])
    await db_session.commit()

    token = create_access_token(user_id=user.id, org_id=org.id, role=Role.ADMIN, settings=settings)
    return project, queue, token


@pytest.mark.asyncio
async def test_worker_registration(api_client: AsyncClient, db_session: AsyncSession, seed_env) -> None:
    _, _, token = seed_env
    headers = {"Authorization": f"Bearer {token}"}
    worker_id = uuid.uuid4()

    # Register worker
    payload = {
        "worker_id": str(worker_id),
        "name": "worker-alpha",
        "version": "1.0.0",
        "concurrency_limit": 5
    }
    response = await api_client.post("/api/v1/workers/register", json=payload, headers=headers)
    assert response.status_code in (200, 201)
    data = response.json()
    assert data["id"] == str(worker_id)
    assert data["status"] == WorkerState.READY
    assert data["concurrency_limit"] == 5

    # Check database
    stmt = select(Worker).where(Worker.id == worker_id)
    res = await db_session.execute(stmt)
    worker = res.scalar_one()
    assert worker.name == "worker-alpha"

    # Idempotent update
    payload["concurrency_limit"] = 10
    response2 = await api_client.post("/api/v1/workers/register", json=payload, headers=headers)
    assert response2.status_code in (200, 201)
    assert response2.json()["concurrency_limit"] == 10

    await db_session.refresh(worker)
    assert worker.capacity == 10


@pytest.mark.asyncio
async def test_worker_heartbeat_cancelled_jobs(api_client: AsyncClient, db_session: AsyncSession, seed_env) -> None:
    project, queue, token = seed_env
    headers = {"Authorization": f"Bearer {token}"}
    worker_id = uuid.uuid4()

    # Create worker first
    worker = Worker(id=worker_id, name="worker-beta", version="1.0.0", capacity=5, status=WorkerState.READY)
    db_session.add(worker)

    # Create a cancelled job claimed by worker
    job_cancelled = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        queue_id=queue.id,
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.CANCELLED,
        timeout_seconds=300,
        max_attempts=3,
        attempt_count=1,
        claimed_by_worker_id=worker_id
    )
    # Create a running job claimed by worker
    job_running = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        queue_id=queue.id,
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.RUNNING,
        timeout_seconds=300,
        max_attempts=3,
        attempt_count=1,
        claimed_by_worker_id=worker_id
    )
    db_session.add_all([job_cancelled, job_running])
    await db_session.commit()

    # Heartbeat reports both running in worker memory
    payload = {
        "running_job_ids": [str(job_cancelled.id), str(job_running.id)]
    }
    response = await api_client.post(f"/api/v1/workers/{worker_id}/heartbeat", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    # Should flag job_cancelled as cancelled
    assert str(job_cancelled.id) in data["cancelled_job_ids"]
    assert str(job_running.id) not in data["cancelled_job_ids"]

    # Verify a new WorkerHeartbeat row is logged in db
    hb_stmt = select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == worker_id)
    hb_res = await db_session.execute(hb_stmt)
    hbs = hb_res.scalars().all()
    assert len(hbs) == 1
    assert hbs[0].status == WorkerState.READY
    assert hbs[0].active_jobs == 2
