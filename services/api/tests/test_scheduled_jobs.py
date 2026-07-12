from datetime import datetime, timezone
import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from scheduler_api.enums import Role
from scheduler_api.models import Organization, OrganizationMembership, Project, Queue, ScheduledJob, User
from scheduler_api.security import create_access_token


@pytest.fixture
async def scheduled_job_environment(db_session: AsyncSession):
    from scheduler_api.config import settings

    org = Organization(id=uuid.uuid4(), name="Schedules Org")
    user = User(id=uuid.uuid4(), email="schedules@example.com", password_hash="hash")
    membership = OrganizationMembership(organization_id=org.id, user_id=user.id, role=Role.ADMIN)
    project = Project(id=uuid.uuid4(), organization_id=org.id, name="Schedules Project")
    queue = Queue(id=uuid.uuid4(), project_id=project.id, name="scheduled-work", priority=0, concurrency_limit=10)

    other_org = Organization(id=uuid.uuid4(), name="Other Schedules Org")
    other_project = Project(id=uuid.uuid4(), organization_id=other_org.id, name="Other Project")
    other_queue = Queue(id=uuid.uuid4(), project_id=other_project.id, name="other-work", priority=0, concurrency_limit=10)

    db_session.add_all([org, user, membership, project, queue, other_org, other_project, other_queue])
    await db_session.commit()
    token = create_access_token(user_id=user.id, org_id=org.id, role=Role.ADMIN, settings=settings)
    return project, queue, other_project, other_queue, {"Authorization": f"Bearer {token}"}


def scheduled_job_payload(project_id: uuid.UUID, queue_id: uuid.UUID) -> dict:
    return {
        "project_id": str(project_id),
        "queue_id": str(queue_id),
        "name": "hourly-health-check",
        "expression": "0 * * * *",
        "job_type": "noop",
        "payload": {"source": "schedule-test"},
        "next_run_at": datetime(2026, 7, 12, 13, 0, tzinfo=timezone.utc).isoformat(),
    }


@pytest.mark.asyncio
async def test_scheduled_job_create_list_and_delete(
    api_client: AsyncClient, scheduled_job_environment
) -> None:
    project, queue, _, _, headers = scheduled_job_environment

    create = await api_client.post(
        "/api/v1/scheduled-jobs",
        json=scheduled_job_payload(project.id, queue.id),
        headers=headers,
    )
    assert create.status_code == 201
    created = create.json()
    assert created["project_id"] == str(project.id)
    assert created["queue_id"] == str(queue.id)
    assert created["expression"] == "0 * * * *"

    listed = await api_client.get(f"/api/v1/scheduled-jobs?project_id={project.id}", headers=headers)
    assert listed.status_code == 200
    assert [item["id"] for item in listed.json()["items"]] == [created["id"]]

    deleted = await api_client.delete(f"/api/v1/scheduled-jobs/{created['id']}", headers=headers)
    assert deleted.status_code == 204

    listed_after_delete = await api_client.get(f"/api/v1/scheduled-jobs?project_id={project.id}", headers=headers)
    assert listed_after_delete.status_code == 200
    assert listed_after_delete.json()["items"] == []


@pytest.mark.asyncio
async def test_scheduled_job_rejects_invalid_cron_expression(
    api_client: AsyncClient, scheduled_job_environment
) -> None:
    project, queue, _, _, headers = scheduled_job_environment
    payload = scheduled_job_payload(project.id, queue.id)
    payload["expression"] = "not a cron"

    response = await api_client.post("/api/v1/scheduled-jobs", json=payload, headers=headers)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_failed"


@pytest.mark.asyncio
async def test_scheduled_job_duplicate_name_returns_conflict_envelope(
    api_client: AsyncClient, scheduled_job_environment
) -> None:
    project, queue, _, _, headers = scheduled_job_environment
    payload = scheduled_job_payload(project.id, queue.id)

    first = await api_client.post("/api/v1/scheduled-jobs", json=payload, headers=headers)
    assert first.status_code == 201

    duplicate = await api_client.post("/api/v1/scheduled-jobs", json=payload, headers=headers)
    assert duplicate.status_code == 409
    assert duplicate.json()["error"]["code"] == "conflict"


@pytest.mark.asyncio
async def test_scheduled_job_rejects_cross_project_queue_and_cross_tenant_access(
    api_client: AsyncClient, db_session: AsyncSession, scheduled_job_environment
) -> None:
    project, queue, other_project, other_queue, headers = scheduled_job_environment

    own_project_list = await api_client.get(f"/api/v1/scheduled-jobs?project_id={project.id}", headers=headers)
    assert own_project_list.status_code == 200

    wrong_queue = await api_client.post(
        "/api/v1/scheduled-jobs",
        json=scheduled_job_payload(project.id, other_queue.id),
        headers=headers,
    )
    assert wrong_queue.status_code == 404
    assert wrong_queue.json()["error"]["code"] == "not_found"

    cross_tenant_project = await api_client.get(
        f"/api/v1/scheduled-jobs?project_id={other_project.id}", headers=headers
    )
    assert cross_tenant_project.status_code == 404
    assert cross_tenant_project.json()["error"]["code"] == "not_found"

    cross_tenant_create = await api_client.post(
        "/api/v1/scheduled-jobs",
        json=scheduled_job_payload(other_project.id, other_queue.id),
        headers=headers,
    )
    assert cross_tenant_create.status_code == 404
    assert cross_tenant_create.json()["error"]["code"] == "not_found"

    foreign_schedule = ScheduledJob(
        id=uuid.uuid4(),
        project_id=other_project.id,
        queue_id=other_queue.id,
        name="foreign-schedule",
        expression="0 * * * *",
        job_type="noop",
        payload={},
        next_run_at=datetime(2026, 7, 12, 13, 0, tzinfo=timezone.utc),
    )
    db_session.add(foreign_schedule)
    await db_session.commit()
    cross_tenant_delete = await api_client.delete(f"/api/v1/scheduled-jobs/{foreign_schedule.id}", headers=headers)
    assert cross_tenant_delete.status_code == 404
    assert cross_tenant_delete.json()["error"]["code"] == "not_found"
