import pytest
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from scheduler_api.models import Job, Project, Queue, Organization
from scheduler_api.enums import JobStatus


@pytest.fixture
async def setup_test_data(db_session: AsyncSession):
    # This fixture will be expanded as models are defined
    # For now, it provides helper logic
    pass


@pytest.mark.asyncio
async def test_idempotency_key_is_unique_per_project(db_session: AsyncSession) -> None:
    # First we need to insert an organization, project, queue
    org = Organization(id=uuid.uuid4(), name="Test Org")
    db_session.add(org)
    await db_session.flush()

    project = Project(id=uuid.uuid4(), organization_id=org.id, name="Test Project")
    db_session.add(project)
    await db_session.flush()

    queue = Queue(id=uuid.uuid4(), project_id=project.id, name="default", priority=0, concurrency_limit=10)
    db_session.add(queue)
    await db_session.flush()

    job1 = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        queue_id=queue.id,
        idempotency_key="unique_key",
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.QUEUED,
        timeout_seconds=300,
        max_attempts=3,
        attempt_count=0
    )
    db_session.add(job1)
    await db_session.commit()

    # Create job2 with the same project and idempotency key
    job2 = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        queue_id=queue.id,
        idempotency_key="unique_key",
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.QUEUED,
        timeout_seconds=300,
        max_attempts=3,
        attempt_count=0
    )
    db_session.add(job2)
    
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_same_key_is_allowed_in_another_project(db_session: AsyncSession) -> None:
    org = Organization(id=uuid.uuid4(), name="Test Org")
    db_session.add(org)
    await db_session.flush()

    project1 = Project(id=uuid.uuid4(), organization_id=org.id, name="Project 1")
    project2 = Project(id=uuid.uuid4(), organization_id=org.id, name="Project 2")
    db_session.add_all([project1, project2])
    await db_session.flush()

    queue1 = Queue(id=uuid.uuid4(), project_id=project1.id, name="default", priority=0, concurrency_limit=10)
    queue2 = Queue(id=uuid.uuid4(), project_id=project2.id, name="default", priority=0, concurrency_limit=10)
    db_session.add_all([queue1, queue2])
    await db_session.flush()

    job1 = Job(
        id=uuid.uuid4(),
        project_id=project1.id,
        queue_id=queue1.id,
        idempotency_key="same_key",
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.QUEUED,
        timeout_seconds=300,
        max_attempts=3,
        attempt_count=0
    )
    job2 = Job(
        id=uuid.uuid4(),
        project_id=project2.id,
        queue_id=queue2.id,
        idempotency_key="same_key",
        type="noop",
        payload={},
        priority=0,
        status=JobStatus.QUEUED,
        timeout_seconds=300,
        max_attempts=3,
        attempt_count=0
    )
    db_session.add_all([job1, job2])
    await db_session.commit()  # Should succeed without error
