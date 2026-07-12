from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4
from sqlalchemy import ForeignKey, Index, String, Text, DateTime, Boolean, Integer, UniqueConstraint, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from scheduler_api.enums import JobStatus, RetryStrategy, WorkerState, Role


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    memberships: Mapped[List["OrganizationMembership"]] = relationship(back_populates="user", cascade="all, delete-orphan")


class Organization(Base):
    __tablename__ = "organizations"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    memberships: Mapped[List["OrganizationMembership"]] = relationship(back_populates="organization", cascade="all, delete-orphan")
    projects: Mapped[List["Project"]] = relationship(back_populates="organization", cascade="all, delete-orphan")


class OrganizationMembership(Base):
    __tablename__ = "organization_memberships"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    organization_id: Mapped[UUID] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id: Mapped[UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    role: Mapped[Role] = mapped_column(String(50), nullable=False, default=Role.MEMBER)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    user: Mapped["User"] = relationship(back_populates="memberships")
    organization: Mapped["Organization"] = relationship(back_populates="memberships")

    __table_args__ = (
        UniqueConstraint("organization_id", "user_id", name="uq_org_membership_user"),
    )


class Project(Base):
    __tablename__ = "projects"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    organization_id: Mapped[UUID] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    organization: Mapped["Organization"] = relationship(back_populates="projects")
    queues: Mapped[List["Queue"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    retry_policies: Mapped[List["RetryPolicy"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    job_batches: Mapped[List["JobBatch"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    jobs: Mapped[List["Job"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    scheduled_jobs: Mapped[List["ScheduledJob"]] = relationship(back_populates="project", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("organization_id", "name", name="uq_project_org_name"),
    )


class Queue(Base):
    __tablename__ = "queues"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    project_id: Mapped[UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    priority: Mapped[int] = mapped_column(default=0, nullable=False)
    concurrency_limit: Mapped[int] = mapped_column(default=10, nullable=False)
    rate_limit_per_minute: Mapped[Optional[int]] = mapped_column(nullable=True)
    rate_tokens: Mapped[Optional[int]] = mapped_column(nullable=True)
    rate_refilled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    pause_state: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    retry_policy_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("retry_policies.id", ondelete="SET NULL"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    project: Mapped["Project"] = relationship(back_populates="queues")
    retry_policy: Mapped[Optional["RetryPolicy"]] = relationship()
    jobs: Mapped[List["Job"]] = relationship(back_populates="queue", cascade="all, delete-orphan")
    scheduled_jobs: Mapped[List["ScheduledJob"]] = relationship(back_populates="queue", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("project_id", "name", name="uq_queue_project_name"),
    )


class RetryPolicy(Base):
    __tablename__ = "retry_policies"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    project_id: Mapped[UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    strategy: Mapped[RetryStrategy] = mapped_column(String(50), nullable=False, default=RetryStrategy.FIXED)
    base_delay_seconds: Mapped[int] = mapped_column(default=10, nullable=False)
    max_delay_seconds: Mapped[int] = mapped_column(default=3600, nullable=False)
    max_attempts: Mapped[int] = mapped_column(default=3, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    project: Mapped["Project"] = relationship(back_populates="retry_policies")

    __table_args__ = (
        UniqueConstraint("project_id", "name", name="uq_retry_policy_project_name"),
        CheckConstraint("base_delay_seconds > 0", name="chk_retry_policy_base_delay"),
        CheckConstraint("max_delay_seconds >= base_delay_seconds", name="chk_retry_policy_max_delay"),
        CheckConstraint("max_attempts >= 1", name="chk_retry_policy_max_attempts"),
    )


class JobBatch(Base):
    __tablename__ = "job_batches"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    project_id: Mapped[UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)
    expected_count: Mapped[int] = mapped_column(default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    project: Mapped["Project"] = relationship(back_populates="job_batches")
    jobs: Mapped[List["Job"]] = relationship(back_populates="batch", cascade="all, delete-orphan")


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    project_id: Mapped[UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    queue_id: Mapped[UUID] = mapped_column(ForeignKey("queues.id", ondelete="CASCADE"), nullable=False, index=True)
    retry_policy_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("retry_policies.id", ondelete="SET NULL"), nullable=True, index=True)
    batch_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("job_batches.id", ondelete="SET NULL"), nullable=True, index=True)
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    priority: Mapped[int] = mapped_column(default=0, nullable=False)
    status: Mapped[JobStatus] = mapped_column(String(50), default=JobStatus.QUEUED, nullable=False, index=True)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True)
    timeout_seconds: Mapped[int] = mapped_column(default=300, nullable=False)
    attempt_count: Mapped[int] = mapped_column(default=0, nullable=False)
    max_attempts: Mapped[int] = mapped_column(default=3, nullable=False)
    claimed_by_worker_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("workers.id", ondelete="SET NULL"), nullable=True, index=True)
    lease_token: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    project: Mapped["Project"] = relationship(back_populates="jobs")
    queue: Mapped["Queue"] = relationship(back_populates="jobs")
    batch: Mapped[Optional["JobBatch"]] = relationship(back_populates="jobs")
    worker: Mapped[Optional["Worker"]] = relationship()

    executions: Mapped[List["JobExecution"]] = relationship(back_populates="job", cascade="all, delete-orphan")
    events: Mapped[List["JobStateEvent"]] = relationship(back_populates="job", cascade="all, delete-orphan")
    logs: Mapped[List["JobLog"]] = relationship(back_populates="job", cascade="all, delete-orphan")
    dead_letter_entry: Mapped[Optional["DeadLetterEntry"]] = relationship(back_populates="job", foreign_keys="[DeadLetterEntry.job_id]", cascade="all, delete-orphan", uselist=False)

    __table_args__ = (
        UniqueConstraint("project_id", "idempotency_key", name="uq_jobs_project_idempotency"),
        CheckConstraint("timeout_seconds >= 1 AND timeout_seconds <= 86400", name="chk_jobs_timeout_seconds"),
    )


class ScheduledJob(Base):
    __tablename__ = "scheduled_jobs"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    project_id: Mapped[UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    queue_id: Mapped[UUID] = mapped_column(ForeignKey("queues.id", ondelete="CASCADE"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    expression: Mapped[str] = mapped_column(String(100), nullable=False)
    next_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    project: Mapped["Project"] = relationship(back_populates="scheduled_jobs")
    queue: Mapped["Queue"] = relationship(back_populates="scheduled_jobs")

    __table_args__ = (
        UniqueConstraint("project_id", "name", name="uq_scheduled_job_project_name"),
    )


class Worker(Base):
    __tablename__ = "workers"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    version: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[WorkerState] = mapped_column(String(50), default=WorkerState.STARTING, nullable=False)
    capacity: Mapped[int] = mapped_column(default=10, nullable=False)
    active_jobs: Mapped[int] = mapped_column(default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    heartbeats: Mapped[List["WorkerHeartbeat"]] = relationship(back_populates="worker", cascade="all, delete-orphan")


class WorkerHeartbeat(Base):
    __tablename__ = "worker_heartbeats"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    worker_id: Mapped[UUID] = mapped_column(ForeignKey("workers.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    active_jobs: Mapped[int] = mapped_column(default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True)

    worker: Mapped["Worker"] = relationship(back_populates="heartbeats")


class JobExecution(Base):
    __tablename__ = "job_executions"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    worker_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("workers.id", ondelete="SET NULL"), nullable=True, index=True)
    attempt_number: Mapped[int] = mapped_column(nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    logs: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(JSONB, nullable=True)

    job: Mapped["Job"] = relationship(back_populates="executions")
    worker: Mapped[Optional["Worker"]] = relationship()

    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="uq_job_execution_attempt"),
    )


class JobStateEvent(Base):
    __tablename__ = "job_state_events"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    from_status: Mapped[Optional[JobStatus]] = mapped_column(String(50), nullable=True)
    to_status: Mapped[JobStatus] = mapped_column(String(50), nullable=False)
    worker_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("workers.id", ondelete="SET NULL"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True)

    job: Mapped["Job"] = relationship(back_populates="events")
    worker: Mapped[Optional["Worker"]] = relationship()


class JobLog(Base):
    __tablename__ = "job_logs"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    execution_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("job_executions.id", ondelete="SET NULL"), nullable=True, index=True)
    level: Mapped[str] = mapped_column(String(20), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    job: Mapped["Job"] = relationship(back_populates="logs")
    execution: Mapped[Optional["JobExecution"]] = relationship()


class DeadLetterEntry(Base):
    __tablename__ = "dead_letter_entries"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    details: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    replayed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    replayed_from_job_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True, index=True)
    replacement_job_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    job: Mapped["Job"] = relationship(back_populates="dead_letter_entry", foreign_keys=[job_id])
    replayed_from_job: Mapped[Optional["Job"]] = relationship(foreign_keys=[replayed_from_job_id])
    replacement_job: Mapped[Optional["Job"]] = relationship(foreign_keys=[replacement_job_id])
