# Control Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the PostgreSQL schema, authenticated FastAPI control plane, job APIs, scheduling, retries, DLQ operations, and lease recovery.

**Architecture:** SQLAlchemy models and services hold domain behavior behind focused repositories. FastAPI routes validate and authorize requests, while a separate scheduler process promotes due work and reclaims expired leases. All state transitions and audit events commit atomically.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2, Alembic, Pydantic 2, psycopg 3, PyJWT, pwdlib/Argon2, croniter, pytest, pytest-asyncio, and Testcontainers PostgreSQL.

## Global Constraints

- Own only `services/api/**`, `services/scheduler/**`, and `packages/contracts/**`.
- Use PostgreSQL-specific integration tests for locking, constraints, notifications, and JSONB behavior.
- Use UTC-aware timestamps and server-side `now()` defaults.
- All tenant resources require organization membership authorization.
- Every state transition writes `job_state_events` in the same transaction.
- Reusing an idempotency key with the same payload returns the existing job; a different payload returns `409`.
- The scheduler is safe to run in multiple replicas.
- Do not edit root `README.md`; report the exact README changes required to root after each task.

## File Map

```text
services/api/
  pyproject.toml                   Python dependencies and test configuration
  alembic.ini                     Migration runner configuration
  migrations/env.py              Async/sync migration metadata wiring
  migrations/versions/0001_initial.py
  src/scheduler_api/main.py       FastAPI application factory
  src/scheduler_api/config.py     Environment settings
  src/scheduler_api/db.py         Engine and session lifecycle
  src/scheduler_api/models.py     Relational mappings only
  src/scheduler_api/enums.py      Job, worker, retry, and role enums
  src/scheduler_api/errors.py     Stable API error envelope
  src/scheduler_api/security.py   Password and JWT primitives
  src/scheduler_api/deps.py       Sessions and authorization dependencies
  src/scheduler_api/schemas/      Pydantic request/response types
  src/scheduler_api/repositories/ Focused SQL operations
  src/scheduler_api/services/     Domain transactions and calculations
  src/scheduler_api/routes/       Auth, projects, queues, jobs, workers, metrics
  tests/                          API and PostgreSQL integration tests
services/scheduler/
  src/scheduler_service/main.py   Poll loop and signal handling
  src/scheduler_service/tick.py   One deterministic scheduling iteration
  tests/test_tick.py
packages/contracts/
  job_states.json
  openapi.json
```

### Task 1: Python service skeleton and error contract

**Files:**
- Create: `services/api/pyproject.toml`
- Create: `services/api/src/scheduler_api/config.py`
- Create: `services/api/src/scheduler_api/db.py`
- Create: `services/api/src/scheduler_api/errors.py`
- Create: `services/api/src/scheduler_api/main.py`
- Create: `services/api/tests/test_health.py`

**Interfaces:**
- Produces: `create_app() -> FastAPI`
- Produces: `Settings` with `database_url`, `jwt_secret`, and `api_port`
- Produces: error body `{"error":{"code":str,"message":str,"details":object|null,"trace_id":str}}`

- [ ] **Step 1: Write the failing health and error-envelope tests**

```python
from fastapi.testclient import TestClient
from scheduler_api.main import create_app


def test_liveness_has_stable_shape() -> None:
    response = TestClient(create_app()).get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


def test_unknown_route_uses_error_envelope() -> None:
    response = TestClient(create_app()).get("/does-not-exist")
    body = response.json()["error"]
    assert response.status_code == 404
    assert body["code"] == "not_found"
    assert body["trace_id"]
```

- [ ] **Step 2: Run the tests and verify collection fails**

Run: `cd services/api && python -m pytest tests/test_health.py -v`

Expected: FAIL because `scheduler_api.main` does not exist.

- [ ] **Step 3: Implement the application factory, trace middleware, and exception handlers**

```python
def create_app() -> FastAPI:
    app = FastAPI(title="PulseQueue API", version="0.1.0")
    app.add_middleware(TraceIdMiddleware)
    install_error_handlers(app)

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "alive"}

    return app
```

Implement `TraceIdMiddleware` to accept `x-trace-id` or create a UUID, return it as a response header, and place it on `request.state.trace_id`. Convert Starlette 404s, validation failures, and explicit `DomainError` instances into the documented envelope.

- [ ] **Step 4: Add readiness with a real `SELECT 1` database probe**

```python
@router.get("/health/ready")
async def ready(session: AsyncSession = Depends(get_session)) -> dict[str, str]:
    await session.execute(text("SELECT 1"))
    return {"status": "ready"}
```

- [ ] **Step 5: Run focused tests and report README changes**

Run: `cd services/api && python -m pytest tests/test_health.py -v`

Expected: PASS. Report `/health/live`, `/health/ready`, and Python setup commands to root.

- [ ] **Step 6: Root updates README and commits the verified foundation**

```bash
git add services/api README.md
git commit -m "feat: add FastAPI service foundation"
```

### Task 2: Relational schema and initial migration

**Files:**
- Create: `services/api/src/scheduler_api/enums.py`
- Create: `services/api/src/scheduler_api/models.py`
- Create: `services/api/migrations/env.py`
- Create: `services/api/migrations/versions/0001_initial.py`
- Create: `services/api/tests/test_schema.py`
- Create: `packages/contracts/job_states.json`

**Interfaces:**
- Produces: canonical SQLAlchemy models listed in design section 6
- Produces: PostgreSQL enum `job_status`
- Produces: partial index `ix_jobs_claimable`
- Produces: unique constraints `uq_jobs_project_idempotency` and `uq_scheduled_occurrence`

- [ ] **Step 1: Write schema tests against PostgreSQL**

```python
async def test_idempotency_key_is_unique_per_project(session, job_factory):
    await job_factory(project_id="p1", idempotency_key="same")
    await session.commit()
    await job_factory(project_id="p1", idempotency_key="same")
    with pytest.raises(IntegrityError):
        await session.commit()


async def test_same_key_is_allowed_in_another_project(session, job_factory):
    await job_factory(project_id="p1", idempotency_key="same")
    await job_factory(project_id="p2", idempotency_key="same")
    await session.commit()
```

Also assert that every required table exists and that `ix_jobs_claimable` is partial on eligible states.

- [ ] **Step 2: Run the schema tests before the migration exists**

Run: `cd services/api && python -m pytest tests/test_schema.py -v`

Expected: FAIL because the initial migration and models do not exist.

- [ ] **Step 3: Define canonical enums and focused SQLAlchemy mappings**

```python
class JobStatus(StrEnum):
    QUEUED = "queued"
    SCHEDULED = "scheduled"
    CLAIMED = "claimed"
    RUNNING = "running"
    RETRY_SCHEDULED = "retry_scheduled"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    DEAD_LETTERED = "dead_lettered"


class RetryStrategy(StrEnum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
```

Split mappings into focused classes while preserving one metadata object. Add explicit foreign keys, named constraints, UTC timestamps, JSONB payloads, and indexes matching the design.

The `queues` table stores `rate_limit_per_minute`, `rate_tokens`, and `rate_refilled_at`. New or unlimited queues initialize tokens to null; limited queues initialize tokens to their configured per-minute capacity. These fields are updated only inside the worker claim transaction.

The `jobs` table stores `timeout_seconds` with a positive check constraint. API inputs default it to 300 seconds and accept values from 1 through 86,400 seconds.

- [ ] **Step 4: Write the initial Alembic migration explicitly**

The migration must create all tables in dependency order and include this claim index shape:

```sql
CREATE INDEX ix_jobs_claimable
ON jobs (queue_id, priority DESC, scheduled_at, id)
WHERE status IN ('queued', 'retry_scheduled')
  AND lease_expires_at IS NULL;
```

Add an index on `jobs(lease_expires_at)` for claimed/running rows, `scheduled_jobs(next_run_at)`, `job_executions(job_id, attempt_number)`, and `worker_heartbeats(worker_id, created_at DESC)`.

- [ ] **Step 5: Verify upgrade and downgrade on a clean database**

Run:

```bash
cd services/api
alembic upgrade head
python -m pytest tests/test_schema.py -v
alembic downgrade base
alembic upgrade head
```

Expected: all tests pass and both migration directions exit zero.

- [ ] **Step 6: Root updates the schema documentation and commits**

```bash
git add services/api packages/contracts README.md
git commit -m "feat: add scheduler relational schema"
```

### Task 3: Authentication, tenancy, projects, and queues

**Files:**
- Create: `services/api/src/scheduler_api/security.py`
- Create: `services/api/src/scheduler_api/deps.py`
- Create: `services/api/src/scheduler_api/schemas/auth.py`
- Create: `services/api/src/scheduler_api/schemas/projects.py`
- Create: `services/api/src/scheduler_api/schemas/queues.py`
- Create: `services/api/src/scheduler_api/schemas/retry_policies.py`
- Create: `services/api/src/scheduler_api/routes/auth.py`
- Create: `services/api/src/scheduler_api/routes/projects.py`
- Create: `services/api/src/scheduler_api/routes/queues.py`
- Create: `services/api/src/scheduler_api/routes/retry_policies.py`
- Create: `services/api/tests/test_auth_and_tenancy.py`
- Create: `services/api/tests/test_queues.py`

**Interfaces:**
- Produces: `POST /api/v1/auth/register`, `POST /api/v1/auth/login`
- Produces: bearer JWT containing `sub`, `org_id`, `role`, `iat`, and `exp`
- Produces: project and queue CRUD with organization isolation
- Produces: retry-policy CRUD with strategy-specific validation

- [ ] **Step 1: Write failing registration, login, and isolation tests**

```python
async def test_user_cannot_read_another_organizations_project(client, users):
    owner_token, outsider_token, project_id = await users.two_orgs_with_project()
    response = await client.get(
        f"/api/v1/projects/{project_id}",
        headers={"Authorization": f"Bearer {outsider_token}"},
    )
    assert response.status_code == 404
```

Use 404 rather than 403 for cross-tenant resource lookup to avoid exposing resource existence.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `cd services/api && python -m pytest tests/test_auth_and_tenancy.py tests/test_queues.py -v`

Expected: FAIL because routes do not exist.

- [ ] **Step 3: Implement password hashing, JWTs, and authorization dependencies**

```python
def create_access_token(*, user_id: UUID, org_id: UUID, role: Role, settings: Settings) -> str:
    now = datetime.now(UTC)
    return jwt.encode(
        {"sub": str(user_id), "org_id": str(org_id), "role": role.value,
         "iat": now, "exp": now + timedelta(hours=1)},
        settings.jwt_secret,
        algorithm="HS256",
    )
```

`get_current_membership` must verify the user still has an active membership instead of trusting only the JWT role claim.

- [ ] **Step 4: Implement project, retry-policy, and queue transactions**

Queue inputs must validate:

```python
class QueueCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9_-]*$")
    priority: int = Field(default=0, ge=-100, le=100)
    concurrency_limit: int = Field(default=10, ge=1, le=1000)
    rate_limit_per_minute: int | None = Field(default=None, ge=1, le=1_000_000)
    retry_policy_id: UUID | None = None
```

Pause and resume are explicit endpoints that create audit events and are idempotent.

Retry policy validation requires positive base delay, a maximum delay greater than or equal to the base delay, at least one attempt, and a strategy from `fixed`, `linear`, or `exponential`.

- [ ] **Step 5: Run tests and export the intermediate OpenAPI document**

Run:

```bash
cd services/api
python -m pytest tests/test_auth_and_tenancy.py tests/test_queues.py -v
python -c 'import json; from scheduler_api.main import create_app; print(json.dumps(create_app().openapi(), indent=2))' > ../../packages/contracts/openapi.json
```

Expected: tests pass and `openapi.json` parses as JSON.

- [ ] **Step 6: Root updates README and commits authenticated configuration APIs**

```bash
git add services/api packages/contracts README.md
git commit -m "feat: add authenticated project and queue APIs"
```

### Task 4: Job creation, listing, lifecycle actions, and DLQ

**Files:**
- Create: `services/api/src/scheduler_api/schemas/jobs.py`
- Create: `services/api/src/scheduler_api/repositories/jobs.py`
- Create: `services/api/src/scheduler_api/services/jobs.py`
- Create: `services/api/src/scheduler_api/services/retries.py`
- Create: `services/api/src/scheduler_api/routes/jobs.py`
- Create: `services/api/tests/test_jobs.py`
- Create: `services/api/tests/test_retries.py`

**Interfaces:**
- Produces: immediate, delayed, cron, and batch creation endpoints
- Produces: cursor pagination ordered by `(created_at DESC, id DESC)`
- Produces: retry, cancel, and DLQ replay operations
- Produces: `calculate_retry_at(strategy, base_delay_seconds, attempt, now)`

- [ ] **Step 1: Write failing retry calculation tests**

```python
@pytest.mark.parametrize(("strategy", "attempt", "seconds"), [
    (RetryStrategy.FIXED, 3, 10),
    (RetryStrategy.LINEAR, 3, 30),
    (RetryStrategy.EXPONENTIAL, 3, 40),
])
def test_retry_delay(strategy, attempt, seconds):
    now = datetime(2026, 7, 12, tzinfo=UTC)
    assert calculate_retry_at(strategy, 10, attempt, now) == now + timedelta(seconds=seconds)
```

The exponent uses `base * 2 ** (attempt - 1)` with attempt one as the first retry.

- [ ] **Step 2: Write failing idempotency and lifecycle tests**

Cover equivalent replay, conflicting payload, delayed state, unknown handler rejection, batch membership, cancellation conflict, and DLQ replay linkage.

Job creation accepts only `noop`, `http`, and `chaos` handler types and validates `timeout_seconds` from 1 through 86,400. HTTP payloads require an `http` or `https` URL and an allowed method.

- [ ] **Step 3: Run focused tests and verify failure**

Run: `cd services/api && python -m pytest tests/test_jobs.py tests/test_retries.py -v`

Expected: FAIL because job services and routes do not exist.

- [ ] **Step 4: Implement request hashing and atomic job creation**

```python
def canonical_request_hash(job_type: str, payload: dict, queue_id: UUID, scheduled_at: datetime) -> str:
    value = json.dumps(
        {"type": job_type, "payload": payload, "queue_id": str(queue_id),
         "scheduled_at": scheduled_at.isoformat()},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(value.encode()).hexdigest()
```

Insert the job and `job_state_events` record together. Execute `SELECT pg_notify('jobs_available', queue_id::text)` in the same transaction for immediately eligible jobs.

- [ ] **Step 5: Implement filtered cursor listing and lifecycle actions**

Filters are `project_id`, `queue_id`, `status`, `worker_id`, `created_after`, and `created_before`. Cursor content is an opaque URL-safe encoding of `created_at` and `id`; limit is 1 through 100.

Retrying a dead-lettered job creates a new queued job linked by `replayed_from_job_id` and marks the DLQ entry with replay timestamp and replacement job ID.

- [ ] **Step 6: Run tests and regenerate OpenAPI**

Run: `cd services/api && python -m pytest tests/test_jobs.py tests/test_retries.py -v`

Expected: PASS. Regenerate `packages/contracts/openapi.json` and report all new endpoints to root.

- [ ] **Step 7: Root updates README and commits lifecycle APIs**

```bash
git add services/api packages/contracts README.md
git commit -m "feat: add job lifecycle APIs"
```

### Task 5: Scheduler ticks, cron materialization, and lease recovery

**Files:**
- Create: `services/scheduler/pyproject.toml`
- Create: `services/scheduler/src/scheduler_service/tick.py`
- Create: `services/scheduler/src/scheduler_service/main.py`
- Create: `services/scheduler/tests/test_tick.py`
- Create: `services/api/src/scheduler_api/repositories/scheduling.py`

**Interfaces:**
- Produces: `run_tick(session, now, batch_size) -> TickResult`
- Produces: deterministic cron key `scheduled_job_id:occurrence_utc_iso`
- Produces: expired lease transition back to `retry_scheduled` or `dead_lettered`

- [ ] **Step 1: Write failing deterministic tick tests**

```python
async def test_two_ticks_materialize_one_cron_occurrence(session, cron_job):
    now = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
    await asyncio.gather(
        run_tick(session_factory(), now, 100),
        run_tick(session_factory(), now, 100),
    )
    jobs = await jobs_for_occurrence(cron_job.id, now)
    assert len(jobs) == 1
```

Also test due delayed promotion, retry promotion, expired running lease recovery, stale-token invalidation, and max-attempt DLQ placement.

- [ ] **Step 2: Run tests and verify failure**

Run: `cd services/scheduler && python -m pytest tests/test_tick.py -v`

Expected: FAIL because `run_tick` does not exist.

- [ ] **Step 3: Implement one bounded, deterministic scheduler tick**

Use short transactions and `FOR UPDATE SKIP LOCKED` on due scheduled definitions and expired leases. The tick order is:

```python
async def run_tick(session_factory, now: datetime, batch_size: int) -> TickResult:
    cron_count = await materialize_cron(session_factory, now, batch_size)
    delayed_count = await promote_due_jobs(session_factory, now, batch_size)
    recovered_count = await recover_expired_leases(session_factory, now, batch_size)
    return TickResult(cron=cron_count, promoted=delayed_count, recovered=recovered_count)
```

Each helper creates state events in its state-change transaction and notifies workers after making jobs eligible.

- [ ] **Step 4: Implement the poll loop and graceful shutdown**

`SIGTERM` and `SIGINT` set an asyncio event. The process stops starting new ticks, waits for the active tick, closes the engine, and exits zero.

- [ ] **Step 5: Run all control-plane tests and report verification**

Run:

```bash
cd services/api && python -m pytest -v
cd ../scheduler && python -m pytest -v
```

Expected: all tests pass. Report migration, API, scheduler, and README contract changes to root.

- [ ] **Step 6: Root updates README and commits scheduling behavior**

```bash
git add services/api services/scheduler README.md
git commit -m "feat: add scheduling and lease recovery"
```

### Task 6: Operational read APIs, metrics, logs, and service health

**Files:**
- Create: `services/api/src/scheduler_api/schemas/workers.py`
- Create: `services/api/src/scheduler_api/schemas/metrics.py`
- Create: `services/api/src/scheduler_api/repositories/operations.py`
- Create: `services/api/src/scheduler_api/routes/workers.py`
- Create: `services/api/src/scheduler_api/routes/metrics.py`
- Create: `services/api/src/scheduler_api/observability.py`
- Create: `services/api/tests/test_operations.py`
- Modify: `services/api/src/scheduler_api/routes/jobs.py`
- Modify: `services/api/src/scheduler_api/main.py`
- Modify: `services/scheduler/src/scheduler_service/main.py`

**Interfaces:**
- Produces: `GET /api/v1/workers`
- Produces: `GET /api/v1/jobs/{id}` with attempts, state events, and structured logs
- Produces: `GET /api/v1/metrics/overview?project_id=&from=&to=&bucket=`
- Produces: Prometheus `/metrics` for API and scheduler
- Produces: scheduler `/health/live` and `/health/ready`

- [ ] **Step 1: Write failing operational API tests**

```python
async def test_job_detail_contains_immutable_timeline(client, auth, recovered_job):
    response = await client.get(
        f"/api/v1/jobs/{recovered_job.id}", headers=auth.headers
    )
    assert response.status_code == 200
    body = response.json()
    assert [event["to_status"] for event in body["events"]] == [
        "queued", "claimed", "running", "retry_scheduled", "claimed", "running", "completed"
    ]
    assert body["executions"][0]["logs"][0]["level"] == "info"


async def test_overview_metrics_include_latency_percentiles(client, auth, completed_jobs):
    response = await client.get("/api/v1/metrics/overview?bucket=minute", headers=auth.headers)
    assert response.status_code == 200
    assert {"p50_ms", "p95_ms", "p99_ms"} <= response.json()["latency"].keys()
```

Also test queue depth, throughput buckets, retry/failure/DLQ rates, stale worker status, tenant isolation, and pagination of execution logs.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `cd services/api && python -m pytest tests/test_operations.py -v`

Expected: FAIL because operational repositories and routes do not exist.

- [ ] **Step 3: Implement tenant-scoped operational queries**

Use PostgreSQL `date_trunc` for bounded time buckets and `percentile_cont` for p50/p95/p99. Validate the requested range is at most seven days and bucket is `minute`, `hour`, or `day`. Determine worker health from the latest heartbeat relative to the configured stale threshold; never trust a stored `ready` value after heartbeats stop.

- [ ] **Step 4: Implement detail, worker, and metrics routes**

Job detail returns the job, attempts, worker assignments, retry history, events ordered by `(created_at, id)`, and paginated logs. Queue metrics expose depth, active executions, configured concurrency, throughput, failure/retry rates, and saturation.

- [ ] **Step 5: Instrument API and scheduler without unbounded labels**

Expose request count/latency, scheduler tick duration, promoted jobs, cron materializations, expired lease recoveries, and database errors. Use route templates and operation names as labels; never use job IDs, user IDs, trace IDs, or raw URLs as metric labels.

- [ ] **Step 6: Add scheduler health endpoints and graceful readiness**

The scheduler health server reports live while the process can run and ready only after a successful database probe and recent successful tick. Readiness becomes false as soon as shutdown starts.

- [ ] **Step 7: Run all control-plane suites and regenerate OpenAPI**

Run:

```bash
cd services/api && python -m pytest -v
cd ../scheduler && python -m pytest -v
cd ../api && python -c 'import json; from scheduler_api.main import create_app; print(json.dumps(create_app().openapi(), indent=2))' > ../../packages/contracts/openapi.json
```

Expected: all tests pass and OpenAPI contains worker, job-detail, logs, and overview metrics schemas.

- [ ] **Step 8: Root updates README and commits operational APIs**

```bash
git add services/api services/scheduler packages/contracts README.md
git commit -m "feat: add operational metrics APIs"
```
