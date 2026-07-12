# Distributed Job Scheduler Design

Date: 2026-07-12

## 1. Purpose

Build a production-inspired, multi-tenant distributed job scheduling platform in approximately one day. The submission is expected to be reviewed primarily through its source code, README, tests, diagrams, and reproducible setup rather than a live presentation.

The project therefore optimizes for verifiable engineering quality. Every major claim in the README must link to implementation evidence, a test, a benchmark, a query plan, or an architecture decision record.

## 2. Differentiation Strategy

The primary differentiator is operationally meaningful containerization. Docker Compose must demonstrate that the system is distributed and failure-tolerant, rather than merely package the application.

The supporting differentiators are:

- A Go execution plane with bounded concurrency and atomic batch claiming.
- PostgreSQL-backed leases and fencing tokens for safe crash recovery.
- Transactional `LISTEN/NOTIFY` wake-ups with durable polling fallback.
- A shadcn dashboard with first-class operational metrics and job timelines.
- An optional, automatically provisioned Prometheus and Grafana profile.
- A reproducible chaos scenario that terminates a worker and verifies recovery.
- Fair queue scheduling, concurrency limits, and backpressure.
- An immutable execution audit trail.
- Checked-in benchmarks, critical query plans, and claim-to-evidence documentation.

Redis and Kafka are intentionally excluded. The expected workload does not justify their additional consistency and operational failure modes. PostgreSQL remains the single source of truth and provides adequate transactional claiming when supported by batch operations and purpose-built indexes.

Rust is intentionally excluded. For this I/O-heavy worker, it does not provide enough measurable advantage over Go to justify its implementation and maintenance risk.

## 3. Technology Stack

- Frontend: Next.js, TypeScript, shadcn/ui, Tailwind CSS, and a React charting library.
- API: Python, FastAPI, SQLAlchemy, Alembic, and Pydantic.
- Scheduler: Python using the API's database domain package but a separate process and container.
- Worker: Go with `pgx`, goroutines, contexts, and Prometheus instrumentation.
- Database: PostgreSQL.
- Observability: structured JSON logs, Prometheus metrics, optional Grafana, and application-level metrics APIs.
- Deployment: Dockerfiles and Docker Compose.
- Tests: pytest for API/integration behavior, Go tests for workers, and containerized concurrency and chaos tests.

## 4. Repository Layout

```text
apps/
  dashboard/
services/
  api/
  scheduler/
  worker/
packages/
  contracts/
deploy/
  compose.yaml
  docker/
  prometheus/
  grafana/
tests/
  integration/
  concurrency/
  chaos/
docs/
  architecture/
  benchmarks/
  decisions/
  superpowers/specs/
```

The API and scheduler share Python domain and database modules. They run as separate containers and entry points. The Go worker communicates directly with PostgreSQL for claims, leases, execution attempts, heartbeats, logs, and results.

## 5. System Components

### 5.1 Dashboard

The dashboard provides:

- Queue health and configuration.
- Worker state and heartbeat health.
- Paginated and filterable job exploration.
- Job execution logs, retry history, and lifecycle timeline.
- Manual retry, cancellation, pause, resume, and DLQ replay actions.
- Throughput, queue depth, success rate, failure rate, retry rate, DLQ growth, worker utilization, and latency percentiles.

Live views use bounded polling for the initial implementation. The API contracts will not prevent a later WebSocket implementation.

### 5.2 API Control Plane

FastAPI owns:

- Authentication and organization membership authorization.
- Projects, queues, retry policies, and scheduled-job configuration.
- Immediate, delayed, recurring, and batch job creation.
- Pagination, filtering, validation, and structured error responses.
- Operational actions such as queue pause/resume, cancellation, retry, and DLQ replay.
- OpenAPI documentation and dashboard metrics queries.

### 5.3 Scheduler

The scheduler owns:

- Promoting delayed jobs when their schedule becomes due.
- Materializing recurring cron occurrences.
- Promoting retry attempts at their calculated time.
- Recovering expired worker leases.
- Reconciling jobs that may have missed a notification.

Cron occurrences use deterministic occurrence keys, ensuring that multiple scheduler replicas cannot create duplicate instances.

### 5.4 Go Worker

The worker owns:

- Listening for PostgreSQL job notifications.
- Polling periodically as the correctness fallback.
- Atomically claiming bounded batches.
- Executing jobs through a bounded goroutine pool.
- Creating execution attempts and structured logs.
- Sending worker and job heartbeats.
- Extending valid leases.
- Completing, retrying, timing out, or dead-lettering jobs.
- Gracefully draining work after `SIGTERM`.

### 5.5 Observability Services

Every long-running service exposes liveness, readiness, and Prometheus metrics. The optional Compose `observability` profile starts Prometheus and Grafana with an automatically provisioned dashboard.

## 6. Data Model

The relational schema contains:

- `users`: authenticated identities and password hashes.
- `organizations`: tenant boundaries.
- `organization_memberships`: user-to-organization roles.
- `projects`: organization-owned namespaces.
- `queues`: priority, concurrency, rate limit, pause state, and configuration.
- `retry_policies`: fixed, linear, or exponential retry settings.
- `job_batches`: batch identity, aggregate status, and requested job count.
- `jobs`: authoritative job state, payload, schedule, and active lease.
- `scheduled_jobs`: cron definitions and next occurrence times.
- `job_executions`: one record per execution attempt.
- `job_state_events`: immutable lifecycle transitions.
- `workers`: worker identity, version, capacity, status, and current heartbeat.
- `worker_heartbeats`: retained worker heartbeat history.
- `job_logs`: structured execution log entries.
- `dead_letter_entries`: terminal-failure snapshots and replay metadata.

Important `jobs` fields include:

```text
id
project_id
queue_id
retry_policy_id
batch_id
idempotency_key
type
payload JSONB
priority
status
scheduled_at
attempt_count
max_attempts
claimed_by_worker_id
lease_token
lease_expires_at
created_at
updated_at
completed_at
```

A unique `(project_id, idempotency_key)` constraint makes repeated job-creation requests safe. Repeating a request with the same key and equivalent payload returns the existing job. Reusing a key with a conflicting payload returns `409 Conflict`.

Jobs may optionally belong to a `job_batches` record. Batch status and progress are derived from member job states, while the batch row preserves the original request identity and expected count.

Foreign keys cascade only for tenant-owned configuration where deletion is intentional. Execution history and audit evidence use restricted deletion or controlled archival. Logs and heartbeat histories have documented retention policies.

## 7. Job State Machine

The externally meaningful states are:

```text
queued
scheduled
claimed
running
retry_scheduled
completed
cancelled
dead_lettered
```

The normal lifecycle is:

```text
queued/scheduled -> claimed -> running -> completed
                                  |-> retry_scheduled -> claimed
                                  |-> dead_lettered
```

Every valid state transition writes a `job_state_events` record in the same database transaction. Invalid transitions return `409 Conflict` through the API or are rejected by guarded SQL in background services.

## 8. Atomic Claiming and Fencing

The worker claims a bounded batch in one transaction using a common table expression with `FOR UPDATE SKIP LOCKED`, followed by `UPDATE ... RETURNING`.

Candidate ordering considers queue eligibility, queue fairness, priority, schedule, and stable job ID ordering. The query excludes paused queues and queues at their concurrency limit.

On claim, each job receives:

- A worker ID.
- A unique lease token.
- A lease expiration timestamp.

All subsequent worker updates must match the job ID, worker ID, and lease token. If the scheduler reclaims an expired job, the token changes. A stale worker can no longer mark that job completed or mutate its current execution state.

The system promises at-least-once execution. It does not claim exactly-once external side effects. Idempotency keys, leases, fencing, and handler guidance reduce duplication risks.

## 9. Dispatch, Scheduling, and Fairness

An enqueue transaction inserts the job and sends PostgreSQL `NOTIFY`. PostgreSQL delivers the notification only after commit. Workers use notifications to avoid unnecessary latency but still poll periodically because notifications are not durable.

Queue scheduling enforces:

- Configured job priority.
- Per-queue concurrency limits.
- Worker capacity and backpressure.
- Priority aging: waiting jobs periodically gain effective priority, so a constantly busy high-priority queue cannot permanently starve older eligible work.
- An optional per-queue token-bucket rate limit.

Effective priority is calculated from the configured queue priority, job priority, and whole aging intervals since `scheduled_at`. The aging interval and maximum boost are configuration values with deterministic defaults. Distributed rate limiting, when enabled, stores the current token count and refill timestamp on the queue and updates them atomically in the claim transaction.

Batch job creation uses bounded chunks in one logical API operation. Failure returns a clear response and never reports a partially successful batch as complete.

## 9.1 Execution Contract

Workers execute jobs through a registry keyed by `jobs.type`. The initial registry provides:

- `noop` for deterministic verification and benchmarks.
- `http` for an outbound HTTP request with configured method, URL, headers, timeout, and body.
- Synthetic chaos handlers described later in this document.

Unknown job types fail validation during creation. Handler results use a common structure containing success state, output metadata, structured logs, retryability, and an optional error classification. Arbitrary source code or shell commands supplied in a job payload are not executed.

## 10. Retries and Dead Letter Queue

Retry policies support:

- Fixed delay.
- Linear backoff.
- Exponential backoff.

The failure transaction records the execution outcome, increments the attempt count, calculates the next eligible time, and creates the associated state event. When attempts are exhausted, it creates a `dead_letter_entries` record and transitions the job to `dead_lettered` atomically.

DLQ replay retains a link to the original failure and creates a traceable new execution path rather than silently deleting historical evidence.

## 11. Error Handling

REST errors use a stable envelope containing:

- Machine-readable error code.
- Human-readable message.
- Validation or domain details.
- Trace ID.

Authentication failures return `401`, authorization failures return `403`, missing resources return `404`, validation failures return `422`, and state or idempotency conflicts return `409`.

Database disconnections use bounded exponential reconnect backoff. Readiness becomes unhealthy when a service cannot safely accept work. Liveness remains healthy when the process can recover without replacement.

Worker shutdown stops new claims, marks the worker as draining, and gives active jobs a configurable grace period. Jobs that cannot drain safely are left for lease recovery.

## 12. Container Topology

Docker Compose includes:

- `postgres`: persistent volume, backend network only, and a readiness health check.
- `migrate`: one-shot Alembic migration service that must complete successfully.
- `api`: public and backend networks with liveness and readiness checks.
- `scheduler`: backend network only.
- `worker`: backend network only, scalable replicas, and no fixed `container_name`.
- `dashboard`: public network and documented public port.
- `prometheus` and `grafana`: optional `observability` profile.

Service dependencies use health or successful-completion conditions rather than basic startup order. Long-running containers handle `SIGTERM` gracefully. Runtime settings expose worker concurrency, batch size, lease duration, polling interval, and drain timeout.

The required scaling demonstration is:

```bash
docker compose -f deploy/compose.yaml up --scale worker=3
```

The required resilience demonstration is a documented command such as `make chaos-demo`, which enqueues synthetic jobs, terminates a worker during execution, waits for lease recovery, and verifies final execution counts.

## 13. Observability

Structured JSON logs carry trace ID, organization ID, project ID, queue ID, job ID, execution ID, worker ID, and lease token where applicable.

Metrics include:

- Queue depth and scheduled depth.
- Claim and execution throughput.
- Success, failure, retry, timeout, and DLQ rates.
- Claim latency and execution latency histograms.
- Worker capacity, active jobs, and utilization.
- Heartbeat age and expired lease recovery count.
- Queue concurrency and rate-limit saturation.

The main shadcn dashboard provides product-level operations views. Grafana provides optional infrastructure-focused views and is automatically provisioned through Compose.

## 14. Chaos and Synthetic Jobs

Synthetic handlers support:

- Immediate success.
- Configurable sleep.
- Failure for the first N attempts.
- Permanent failure.
- Timeout.
- Configurable structured logs.

The chaos scenario validates worker termination, expired lease recovery, retry timing, DLQ placement, stale fencing-token rejection, and the absence of multiple successful completions for one job.

## 15. Testing Strategy

Critical automated coverage includes:

- Atomic claim exclusivity under contention.
- Lease expiry and recovery.
- Stale fencing-token rejection.
- Queue pause and concurrency enforcement.
- Priority with starvation prevention.
- Fixed, linear, and exponential retry calculations.
- Unique cron occurrence creation.
- DLQ placement and traceable replay.
- Graceful worker drain behavior.
- Organization isolation and authorization.
- Idempotent job creation and conflicting-key rejection.
- Clean container startup with migration gating.
- Multiple scalable worker replicas.

Benchmarks report the environment, workload, worker count, concurrency, batch size, throughput, error count, and latency percentiles. The README must not present benchmark numbers from a different environment as universal guarantees.

## 16. Documentation and Review Evidence

The repository includes:

- Setup and clean-start instructions.
- Mermaid architecture and ER diagrams.
- OpenAPI documentation.
- Architecture decision records for PostgreSQL, Go, notification-assisted polling, and container topology.
- A database design document covering keys, indexes, normalization, cascades, retention, and performance.
- Reproducible benchmarks and critical `EXPLAIN ANALYZE` plans.
- A README claim-to-evidence table linking each claim to code or tests.

## 17. Agent Orchestration

The root agent owns architecture, cross-service contracts, integration, repository-wide commits, and final verification. Three file-isolated implementation agents may work concurrently:

- `api_database`: `gpt-5.6-luna`, medium reasoning.
- `go_worker`: `gpt-5.6-terra`, medium reasoning.
- `frontend_container`: `gpt-5.6-luna`, medium reasoning.

A later review agent uses `gpt-5.6-luna`, medium reasoning. The effective thread cap is four including the root agent, and recursive fan-out is disabled. Each implementation agent receives exact file ownership and must not change shared contracts without root approval.

Custom agents will be configured under project-local `.codex/agents/` when implementation begins. If a requested model is unavailable to the account, the root agent must report the failure and select an available lower-cost substitute rather than silently claiming the requested model was used.

## 18. Git and Delivery Cadence

The repository is initialized before implementation. The root agent owns integration commits so parallel agents do not race over shared Git state.

Expected milestone commits are:

1. `docs: define distributed scheduler architecture`
2. `chore: scaffold services and container topology`
3. `feat: add relational schema and control-plane API`
4. `feat: implement concurrent Go worker`
5. `feat: add scheduler and reliability lifecycle`
6. `feat: build operations dashboard`
7. `test: add contention and failure recovery coverage`
8. `docs: add benchmarks diagrams and reviewer guide`
9. `chore: finalize reproducible container deployment`

Milestone commits are created after their relevant verification passes. Smaller coherent commits may be retained when they improve the implementation history.

## 19. Scope Exclusions

The one-day implementation excludes:

- Kafka or Redis-based dispatch.
- Queue sharding.
- General workflow dependency graphs.
- Full enterprise RBAC beyond organization membership roles.
- Required external AI APIs.
- Claims of exactly-once external side effects.

Failure fingerprinting may group normalized errors without an external AI dependency. An AI summary provider remains a future extension and is not part of the critical path.

## 20. Success Criteria

The design succeeds when a reviewer can clone the repository, start the complete system with documented Docker commands, scale workers horizontally, create each required job type, inspect its lifecycle, observe metrics, terminate a worker, and verify automated recovery through code and tests.

The submission must be honest about incomplete functionality. README claims are included only when the referenced implementation and verification evidence exist.
