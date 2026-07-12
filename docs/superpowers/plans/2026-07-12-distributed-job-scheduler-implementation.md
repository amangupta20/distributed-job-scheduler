# Distributed Job Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a reproducible distributed scheduler with a FastAPI control plane, PostgreSQL scheduling state, horizontally scalable Go workers, a polished shadcn operations dashboard, and executable reliability evidence.

**Architecture:** PostgreSQL is the single source of truth. FastAPI and a Python scheduler own control-plane behavior; Go workers use notification-assisted polling, atomic `SKIP LOCKED` claims, leases, and fencing. Docker Compose demonstrates migration gating, private networks, health checks, graceful shutdown, worker scaling, and an optional Prometheus/Grafana profile.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2, Alembic, PostgreSQL 17, Go 1.24 with pgx, Next.js with TypeScript and shadcn/ui, Docker Compose, Prometheus, Grafana, pytest, and Go test.

## Global Constraints

- The implementation is scoped for approximately one day; correctness and evidence outrank feature breadth.
- PostgreSQL is the only durable queue and source of truth; do not add Redis or Kafka.
- Workers provide at-least-once execution and must never claim exactly-once external side effects.
- Every state-changing worker query must guard on job ID, worker ID, and lease token.
- Worker Compose services must not set `container_name`, so `docker compose up --scale worker=3` remains valid.
- PostgreSQL, scheduler, and workers use only the private backend network; only the API and dashboard join the public edge network.
- Every milestone that changes commands, architecture, configuration, APIs, UI, metrics, limitations, or evidence updates `README.md` in the same milestone.
- README screenshots must come from the working seeded application; generated mock UI must not be presented as evidence.
- The root agent owns cross-service contracts, repository-wide commits, integration, and final verification.
- Subagents have exclusive directory ownership and must not edit another workstream without root approval.
- Effective live-agent cap is four including root; recursive agent fan-out is disabled.
- Root remains on `gpt-5.6-sol` high; `go_worker` uses `gpt-5.6-terra` medium; other implementation and review agents use `gpt-5.6-luna` medium.

## Workstream Files and Ownership

| Workstream | Exclusive ownership | Plan |
|---|---|---|
| Control plane | `services/api/**`, `services/scheduler/**`, `packages/contracts/**` | [Control plane plan](2026-07-12-control-plane.md) |
| Go worker | `services/worker/**` | [Go worker plan](2026-07-12-go-worker.md) |
| Dashboard and containers | `apps/dashboard/**`, `deploy/**` | [Dashboard/container plan](2026-07-12-dashboard-containers.md) |
| Integration and evidence | `tests/**`, `docs/architecture/**`, `docs/benchmarks/**`, `docs/decisions/**`, `docs/assets/**`, `README.md`, root `Makefile` | [Integration/evidence plan](2026-07-12-integration-evidence.md) |

The root agent owns root-level dependency and environment files, `.codex/**`, `.gitignore`, and `.env.example`. If a workstream needs one changed, it requests the exact change from root.

## Shared Contracts Locked Before Parallel Work

The root agent publishes these files before spawning implementation agents:

- `packages/contracts/openapi.json`: generated from FastAPI once API routes exist.
- `packages/contracts/job_states.json`: canonical job and worker states.
- `.env.example`: canonical service environment names.
- `deploy/compose.yaml`: canonical service and network names after dashboard/container integration.

Canonical job states:

```json
["queued", "scheduled", "claimed", "running", "retry_scheduled", "completed", "cancelled", "dead_lettered"]
```

Canonical worker states:

```json
["starting", "ready", "draining", "offline"]
```

Canonical database-related environment variables:

```text
DATABASE_URL=postgresql+psycopg://scheduler:scheduler@postgres:5432/scheduler
WORKER_DATABASE_URL=postgresql://scheduler:scheduler@postgres:5432/scheduler
POSTGRES_DB=scheduler
POSTGRES_USER=scheduler
POSTGRES_PASSWORD=scheduler
```

Canonical runtime environment variables:

```text
JWT_SECRET
API_PORT=8000
DASHBOARD_PORT=3000
WORKER_CONCURRENCY=8
WORKER_BATCH_SIZE=8
WORKER_LEASE_SECONDS=30
WORKER_HEARTBEAT_SECONDS=10
WORKER_POLL_SECONDS=2
WORKER_DRAIN_SECONDS=20
SCHEDULER_POLL_SECONDS=1
PRIORITY_AGING_SECONDS=60
PRIORITY_AGING_MAX_BOOST=100
```

## Execution Order

### Wave 0: Root bootstrap

- [ ] Create project-local custom agent definitions and thread limits.
- [ ] Create `.gitignore`, `.env.example`, the root `Makefile`, and the initial continuously maintained `README.md`.
- [ ] Commit `chore: scaffold repository workflow` after configuration validation.

### Wave 1: Parallel core work

- [ ] Dispatch `api_database` on the control-plane plan.
- [ ] Dispatch `go_worker` on the Go worker plan after the initial migration contract is available.
- [ ] Dispatch `frontend_container` on the dashboard/container plan using documented API contracts and deterministic fixture responses until the real API is ready.
- [ ] Root monitors file ownership, resolves interface questions, and integrates only verified milestones.

### Wave 2: Integration

- [ ] Generate and validate the OpenAPI contract.
- [ ] Connect the dashboard client to the real API.
- [ ] Run migrations against a clean PostgreSQL volume.
- [ ] Run one API, one scheduler, and three workers through Compose.
- [ ] Execute integration, contention, and shutdown tests from the integration/evidence plan.

### Wave 3: Review and evidence

- [ ] Dispatch a read-only Luna reviewer for spec compliance and code quality.
- [ ] Resolve critical and important findings with the owning implementation agent.
- [ ] Run benchmarks and save the environment-qualified report.
- [ ] Generate query plans and verify they use intended indexes.
- [ ] Seed the full demo, capture available screenshots, and write exact local capture instructions for any missing asset.
- [ ] Update the README claim-to-evidence table and limitations.
- [ ] Run final clean-clone-equivalent verification and commit the final reproducibility milestone.

## Required Final Commands

```bash
docker compose -f deploy/compose.yaml config
docker compose -f deploy/compose.yaml build
docker compose -f deploy/compose.yaml up -d --wait
docker compose -f deploy/compose.yaml up -d --scale worker=3
make test
make integration-test
make chaos-demo
make benchmark
docker compose -f deploy/compose.yaml --profile observability up -d
```

Expected outcomes:

- Compose configuration renders without errors.
- All images build from a clean dependency cache.
- Migration completes before API, scheduler, and workers report ready.
- Three worker replicas are visible without naming collisions.
- Unit and integration tests pass.
- Chaos recovery ends with no duplicated successful completion.
- Benchmark artifacts disclose their environment and contain no failed jobs.
- Prometheus and Grafana become healthy with a provisioned scheduler dashboard.

## Commit Policy

The root agent creates repository-wide commits after each independently verified deliverable. Agents report changed paths and test output but do not run overlapping Git operations. The target history is:

```text
docs: define distributed scheduler architecture
docs: require visual and current project readme
chore: scaffold repository workflow
feat: add relational scheduler control plane
feat: implement atomic concurrent worker
feat: add scheduling and lease recovery
feat: build operations dashboard
feat: add observable container platform
test: prove concurrency and failure recovery
docs: publish architecture benchmarks and reviewer guide
chore: finalize reproducible container deployment
```

Commit messages may be split further when a smaller coherent and verified boundary improves reviewability.
