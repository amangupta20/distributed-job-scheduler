# PulseQueue

**A PostgreSQL-backed distributed job scheduler engineered for observable, failure-aware execution.**

![Status: work in progress](https://img.shields.io/badge/status-work_in_progress-orange)
![Worker persistence: verified](https://img.shields.io/badge/worker_persistence-verified-brightgreen)

> **Hero image capture pending.** A screenshot will be added only after the seeded application is running and the capture can be tied to a verified commit. No mock image is presented as implementation evidence.

## Why this stands out

PulseQueue is being built to demonstrate the hard parts of distributed job execution rather than hide them: atomic claims, renewable leases, fencing, bounded concurrency, graceful drain, and recovery after worker loss. The Go worker now has database-backed evidence for exclusive claims, strict per-queue concurrency, shared rate-token budgets, lease fencing, policy-driven retries, and atomic dead-letter transitions. Graceful-drain and worker-loss recovery evidence remain in progress.

## Quick start

The repository includes container definitions for PostgreSQL, one-shot migrations, the API, scheduler, Go worker, Prometheus, and Grafana. Start the implemented backend services with:

```bash
cp .env.example .env
# Replace JWT_SECRET in .env before starting the stack.
docker compose -f deploy/compose.yaml up -d postgres
until docker compose -f deploy/compose.yaml exec -T postgres pg_isready -U scheduler -d scheduler; do sleep 1; done
docker compose -f deploy/compose.yaml run --rm migrate
docker compose -f deploy/compose.yaml up -d --build api scheduler worker
```

The API defaults to port `8000`. The dashboard is not implemented yet. Use `make down` to stop the stack.

## Container topology

The Compose topology currently has `postgres`, `migrate`, `api`, `scheduler`, `worker`, `prometheus`, and `grafana` services. PostgreSQL, the scheduler, and workers use an internal backend network; the API also joins the public network. Worker replicas have no fixed `container_name`, so `docker compose -f deploy/compose.yaml up -d --scale worker=3` remains available. Health-gated dependency conditions and true observability profiles are still being hardened and are not claimed complete.

## Architecture

PostgreSQL is the single durable queue and source of truth. FastAPI provides the control plane, a Python scheduler materializes scheduled work and scans leases, and concurrent Go workers claim and execute jobs. Workers use notification-assisted polling as a latency optimization while retaining polling for correctness. Architecture diagrams remain to be added.

## Reliability model

The delivery model is at-least-once execution. Worker claims use PostgreSQL `SKIP LOCKED`, database-clock schedule eligibility and leases, unique fencing tokens, strict queue concurrency limits, and transactionally shared rate tokens. Queue selection locks only the selected contributing queue, so an unrelated queue remains claimable by another worker; contenders for the same queue serialize and share its capacity correctly. Claiming also creates the execution attempt and state event in the same transaction. Completion and failure reject expired or stale leases; each terminal transaction persists the job state, execution outcome, structured logs, and state event together. Retryable failures use fixed, linear, or capped exponential policy delays, while permanent and exhausted failures enter the DLQ atomically. Priority aging currently uses fixed worker defaults of a 60-second interval and a maximum boost of 100. Exactly-once external side effects are explicitly not a project claim. Graceful worker drain and scheduler-driven recovery still require end-to-end evidence.

## Visual feature tour

Planned dashboard views include operational overview, queues, workers, jobs, job lifecycle detail, and dead-letter recovery. Screenshots are intentionally absent until those routes exist and display data created by the deterministic seed workflow.

## Performance evidence

No throughput or latency figures are published yet. The planned benchmark will run a fixed workload against one, two, and three worker replicas, reject failed or duplicate successful completions, and record its environment alongside machine-readable output.

## Chaos recovery

`make chaos-demo` is reserved for the future recovery scenario that terminates a busy worker and verifies lease recovery through executable tests. The target exists now as a stable interface, but the scenario and any recovery claim are work in progress.

## API and data model

The FastAPI control plane implements authentication plus project, queue, retry-policy, job, worker, health, and dead-letter replay routes. SQLAlchemy models, an Alembic migration, and static contracts under `packages/contracts/` are present. Operational detail endpoints and contract-drift verification are still in progress.

## Development and tests

The worker persistence suite runs inside a Go builder container attached only to the Compose backend network. This keeps PostgreSQL private instead of publishing a host database port:

```bash
docker compose -f deploy/compose.yaml up -d postgres
until docker compose -f deploy/compose.yaml exec -T postgres pg_isready -U scheduler -d scheduler; do sleep 1; done
docker compose -f deploy/compose.yaml run --rm migrate
docker build --target builder -t pulsequeue-worker-test services/worker
BACKEND_NETWORK=$(docker inspect "$(docker compose -f deploy/compose.yaml ps -q postgres)" \
  --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}')
docker run --rm --network "$BACKEND_NETWORK" \
  -e WORKER_DATABASE_URL=postgresql://scheduler:scheduler@postgres:5432/scheduler \
  pulsequeue-worker-test go test -count=1 ./internal/claim -v
```

The [Makefile](Makefile) also records intended integration, seed, chaos, and benchmark workflows. Several referenced test services/scripts are not wired yet, so those targets are roadmap interfaces rather than current evidence.

## Claim-to-evidence table

| Claim | Status | Evidence |
|---|---|---|
| Go workers claim exclusively while enforcing shared queue concurrency and rate budgets. | Verified | [`services/worker/internal/claim/persistence_test.go`](services/worker/internal/claim/persistence_test.go) |
| A locked busy queue does not prevent another worker from claiming an independent queue. | Verified | [`services/worker/internal/claim/persistence_test.go`](services/worker/internal/claim/persistence_test.go) |
| Lease-fenced completion, policy retries, permanent failure, and exhausted-attempt DLQ transitions are atomic. | Verified | [`services/worker/internal/claim/persistence_test.go`](services/worker/internal/claim/persistence_test.go) and [`retry_test.go`](services/worker/internal/claim/retry_test.go) |
| Scheduler lease recovery, graceful drain, container health, UI behavior, and performance meet their design goals. | Not yet evidenced | Evidence will be added as the corresponding implementation milestones pass. |

## Trade-offs and limitations

- The API, scheduler, worker, database migration, containers, and service-level tests exist; the dashboard, deterministic demo seed, architecture diagrams, end-to-end chaos suite, benchmark harness, and screenshots remain incomplete.
- PostgreSQL is the only planned durable queue; the design intentionally does not add Redis or Kafka.
- At-least-once execution means handlers must tolerate retries and make external side effects idempotent where required.
- Benchmark results will be local and environment-qualified, not universal capacity guarantees.
- The optional observability profile is planned for local demonstration and is not a production deployment prescription.

## Screenshot capture instructions

Capture is blocked until the dashboard and deterministic seed are implemented. At that milestone:

1. Run `make up && make seed` and confirm the stack is healthy.
2. Use a `1440x900` browser viewport with a consistent theme and browser scale.
3. Capture the planned routes `http://localhost:3000/`, `/queues`, `/workers`, and `/jobs` as `docs/assets/dashboard-overview.png`, `queue-health.png`, `worker-grid.png`, and `job-explorer.png` respectively.
4. Do not crop out warnings or error indicators. Record the capture commit and seeded states in `docs/assets/CAPTURE.md`.
5. Add image links here only after each asset exists and has been reviewed against the running application.
