# PulseQueue

**A PostgreSQL-backed distributed job scheduler engineered for observable, failure-aware execution.**

![Status: work in progress](https://img.shields.io/badge/status-work_in_progress-orange)
![Evidence: pending](https://img.shields.io/badge/evidence-pending-lightgrey)

> **Hero image capture pending.** A screenshot will be added only after the seeded application is running and the capture can be tied to a verified commit. No mock image is presented as implementation evidence.

## Why this stands out

PulseQueue is being built to demonstrate the hard parts of distributed job execution rather than hide them: atomic claims, renewable leases, fencing, bounded concurrency, graceful drain, and recovery after worker loss. The Go worker now has database-backed evidence for exclusive claims, strict per-queue concurrency, shared rate-token budgets, lease fencing, policy-driven retries, and atomic dead-letter transitions. Graceful-drain and worker-loss recovery evidence remain in progress.

## Quick start

The repository workflow is scaffolded, but the Compose stack is not implemented yet. Once `deploy/compose.yaml` and the services land, the canonical local workflow will be:

```bash
cp .env.example .env
# Replace JWT_SECRET in .env before starting the stack.
make up
make seed
```

The planned API and dashboard ports default to `8000` and `3000`. Use `make down` to stop the stack.

## Container topology

The planned Compose topology has `postgres`, `migrate`, `api`, `scheduler`, `worker`, and `dashboard` services. The target design keeps PostgreSQL, the scheduler, and scalable worker replicas on a private backend network; only the API and dashboard also join the edge network. The Compose definition and topology checks are work in progress.

## Architecture

PostgreSQL is intended to be the single durable queue and source of truth. FastAPI and a Python scheduler will form the control plane, while concurrent Go workers will claim and execute jobs. Notification-assisted polling is planned as a latency optimization, with polling retained for correctness. Architecture diagrams will be linked after they are implemented and validated.

## Reliability model

The delivery model is at-least-once execution. Worker claims use PostgreSQL `SKIP LOCKED`, database-clock schedule eligibility and leases, unique fencing tokens, strict queue concurrency limits, and transactionally shared rate tokens. Claiming also creates the execution attempt and state event in the same transaction. Completion and failure reject expired or stale leases; each terminal transaction persists the job state, execution outcome, structured logs, and state event together. Retryable failures use fixed, linear, or capped exponential policy delays, while permanent and exhausted failures enter the DLQ atomically. Exactly-once external side effects are explicitly not a project claim. Graceful worker drain and scheduler-driven recovery still require end-to-end evidence.

## Visual feature tour

Planned dashboard views include operational overview, queues, workers, jobs, job lifecycle detail, and dead-letter recovery. Screenshots are intentionally absent until those routes exist and display data created by the deterministic seed workflow.

## Performance evidence

No throughput or latency figures are published yet. The planned benchmark will run a fixed workload against one, two, and three worker replicas, reject failed or duplicate successful completions, and record its environment alongside machine-readable output.

## Chaos recovery

`make chaos-demo` is reserved for the future recovery scenario that terminates a busy worker and verifies lease recovery through executable tests. The target exists now as a stable interface, but the scenario and any recovery claim are work in progress.

## API and data model

The planned API covers authenticated organizations, projects, queues, jobs, workers, dead-letter replay, health, and operational metrics. The canonical OpenAPI document, database schema, and job-state contract will be linked here only after they exist.

## Development and tests

The canonical commands are defined in the [Makefile](Makefile):

```bash
make test
make integration-test
make chaos-demo
make benchmark
make logs
make observability
```

The worker persistence integration suite requires a migrated PostgreSQL database and is run directly with:

```bash
cd services/worker
WORKER_DATABASE_URL=postgresql://scheduler:scheduler@127.0.0.1:5432/scheduler \
  go test -race ./internal/claim -v
```

These commands are stable workflow contracts. Their referenced services, tests, and scripts will be added in later milestones; this scaffold does not claim that application tests can run yet.

## Claim-to-evidence table

| Claim | Status | Evidence |
|---|---|---|
| Project-local agent concurrency is bounded to four threads with no recursive fan-out. | Configured | [Agent configuration](.codex/config.toml) |
| Implementation roles have exclusive path ownership and a read-only reviewer role exists. | Configured | [Agent definitions](.codex/agents/) |
| Environment names and canonical workflow commands are established. | Configured | [.env.example](.env.example) and [Makefile](Makefile) |
| Go workers claim exclusively while enforcing shared queue concurrency and rate budgets. | Verified | [`services/worker/internal/claim/persistence_test.go`](services/worker/internal/claim/persistence_test.go) |
| Lease-fenced completion, policy retries, permanent failure, and exhausted-attempt DLQ transitions are atomic. | Verified | [`services/worker/internal/claim/persistence_test.go`](services/worker/internal/claim/persistence_test.go) and [`retry_test.go`](services/worker/internal/claim/retry_test.go) |
| Scheduler lease recovery, graceful drain, container health, UI behavior, and performance meet their design goals. | Not yet evidenced | Evidence will be added as the corresponding implementation milestones pass. |

## Trade-offs and limitations

- The application, containers, tests, diagrams, benchmarks, and screenshots are not implemented at this scaffold milestone.
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
