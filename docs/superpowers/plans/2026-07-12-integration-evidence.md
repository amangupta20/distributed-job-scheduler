# Integration and Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate all services, prove concurrency and failure recovery, publish reproducible benchmarks and diagrams, and maintain a visually polished claim-backed README.

**Architecture:** Root-owned scripts exercise the system only through public APIs, PostgreSQL verification queries, and Docker Compose operations. Evidence is generated from the real integrated stack and stored with environment disclosures. README claims link directly to their corresponding tests, code, query plans, or reports.

**Tech Stack:** Docker Compose, GNU Make-compatible commands, Python 3.12 test scripts, pytest, HTTPX, PostgreSQL psql, Mermaid, Prometheus/Grafana, and Markdown.

## Global Constraints

- Own `tests/**`, `docs/architecture/**`, `docs/benchmarks/**`, `docs/decisions/**`, `docs/assets/**`, `README.md`, root `Makefile`, `.gitignore`, `.env.example`, and `.codex/**`.
- Do not modify service internals directly; report failed contracts to the owning workstream.
- Do not publish benchmark numbers until the benchmark succeeds against the integrated stack.
- Do not use generated mock screenshots as implementation evidence.
- Missing remote captures require exact local capture instructions with route, viewport, seed command, and filename.
- Every README claim must point to evidence that exists in the committed repository.
- The assignment DOCX remains unmodified and is not committed unless the user explicitly requests it.

## File Map

```text
.codex/config.toml
.codex/agents/api-database.toml
.codex/agents/go-worker.toml
.codex/agents/frontend-container.toml
.codex/agents/reviewer.toml
.env.example
.gitignore
Makefile
README.md
tests/integration/conftest.py
tests/integration/test_job_lifecycle.py
tests/concurrency/test_atomic_claiming.py
tests/chaos/test_worker_recovery.py
tests/benchmark/run.py
tests/seed_demo.py
docs/architecture/architecture.md
docs/architecture/er-diagram.md
docs/benchmarks/latest.md
docs/benchmarks/claim-query-plan.txt
docs/decisions/0001-postgresql-queue.md
docs/decisions/0002-go-worker.md
docs/decisions/0003-notification-assisted-polling.md
docs/decisions/0004-container-topology.md
docs/assets/CAPTURE.md
```

### Task 1: Repository workflow, custom agents, and living README skeleton

**Files:**
- Create: `.codex/config.toml`
- Create: `.codex/agents/api-database.toml`
- Create: `.codex/agents/go-worker.toml`
- Create: `.codex/agents/frontend-container.toml`
- Create: `.codex/agents/reviewer.toml`
- Create: `.env.example`
- Create: `.gitignore`
- Create: `Makefile`
- Create: `README.md`

**Interfaces:**
- Produces: four bounded custom agent roles
- Produces: canonical commands `make up`, `make down`, `make test`, `make integration-test`, `make chaos-demo`, `make benchmark`, and `make seed`

- [ ] **Step 1: Write project-local subagent configuration**

`.codex/config.toml` contains:

```toml
[agents]
max_threads = 4
max_depth = 1
```

The Go agent contains:

```toml
name = "go_worker"
description = "Implements and verifies the PostgreSQL-backed concurrent Go worker."
model = "gpt-5.6-terra"
model_reasoning_effort = "medium"
sandbox_mode = "workspace-write"
developer_instructions = """
Own only services/worker/**. Follow the approved design and assigned plan task.
Use TDD, run Go tests with the race detector, and report changed files and exact verification output.
Do not edit deployment, API, dashboard, documentation, or shared contracts.
"""
```

The API and frontend agents use `gpt-5.6-luna` medium with their exclusive paths. The reviewer uses Luna medium, read-only mode, and prioritizes correctness, concurrency, authorization, container behavior, missing tests, and dishonest documentation claims.

- [ ] **Step 2: Create canonical environment and ignore rules**

`.env.example` includes every variable from the master plan with safe local defaults except `JWT_SECRET`, whose value is clearly marked for replacement. `.gitignore` excludes secrets, caches, virtual environments, node modules, compiled binaries, test artifacts, and local benchmark scratch data while retaining committed evidence files.

- [ ] **Step 3: Create executable orchestration targets**

```make
COMPOSE := docker compose -f deploy/compose.yaml

up:
	$(COMPOSE) up -d --build --wait

scale:
	$(COMPOSE) up -d --scale worker=3

down:
	$(COMPOSE) down

test:
	$(COMPOSE) run --rm api-test python -m pytest
	$(COMPOSE) run --rm worker-test go test -race ./...
	$(COMPOSE) run --rm dashboard-test npm test -- --run

integration-test:
	python -m pytest tests/integration tests/concurrency -v
```

Add `seed`, `chaos-demo`, `benchmark`, `logs`, and `observability` using the canonical service names.

- [ ] **Step 4: Create the initial README structure**

Use these sections in this order:

```text
Project title and engineering tagline
Status badges
Hero image slot with honest capture note until available
Why this stands out
Quick start
Container topology
Architecture
Reliability model
Visual feature tour
Performance evidence
Chaos recovery
API and data model
Development and tests
Claim-to-evidence table
Trade-offs and limitations
Screenshot capture instructions
```

Before real evidence exists, phrase claims as design goals or work-in-progress and do not render nonexistent image links. Replace them with real claims and assets as milestones pass.

- [ ] **Step 5: Validate TOML, environment completeness, Make targets, and Markdown links**

Run a Python TOML parse, compare environment variable names against all plans with `rg`, run `make -n` for every target, and run a local Markdown link checker if available. Expected: zero parse failures, missing variables, or invalid local links.

- [ ] **Step 6: Commit the verified repository workflow**

```bash
git add .codex .env.example .gitignore Makefile README.md
git commit -m "chore: scaffold repository workflow"
```

### Task 2: Deterministic seed and lifecycle integration tests

**Files:**
- Create: `tests/seed_demo.py`
- Create: `tests/integration/conftest.py`
- Create: `tests/integration/test_job_lifecycle.py`
- Modify: `Makefile`
- Modify: `README.md`

**Interfaces:**
- Produces: `python tests/seed_demo.py` with idempotent demo population
- Consumes: public API at `API_BASE_URL`

- [ ] **Step 1: Write a failing full lifecycle test**

```python
def test_immediate_delayed_retry_and_dlq_lifecycle(api, wait_for_job):
    project, queue = api.create_demo_project_and_queue()
    immediate = api.create_job(queue, "noop", {"message": "hello"})
    delayed = api.create_delayed_job(queue, "noop", {}, delay_seconds=2)
    retrying = api.create_job(queue, "chaos", {"fail_first": 2})
    permanent = api.create_job(queue, "chaos", {"always_fail": True}, max_attempts=2)
    assert wait_for_job(immediate, "completed")["attempt_count"] == 1
    assert wait_for_job(delayed, "completed")["scheduled_at"]
    assert wait_for_job(retrying, "completed")["attempt_count"] == 3
    assert wait_for_job(permanent, "dead_lettered")["attempt_count"] == 2
```

Also test cron occurrence uniqueness, batch progress, queue pause/resume, API pagination, and cross-organization isolation.

- [ ] **Step 2: Run against the integrated stack and verify meaningful failure**

Run: `python -m pytest tests/integration/test_job_lifecycle.py -v`

Expected before integration: connection or missing-endpoint failure, not a skipped test.

- [ ] **Step 3: Implement the API fixture and bounded wait helpers**

`wait_for_job` polls every 250 ms with a 30-second deadline and includes the last response and trace ID in timeout failures. No unbounded sleep is allowed.

- [ ] **Step 4: Implement the idempotent visual seed**

The seed creates one organization and project, queues representing healthy, saturated, paused, and failing states, at least three workers, completed history across a time series, active retries, one recovered lease event, and DLQ entries. Use stable idempotency keys so rerunning does not duplicate the dataset.

- [ ] **Step 5: Run lifecycle tests and update README truthfully**

Run: `make up && make seed && python -m pytest tests/integration/test_job_lifecycle.py -v`

Expected: PASS. Update quick start, supported job types, screenshots-to-capture, and the claim-to-evidence table.

- [ ] **Step 6: Commit deterministic integration coverage**

```bash
git add tests/seed_demo.py tests/integration Makefile README.md
git commit -m "test: add deterministic lifecycle integration coverage"
```

### Task 3: Contention, fencing, and container chaos proof

**Files:**
- Create: `tests/concurrency/test_atomic_claiming.py`
- Create: `tests/chaos/test_worker_recovery.py`
- Modify: `Makefile`
- Modify: `README.md`

**Interfaces:**
- Produces: executable evidence for atomic claims, concurrency limits, stale fencing rejection, worker scaling, and recovery

- [ ] **Step 1: Write a failing high-contention invariant test**

```python
def test_500_jobs_have_one_successful_completion_each(api, db, compose):
    queue = api.create_queue(concurrency_limit=24)
    jobs = api.create_batch(queue, count=500, job_type="noop")
    compose.scale("worker", 3)
    api.wait_for_batch(jobs.batch_id, terminal=True, timeout=60)
    duplicates = db.scalar("""
      SELECT count(*) FROM (
        SELECT job_id FROM job_executions
        WHERE status = 'completed'
        GROUP BY job_id HAVING count(*) > 1
      ) duplicated
    """)
    assert duplicates == 0
    assert db.scalar("SELECT count(*) FROM jobs WHERE batch_id=%s AND status='completed'", jobs.batch_id) == 500
```

Also assert observed active executions never exceed the queue concurrency limit.

- [ ] **Step 2: Write a failing real container recovery test**

Create a sleeping job, identify its worker through the API, stop that Compose container, wait for lease expiration, and assert a different worker completes it. Verify the audit trail contains lease expiration and reclaim events and the stopped worker never records a successful completion afterward.

- [ ] **Step 3: Run both tests before integration fixes**

Run: `python -m pytest tests/concurrency tests/chaos -v`

Expected: tests expose any claim, lease, shutdown, or API observability gaps rather than being skipped.

- [ ] **Step 4: Coordinate fixes through owning workstreams**

Provide each failure with exact reproduction command, SQL/API evidence, and expected invariant. The owning agent implements and verifies the smallest correction. Root reruns the failing test and the owning service's regression suite.

- [ ] **Step 5: Add `make chaos-demo` with readable progress**

The target invokes the same tested Python scenario and prints these transitions from observed data:

```text
job created
worker claimed job
worker container stopped
lease expired
replacement worker reclaimed job
job completed
invariants verified
```

- [ ] **Step 6: Run contention and chaos evidence three consecutive times**

Run:

```bash
for run in 1 2 3; do
  python -m pytest tests/concurrency tests/chaos -v || exit 1
done
```

Expected: all three passes. Update README claims only after this result.

- [ ] **Step 7: Commit contention and failure-recovery proof**

```bash
git add tests/concurrency tests/chaos Makefile README.md
git commit -m "test: prove concurrency and failure recovery"
```

### Task 4: Benchmarks, query plans, and architecture decisions

**Files:**
- Create: `tests/benchmark/run.py`
- Create: `docs/benchmarks/latest.md`
- Create: `docs/benchmarks/claim-query-plan.txt`
- Create: `docs/decisions/0001-postgresql-queue.md`
- Create: `docs/decisions/0002-go-worker.md`
- Create: `docs/decisions/0003-notification-assisted-polling.md`
- Create: `docs/decisions/0004-container-topology.md`
- Create: `docs/architecture/architecture.md`
- Create: `docs/architecture/er-diagram.md`
- Modify: `README.md`

**Interfaces:**
- Produces: reproducible benchmark matrix and environment-qualified report
- Produces: Mermaid architecture, deployment, lifecycle, and ER diagrams

- [ ] **Step 1: Write benchmark acceptance tests**

Test the report generator requires timestamp, CPU count, memory, OS, Docker version, PostgreSQL version, commit SHA, job count, worker replicas, concurrency, batch size, throughput, p50/p95/p99, failures, and duplicate successful completions.

- [ ] **Step 2: Implement the benchmark runner**

Run a fixed 2,000 noop-job workload at worker replica counts 1, 2, and 3 after a warm-up. Fail the benchmark if any job fails, times out, or has more than one successful completion. Emit Markdown and machine-readable JSON.

- [ ] **Step 3: Capture the critical query plan**

Seed at least 10,000 mixed-state jobs, run `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)` for the real claim query, and save the output. Verify the report references `ix_jobs_claimable`; if it does not, investigate statistics/query shape before documenting the plan.

- [ ] **Step 4: Write concise architecture decision records**

Every ADR contains Context, Decision, Consequences, Alternatives Rejected, and Evidence. Explicitly document why Redis, Kafka, and Rust were excluded and why notification-assisted polling retains a durable fallback.

- [ ] **Step 5: Write Mermaid diagrams from real names**

Use actual Compose service names and database table names. Validate Mermaid blocks with a renderer when available. Do not include a component or relation that does not exist in the repository.

- [ ] **Step 6: Run benchmark and update README evidence**

Run: `make benchmark`

Expected: report and JSON are generated, all invariants pass, and README numbers match the committed report exactly.

- [ ] **Step 7: Commit architecture and measured evidence**

```bash
git add tests/benchmark docs/architecture docs/benchmarks docs/decisions README.md
git commit -m "docs: publish architecture benchmarks and reviewer guide"
```

### Task 5: Visual evidence, final README, and clean-start verification

**Files:**
- Create: `docs/assets/CAPTURE.md`
- Create when captured: `docs/assets/dashboard-overview.png`
- Create when captured: `docs/assets/queue-health.png`
- Create when captured: `docs/assets/job-execution-timeline.png`
- Create when captured: `docs/assets/worker-scaling.png`
- Create when captured: `docs/assets/dead-letter-replay.png`
- Create when captured: `docs/assets/grafana-operations.png`
- Create when captured: `docs/assets/chaos-recovery.gif`
- Modify: `README.md`

**Interfaces:**
- Produces: polished reviewer-first README with only real assets and claims
- Produces: deterministic capture instructions for user-supplied local images

- [ ] **Step 1: Populate the final README narrative**

Lead with the operational outcome and one-command start. Keep the differentiators near the top. Embed real assets only when their files exist. Link each claim to its test, implementation, ADR, query plan, or benchmark report.

- [ ] **Step 2: Capture all visuals available in the remote environment**

Use seeded data and a 1440x900 viewport for dashboard images. Use the same theme and browser scale. Crop no error indicators out of evidence. Record capture commit SHA in `docs/assets/CAPTURE.md`.

- [ ] **Step 3: Write exact instructions for missing user-supplied captures**

For each missing asset specify:

```text
filename
make command to prepare state
URL and navigation steps
viewport
expected visible cards/rows/events
whether sensitive values require redaction
```

For the GIF, specify the command sequence and start/end screen state. The README must use a static fallback until the GIF exists.

- [ ] **Step 4: Run a documentation truth audit**

For every README feature statement, locate the implementing symbol and passing test. Remove or label any unverified statement. Check every command against `make -n` and Compose config. Check every local link and image path.

- [ ] **Step 5: Run final clean-start verification**

Run:

```bash
docker compose -f deploy/compose.yaml down -v --remove-orphans
docker compose -f deploy/compose.yaml build --no-cache
docker compose -f deploy/compose.yaml up -d --wait
docker compose -f deploy/compose.yaml up -d --scale worker=3
make test
make integration-test
make chaos-demo
make benchmark
docker compose -f deploy/compose.yaml --profile observability up -d --wait
docker compose -f deploy/compose.yaml ps
```

Expected: every command exits zero, migration is completed, long-running services are healthy, three workers are present, tests and evidence pass, and observability services are ready.

- [ ] **Step 6: Commit the final evidence and reproducibility milestone**

```bash
git add README.md Makefile tests docs .codex .env.example .gitignore
git commit -m "chore: finalize reproducible container deployment"
```
