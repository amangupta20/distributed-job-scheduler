# Dashboard and Container Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shadcn operations dashboard and a health-gated Docker Compose topology that visibly demonstrates horizontal scaling, graceful behavior, private networking, migrations, persistence, and optional observability.

**Architecture:** A Next.js application consumes the generated OpenAPI contract through a small typed client and uses bounded polling for live operational views. Docker Compose builds each service, gates startup on PostgreSQL health and migration success, isolates backend traffic, and provisions Prometheus/Grafana through an optional profile.

**Tech Stack:** Node.js 22, Next.js, TypeScript, Tailwind CSS, shadcn/ui, Recharts, TanStack Query, Vitest, Testing Library, Playwright, Docker Compose, Prometheus, and Grafana.

## Global Constraints

- Own only `apps/dashboard/**` and `deploy/**`.
- Do not edit API, scheduler, Go worker, root README, root Makefile, or root environment files.
- Do not invent API fields; use `packages/contracts/openapi.json` and request contract changes from root.
- Dashboard screenshots must use real seeded data before being included as evidence.
- Worker Compose service must not define `container_name`, `ports`, or the public edge network.
- PostgreSQL must not publish a host port by default.
- All long-running service dependencies use health conditions; migration uses successful-completion conditions.
- The observability profile must be optional and automatically provisioned.

## File Map

```text
apps/dashboard/
  package.json
  next.config.ts
  src/app/layout.tsx
  src/app/page.tsx
  src/app/login/page.tsx
  src/app/queues/page.tsx
  src/app/jobs/page.tsx
  src/app/jobs/[id]/page.tsx
  src/app/workers/page.tsx
  src/app/dlq/page.tsx
  src/components/navigation.tsx
  src/components/metric-card.tsx
  src/components/queue-health-table.tsx
  src/components/job-status-badge.tsx
  src/components/job-timeline.tsx
  src/components/throughput-chart.tsx
  src/components/worker-grid.tsx
  src/lib/api.ts
  src/lib/query.ts
  src/lib/types.ts
  src/test/
  e2e/dashboard.spec.ts
deploy/
  compose.yaml
  docker/api.Dockerfile
  docker/scheduler.Dockerfile
  docker/worker.Dockerfile
  docker/dashboard.Dockerfile
  prometheus/prometheus.yml
  grafana/provisioning/datasources/prometheus.yml
  grafana/provisioning/dashboards/provider.yml
  grafana/dashboards/scheduler-operations.json
```

### Task 1: Dashboard shell, typed client, and authentication flow

**Files:**
- Create: `apps/dashboard/package.json`
- Create: `apps/dashboard/src/app/layout.tsx`
- Create: `apps/dashboard/src/app/login/page.tsx`
- Create: `apps/dashboard/src/components/navigation.tsx`
- Create: `apps/dashboard/src/lib/api.ts`
- Create: `apps/dashboard/src/lib/types.ts`
- Create: `apps/dashboard/src/test/api.test.ts`
- Create: `apps/dashboard/src/test/navigation.test.tsx`

**Interfaces:**
- Produces: `apiRequest<T>(path, options) -> Promise<T>`
- Produces: bearer token storage and 401 redirect behavior
- Produces: responsive application navigation

- [ ] **Step 1: Write failing API client tests**

```typescript
it("adds the bearer token and trace id", async () => {
  localStorage.setItem("access_token", "token");
  server.use(http.get("*/api/v1/projects", ({ request }) => {
    expect(request.headers.get("authorization")).toBe("Bearer token");
    expect(request.headers.get("x-trace-id")).toBeTruthy();
    return HttpResponse.json({ items: [], next_cursor: null });
  }));
  await apiRequest("/api/v1/projects");
});
```

Also assert the client throws a typed `ApiError` from the stable envelope and clears credentials on 401.

- [ ] **Step 2: Run tests and verify failure**

Run: `cd apps/dashboard && npm test -- --run src/test/api.test.ts src/test/navigation.test.tsx`

Expected: FAIL because the client and shell do not exist.

- [ ] **Step 3: Implement the typed API client**

```typescript
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly traceId: string,
    readonly details: unknown,
  ) { super(message); }
}

export async function apiRequest<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = window.localStorage.getItem("access_token");
  const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}${path}`, {
    ...init,
    headers: { "content-type": "application/json", "x-trace-id": crypto.randomUUID(),
      ...(token ? { authorization: `Bearer ${token}` } : {}), ...init.headers },
  });
  if (!response.ok) throw await toApiError(response);
  return response.json() as Promise<T>;
}
```

- [ ] **Step 4: Implement login and the responsive operations shell**

The sidebar links to Overview, Queues, Jobs, Workers, and Dead Letter Queue. The mobile layout uses a shadcn sheet. Login submits credentials, stores the returned access token, and redirects to `/`.

- [ ] **Step 5: Run tests and build**

Run:

```bash
cd apps/dashboard
npm test -- --run
npm run build
```

Expected: PASS and production build completes without TypeScript errors.

- [ ] **Step 6: Root updates README and commits the dashboard foundation**

```bash
git add apps/dashboard README.md
git commit -m "feat(ui): add authenticated operations shell"
```

### Task 2: Operational overview, queues, and workers

**Files:**
- Create: `apps/dashboard/src/app/page.tsx`
- Create: `apps/dashboard/src/app/queues/page.tsx`
- Create: `apps/dashboard/src/app/workers/page.tsx`
- Create: `apps/dashboard/src/components/metric-card.tsx`
- Create: `apps/dashboard/src/components/queue-health-table.tsx`
- Create: `apps/dashboard/src/components/throughput-chart.tsx`
- Create: `apps/dashboard/src/components/worker-grid.tsx`
- Create: `apps/dashboard/src/test/overview.test.tsx`

**Interfaces:**
- Consumes: metrics, queue, and worker endpoints from OpenAPI
- Produces: 5-second bounded polling with visible stale/error state

- [ ] **Step 1: Write a failing populated-overview test**

```typescript
it("renders health metrics and worker state", async () => {
  render(<OverviewPage />);
  expect(await screen.findByText("1,240 jobs/min")).toBeInTheDocument();
  expect(screen.getByText("p95 184 ms")).toBeInTheDocument();
  expect(screen.getByText("3 ready")).toBeInTheDocument();
  expect(screen.getByRole("img", { name: /throughput/i })).toBeInTheDocument();
});
```

MSW fixtures must include healthy, saturated, retrying, and DLQ states rather than an unrealistically perfect dashboard.

- [ ] **Step 2: Run the focused test and verify failure**

Run: `cd apps/dashboard && npm test -- --run src/test/overview.test.tsx`

Expected: FAIL because overview components do not exist.

- [ ] **Step 3: Implement cards and accessible charts**

Metric cards show value, comparison, and operational meaning. Charts include an `aria-label`, tooltip, legend, time zone, and an accessible tabular fallback. Display throughput, queue depth, success/failure/retry rate, p50/p95/p99, and DLQ growth.

- [ ] **Step 4: Implement queue controls and worker status**

Queue rows show priority, depth, active/concurrency, rate-limit saturation, pause state, and recent throughput. Pause/resume actions require confirmation and refresh affected queries. Worker cards show readiness, version, capacity, active count, heartbeat age, and draining/offline state.

- [ ] **Step 5: Verify tests and production build**

Run: `cd apps/dashboard && npm test -- --run && npm run build`

Expected: PASS. Report dashboard routes and intended screenshot states to root.

- [ ] **Step 6: Root updates README and commits operations views**

```bash
git add apps/dashboard README.md
git commit -m "feat(ui): add queue and worker operations views"
```

### Task 3: Job explorer, lifecycle timeline, logs, and DLQ replay

**Files:**
- Create: `apps/dashboard/src/app/jobs/page.tsx`
- Create: `apps/dashboard/src/app/jobs/[id]/page.tsx`
- Create: `apps/dashboard/src/app/dlq/page.tsx`
- Create: `apps/dashboard/src/components/job-status-badge.tsx`
- Create: `apps/dashboard/src/components/job-timeline.tsx`
- Create: `apps/dashboard/src/test/jobs.test.tsx`
- Create: `apps/dashboard/e2e/dashboard.spec.ts`

**Interfaces:**
- Consumes: cursor-paginated jobs, job detail, execution logs, state events, and DLQ endpoints
- Produces: filter state encoded in URL query parameters

- [ ] **Step 1: Write failing explorer and timeline tests**

```typescript
it("renders fenced recovery in chronological order", async () => {
  render(<JobDetailPage params={Promise.resolve({ id: "job-1" })} />);
  const events = await screen.findAllByTestId("timeline-event");
  expect(events.map((node) => node.textContent)).toEqual(expect.arrayContaining([
    expect.stringContaining("claimed"),
    expect.stringContaining("lease expired"),
    expect.stringContaining("reclaimed"),
    expect.stringContaining("completed"),
  ]));
});
```

Also test filtering, next-cursor navigation, log rendering, retry confirmation, and DLQ replay linkage.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `cd apps/dashboard && npm test -- --run src/test/jobs.test.tsx`

Expected: FAIL because the job views do not exist.

- [ ] **Step 3: Implement filterable job exploration**

Filters include project, queue, status, worker, and time range. Preserve filters in the URL, debounce textual input by 300 ms, and reset the cursor when filters change. Status badges use both text and color.

- [ ] **Step 4: Implement job detail and DLQ views**

The job detail page includes payload, attempt summary, worker assignments, immutable state timeline, structured logs, retry history, timing, and trace IDs. DLQ replay requires confirmation and links both original and replacement jobs.

- [ ] **Step 5: Add the Playwright happy-path review flow**

```typescript
test("operator inspects and replays a failed job", async ({ page }) => {
  await page.goto("/dlq");
  await page.getByRole("link", { name: /job-/ }).first().click();
  await expect(page.getByText("dead_lettered")).toBeVisible();
  await page.getByRole("button", { name: "Replay job" }).click();
  await page.getByRole("button", { name: "Confirm replay" }).click();
  await expect(page.getByText(/replacement job/i)).toBeVisible();
});
```

- [ ] **Step 6: Run UI tests and production build**

Run: `cd apps/dashboard && npm test -- --run && npm run build`

Expected: PASS. Run Playwright when the integrated Compose stack is available.

- [ ] **Step 7: Root updates README and commits job inspection views**

```bash
git add apps/dashboard README.md
git commit -m "feat: build operations dashboard"
```

### Task 4: Production Dockerfiles and Compose topology

**Files:**
- Create: `deploy/docker/api.Dockerfile`
- Create: `deploy/docker/scheduler.Dockerfile`
- Create: `deploy/docker/worker.Dockerfile`
- Create: `deploy/docker/dashboard.Dockerfile`
- Create: `deploy/compose.yaml`

**Interfaces:**
- Produces: services `postgres`, `migrate`, `api`, `scheduler`, `worker`, and `dashboard`
- Produces: explicit-run `tools` profile services `api-test`, `worker-test`, and `dashboard-test`
- Produces: networks `edge` and `backend`
- Produces: named volume `postgres_data`

- [ ] **Step 1: Write the Compose topology assertions**

Create `deploy/test_compose.py` that parses `docker compose config --format json` and asserts:

```python
assert "container_name" not in services["worker"]
assert "ports" not in services["worker"]
assert set(services["worker"]["networks"]) == {"backend"}
assert set(services["postgres"]["networks"]) == {"backend"}
assert set(services["api"]["networks"]) == {"edge", "backend"}
assert services["migrate"]["restart"] == "no"
assert "postgres_data" in volumes
```

- [ ] **Step 2: Run the topology test and verify failure**

Run: `python -m pytest deploy/test_compose.py -v`

Expected: FAIL because Compose does not exist.

- [ ] **Step 3: Implement multi-stage, non-root Dockerfiles**

Each final image uses a non-root user, copies only runtime artifacts, has an explicit `STOPSIGNAL SIGTERM`, and exposes only its internal application port. The Go image compiles a static worker binary in a builder stage.

- [ ] **Step 4: Implement health-gated Compose dependencies**

Required dependency semantics:

```yaml
api:
  depends_on:
    postgres:
      condition: service_healthy
    migrate:
      condition: service_completed_successfully
worker:
  depends_on:
    postgres:
      condition: service_healthy
    migrate:
      condition: service_completed_successfully
```

Add health checks for PostgreSQL, API, scheduler, worker, and dashboard. Do not mount source directories in the default production-like configuration.

Test services use build stages that retain development tooling and source tests. They declare `profiles: [tools]`, do not start during normal `up`, and run only when explicitly requested by `docker compose run --rm api-test`, `worker-test`, or `dashboard-test`.

- [ ] **Step 5: Validate configuration, build, and scaling**

Run:

```bash
docker compose -f deploy/compose.yaml config
docker compose -f deploy/compose.yaml build
docker compose -f deploy/compose.yaml up -d --wait
docker compose -f deploy/compose.yaml up -d --scale worker=3
docker compose -f deploy/compose.yaml ps
```

Expected: migration exits zero; long-running services are healthy; three worker replicas appear with generated Compose names.

- [ ] **Step 6: Root updates README and commits the container topology**

```bash
git add deploy README.md
git commit -m "feat: add scalable container topology"
```

### Task 5: Optional Prometheus and Grafana profile

**Files:**
- Create: `deploy/prometheus/prometheus.yml`
- Create: `deploy/grafana/provisioning/datasources/prometheus.yml`
- Create: `deploy/grafana/provisioning/dashboards/provider.yml`
- Create: `deploy/grafana/dashboards/scheduler-operations.json`
- Modify: `deploy/compose.yaml`

**Interfaces:**
- Produces: `observability` Compose profile
- Produces: Grafana dashboard UID `pulsequeue-operations`

- [ ] **Step 1: Add a failing profile validation test**

Assert Prometheus and Grafana have `profiles: [observability]`, Prometheus scrapes API/scheduler/worker metrics, Grafana has a provisioned Prometheus datasource, and the dashboard JSON contains panels for throughput, failures, p95 execution, queue depth, active workers, and recovered leases.

- [ ] **Step 2: Run the test and verify failure**

Run: `python -m pytest deploy/test_compose.py -v`

Expected: FAIL because observability services and provisioning do not exist.

- [ ] **Step 3: Implement the optional profile and provisioning**

Grafana uses anonymous viewer access only for local demo use. Prometheus retains seven days in its named volume. Neither service is required for the core stack to become healthy.

- [ ] **Step 4: Start and verify the observability profile**

Run:

```bash
docker compose -f deploy/compose.yaml --profile observability up -d --wait
curl -fsS http://localhost:9090/-/ready
curl -fsS http://localhost:3001/api/health
```

Expected: both endpoints succeed and dashboard UID `pulsequeue-operations` is returned by the Grafana API.

- [ ] **Step 5: Report all setup and visual evidence requirements to root**

Include public ports, scaling command, observability command, health semantics, screenshot routes, seeded states required for each screenshot, and any remaining local capture steps.

- [ ] **Step 6: Root updates README and commits observability provisioning**

```bash
git add deploy README.md
git commit -m "feat: add observable container platform"
```
