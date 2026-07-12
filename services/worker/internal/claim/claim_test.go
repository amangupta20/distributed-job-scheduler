package claim_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pulsequeue/worker/internal/claim"
)

func dbURL() string {
	if url := os.Getenv("WORKER_DATABASE_URL"); url != "" {
		return url
	}
	return "postgresql://scheduler:scheduler@127.0.0.1:5432/scheduler"
}

// testPool creates a pgxpool, creates required tables, and registers cleanup.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dbURL())
	if err != nil {
		t.Fatalf("parse db url: %v", err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// seedJob inserts a queued job and returns its ID.
func seedJob(t *testing.T, pool *pgxpool.Pool, projectID, queueID uuid.UUID, jobType string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	jobID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO jobs (id, project_id, queue_id, type, payload, priority, status, scheduled_at, timeout_seconds, max_attempts, attempt_count, created_at, updated_at)
		VALUES ($1, $2, $3, $4, '{}'::jsonb, 0, 'queued', now(), 60, 3, 0, now(), now())
	`, jobID, projectID, queueID, jobType)
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
	return jobID
}

// seedEnv inserts the required org, project, queue and returns their IDs.
func seedEnv(t *testing.T, pool *pgxpool.Pool) (projectID, queueID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	orgID := uuid.New()
	projectID = uuid.New()
	queueID = uuid.New()

	_, err := pool.Exec(ctx, `INSERT INTO organizations (id, name, created_at, updated_at) VALUES ($1, $2, now(), now())`, orgID, fmt.Sprintf("test-org-%s", orgID))
	if err != nil {
		t.Fatalf("seed org: %v", err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO projects (id, organization_id, name, created_at, updated_at) VALUES ($1, $2, $3, now(), now())`, projectID, orgID, "test-proj")
	if err != nil {
		t.Fatalf("seed project: %v", err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO queues (id, project_id, name, priority, concurrency_limit, pause_state, created_at, updated_at) VALUES ($1, $2, $3, 0, 100, false, now(), now())`, queueID, projectID, "default")
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM jobs WHERE queue_id = $1`, queueID)
		pool.Exec(context.Background(), `DELETE FROM queues WHERE id = $1`, queueID)
		pool.Exec(context.Background(), `DELETE FROM projects WHERE id = $1`, projectID)
		pool.Exec(context.Background(), `DELETE FROM organizations WHERE id = $1`, orgID)
	})
	return projectID, queueID
}

// TestClaimExclusivity verifies that two concurrent workers each claim disjoint sets of jobs.
func TestClaimExclusivity(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	projectID, queueID := seedEnv(t, pool)

	// Seed 4 jobs
	const numJobs = 4
	for i := 0; i < numJobs; i++ {
		seedJob(t, pool, projectID, queueID, "noop")
	}

	worker1 := uuid.New()
	worker2 := uuid.New()
	leaseDur := 30 * time.Second

	// Register workers
	for _, wid := range []uuid.UUID{worker1, worker2} {
		pool.Exec(ctx, `INSERT INTO workers (id, name, version, capacity, status, active_jobs, created_at, updated_at) VALUES ($1, 'w', '1.0', 8, 'ready', 0, now(), now()) ON CONFLICT DO NOTHING`, wid)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM workers WHERE id IN ($1, $2)`, worker1, worker2)
	})


	var mu sync.Mutex
	claimedByW1 := map[uuid.UUID]bool{}
	claimedByW2 := map[uuid.UUID]bool{}
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		jobs, err := claim.ClaimBatch(ctx, pool, worker1, 4, leaseDur)
		if err != nil {
			t.Errorf("worker1 claim: %v", err)
			return
		}
		mu.Lock()
		for _, j := range jobs {
			claimedByW1[j.ID] = true
		}
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		jobs, err := claim.ClaimBatch(ctx, pool, worker2, 4, leaseDur)
		if err != nil {
			t.Errorf("worker2 claim: %v", err)
			return
		}
		mu.Lock()
		for _, j := range jobs {
			claimedByW2[j.ID] = true
		}
		mu.Unlock()
	}()
	wg.Wait()

	// No job should be in both sets
	for id := range claimedByW1 {
		if claimedByW2[id] {
			t.Errorf("job %s was claimed by both workers (exclusivity violated)", id)
		}
	}
	// All 4 jobs should be claimed in total
	total := len(claimedByW1) + len(claimedByW2)
	if total != numJobs {
		t.Errorf("expected %d total claimed jobs, got %d", numJobs, total)
	}
}

// TestStaleLeaseFencingRejectsComplete verifies that a stale worker cannot mark a job complete.
func TestStaleLeaseFencingRejectsComplete(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	projectID, queueID := seedEnv(t, pool)
	workerID := uuid.New()
	pool.Exec(ctx, `INSERT INTO workers (id, name, version, capacity, status, active_jobs, created_at, updated_at) VALUES ($1, 'w', '1.0', 8, 'ready', 0, now(), now()) ON CONFLICT DO NOTHING`, workerID)
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM workers WHERE id = $1`, workerID)
	})

	jobID := seedJob(t, pool, projectID, queueID, "noop")

	// Claim the job legitimately
	jobs, err := claim.ClaimBatch(ctx, pool, workerID, 1, 30*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(jobs) == 0 {
		t.Fatalf("expected to claim the job %s", jobID)
	}
	_ = jobID
	j := jobs[0]

	// Simulate scheduler forcibly changing the lease token (lease expiry recovery)
	newToken := "new-lease-token-from-scheduler"
	pool.Exec(ctx, `UPDATE jobs SET lease_token = $1 WHERE id = $2`, newToken, j.ID)

	// Mark running with the original token (now stale)
	err = claim.MarkRunning(ctx, pool, j.ID, workerID, j.LeaseToken)
	if err == nil {
		t.Error("expected stale lease error on MarkRunning, got nil")
	}

	// Complete with stale token
	err = claim.Complete(ctx, pool, j.ID, workerID, j.LeaseToken)
	if err == nil {
		t.Error("expected stale lease error on Complete, got nil")
	}
}

// TestLeaseExtension verifies that lease extension updates the expiry.
func TestLeaseExtension(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	projectID, queueID := seedEnv(t, pool)
	workerID := uuid.New()
	pool.Exec(ctx, `INSERT INTO workers (id, name, version, capacity, status, active_jobs, created_at, updated_at) VALUES ($1, 'w', '1.0', 8, 'ready', 0, now(), now()) ON CONFLICT DO NOTHING`, workerID)
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM workers WHERE id = $1`, workerID)
	})

	seedJob(t, pool, projectID, queueID, "noop")

	jobs, err := claim.ClaimBatch(ctx, pool, workerID, 1, 5*time.Second)
	if err != nil || len(jobs) == 0 {
		t.Fatalf("claim: %v, jobs=%d", err, len(jobs))
	}
	j := jobs[0]
	originalExpiry := j.LeaseExpiresAt

	time.Sleep(100 * time.Millisecond)

	// Extend to 60s
	if err := claim.ExtendLease(ctx, pool, j.ID, workerID, j.LeaseToken, 60*time.Second); err != nil {
		t.Fatalf("extend lease: %v", err)
	}

	var newExpiry time.Time
	pool.QueryRow(ctx, `SELECT lease_expires_at FROM jobs WHERE id = $1`, j.ID).Scan(&newExpiry)

	if !newExpiry.After(originalExpiry) {
		t.Errorf("expected extended expiry %v to be after original %v", newExpiry, originalExpiry)
	}
}
