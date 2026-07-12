package claim_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pulsequeue/worker/internal/claim"
)

func registerWorker(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO workers (id, name, version, capacity, status, active_jobs, created_at, updated_at)
		VALUES ($1, $2, 'test', 100, 'ready', 0, now(), now())
	`, id, fmt.Sprintf("worker-%s", id)); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM workers WHERE id = $1`, id) })
	return id
}

func claimAndRun(t *testing.T, pool *pgxpool.Pool, workerID uuid.UUID) claim.Job {
	t.Helper()
	jobs, err := claim.ClaimBatch(context.Background(), pool, workerID, 1, 30*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim one job: jobs=%d err=%v", len(jobs), err)
	}
	ok, err := claim.MarkRunningFenced(context.Background(), pool, jobs[0])
	if err != nil || !ok {
		t.Fatalf("mark running: ok=%t err=%v", ok, err)
	}
	return jobs[0]
}

func TestClaimBatchEnforcesConcurrencyWithinOneBatch(t *testing.T) {
	pool := testPool(t)
	projectID, queueID := seedEnv(t, pool)
	if _, err := pool.Exec(context.Background(), `UPDATE queues SET concurrency_limit = 2 WHERE id = $1`, queueID); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		seedJob(t, pool, projectID, queueID, "noop")
	}

	jobs, err := claim.ClaimBatch(context.Background(), pool, registerWorker(t, pool), 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("claimed %d jobs, want queue concurrency limit 2", len(jobs))
	}
}

func TestConcurrentClaimsRespectQueueConcurrency(t *testing.T) {
	pool := testPool(t)
	projectID, queueID := seedEnv(t, pool)
	if _, err := pool.Exec(context.Background(), `UPDATE queues SET concurrency_limit = 3 WHERE id = $1`, queueID); err != nil {
		t.Fatal(err)
	}
	for range 30 {
		seedJob(t, pool, projectID, queueID, "noop")
	}

	var wg sync.WaitGroup
	counts := make(chan int, 10)
	errs := make(chan error, 10)
	for range 10 {
		workerID := registerWorker(t, pool)
		wg.Add(1)
		go func() {
			defer wg.Done()
			jobs, err := claim.ClaimBatch(context.Background(), pool, workerID, 10, 30*time.Second)
			if err != nil {
				errs <- err
				return
			}
			counts <- len(jobs)
		}()
	}
	wg.Wait()
	close(counts)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent claim: %v", err)
	}
	total := 0
	for count := range counts {
		total += count
	}
	if total != 3 {
		t.Fatalf("concurrent workers claimed %d jobs, want exactly 3", total)
	}
}

func TestConcurrentClaimsRespectAvailableRateTokens(t *testing.T) {
	pool := testPool(t)
	projectID, queueID := seedEnv(t, pool)
	if _, err := pool.Exec(context.Background(), `
		UPDATE queues
		SET concurrency_limit = 100, rate_limit_per_minute = 5, rate_tokens = 2, rate_refilled_at = now()
		WHERE id = $1
	`, queueID); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		seedJob(t, pool, projectID, queueID, "noop")
	}

	var wg sync.WaitGroup
	counts := make(chan int, 5)
	for range 5 {
		workerID := registerWorker(t, pool)
		wg.Add(1)
		go func() {
			defer wg.Done()
			jobs, err := claim.ClaimBatch(context.Background(), pool, workerID, 20, 30*time.Second)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			counts <- len(jobs)
		}()
	}
	wg.Wait()
	close(counts)
	total := 0
	for count := range counts {
		total += count
	}
	if total != 2 {
		t.Fatalf("claimed %d jobs with 2 available tokens, want 2", total)
	}
}

func TestClaimCreatesExecutionAndClaimedEventAtomically(t *testing.T) {
	pool := testPool(t)
	projectID, queueID := seedEnv(t, pool)
	jobID := seedJob(t, pool, projectID, queueID, "noop")
	workerID := registerWorker(t, pool)

	jobs, err := claim.ClaimBatch(context.Background(), pool, workerID, 1, 30*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: jobs=%d err=%v", len(jobs), err)
	}
	if jobs[0].ExecutionID == uuid.Nil {
		t.Fatal("claimed job has no execution ID")
	}
	var executionCount, eventCount int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM job_executions WHERE job_id = $1 AND status = 'claimed'`, jobID).Scan(&executionCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM job_state_events WHERE job_id = $1 AND to_status = 'claimed' AND worker_id = $2`, jobID, workerID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if executionCount != 1 || eventCount != 1 {
		t.Fatalf("execution=%d event=%d, want one of each", executionCount, eventCount)
	}
}

func TestReclaimedLeaseRejectsStaleCompletionAndLeavesCurrentExecutionUnchanged(t *testing.T) {
	pool := testPool(t)
	projectID, queueID := seedEnv(t, pool)
	jobID := seedJob(t, pool, projectID, queueID, "noop")
	oldJob := claimAndRun(t, pool, registerWorker(t, pool))

	if _, err := pool.Exec(context.Background(), `
		UPDATE jobs
		SET status = 'retry_scheduled', attempt_count = attempt_count + 1,
		    scheduled_at = now(), claimed_by_worker_id = NULL,
		    lease_token = NULL, lease_expires_at = NULL
		WHERE id = $1
	`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE job_executions SET status = 'failed', finished_at = now() WHERE id = $1`, oldJob.ExecutionID); err != nil {
		t.Fatal(err)
	}
	currentJob := claimAndRun(t, pool, registerWorker(t, pool))

	ok, err := claim.CompleteResult(context.Background(), pool, oldJob, claim.ExecutionResult{Output: map[string]any{"ok": true}})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expired lease completed job")
	}
	var jobStatus, executionStatus string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT status FROM job_executions WHERE id = $1`, currentJob.ExecutionID).Scan(&executionStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "running" || executionStatus != "running" {
		t.Fatalf("job=%s execution=%s, want both running", jobStatus, executionStatus)
	}
}

func TestRetryableFailureUsesPolicyAndSchedulesRetry(t *testing.T) {
	pool := testPool(t)
	projectID, queueID := seedEnv(t, pool)
	policyID := uuid.New()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO retry_policies (id, project_id, name, strategy, base_delay_seconds, max_delay_seconds, max_attempts, created_at, updated_at)
		VALUES ($1, $2, 'linear-test', 'linear', 10, 60, 5, now(), now())
	`, policyID, projectID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE queues SET retry_policy_id = $1 WHERE id = $2`, policyID, queueID); err != nil {
		t.Fatal(err)
	}
	jobID := seedJob(t, pool, projectID, queueID, "noop")
	if _, err := pool.Exec(context.Background(), `UPDATE jobs SET max_attempts = 5, attempt_count = 1 WHERE id = $1`, jobID); err != nil {
		t.Fatal(err)
	}
	job := claimAndRun(t, pool, registerWorker(t, pool))
	before := time.Now().UTC()
	ok, err := claim.FailResult(context.Background(), pool, job, claim.ExecutionResult{Error: "temporary", Retryable: true})
	if err != nil || !ok {
		t.Fatalf("fail result: ok=%t err=%v", ok, err)
	}
	var status string
	var attempt int
	var scheduled time.Time
	if err := pool.QueryRow(context.Background(), `SELECT status, attempt_count, scheduled_at FROM jobs WHERE id = $1`, jobID).Scan(&status, &attempt, &scheduled); err != nil {
		t.Fatal(err)
	}
	if status != "retry_scheduled" || attempt != 2 {
		t.Fatalf("status=%s attempt=%d", status, attempt)
	}
	if scheduled.Before(before.Add(19*time.Second)) || scheduled.After(time.Now().UTC().Add(21*time.Second)) {
		t.Fatalf("scheduled_at=%v, want database now + 20s", scheduled)
	}
}

func TestNonRetryableFailureImmediatelyDeadLettersAtomically(t *testing.T) {
	pool := testPool(t)
	projectID, queueID := seedEnv(t, pool)
	jobID := seedJob(t, pool, projectID, queueID, "noop")
	job := claimAndRun(t, pool, registerWorker(t, pool))

	ok, err := claim.FailResult(context.Background(), pool, job, claim.ExecutionResult{
		Error:     "permanent",
		Retryable: false,
		Logs:      []claim.LogEntry{{Level: "error", Message: "handler rejected input", Fields: map[string]any{"code": "invalid"}}},
	})
	if err != nil || !ok {
		t.Fatalf("fail result: ok=%t err=%v", ok, err)
	}
	assertDeadLetteredOnce(t, pool, jobID, job.ExecutionID, "permanent")
}

func TestExhaustedFailureCreatesExactlyOneDeadLetterEntry(t *testing.T) {
	pool := testPool(t)
	projectID, queueID := seedEnv(t, pool)
	jobID := seedJob(t, pool, projectID, queueID, "noop")
	if _, err := pool.Exec(context.Background(), `UPDATE jobs SET attempt_count = 2, max_attempts = 3 WHERE id = $1`, jobID); err != nil {
		t.Fatal(err)
	}
	job := claimAndRun(t, pool, registerWorker(t, pool))
	ok, err := claim.FailResult(context.Background(), pool, job, claim.ExecutionResult{Error: "exhausted", Retryable: true})
	if err != nil || !ok {
		t.Fatalf("fail result: ok=%t err=%v", ok, err)
	}
	assertDeadLetteredOnce(t, pool, jobID, job.ExecutionID, "exhausted")

	ok, err = claim.FailResult(context.Background(), pool, job, claim.ExecutionResult{Error: "again", Retryable: true})
	if err != nil || ok {
		t.Fatalf("repeated stale fail: ok=%t err=%v", ok, err)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM dead_letter_entries WHERE job_id = $1`, jobID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("dead-letter rows=%d, want 1", count)
	}
}

func assertDeadLetteredOnce(t *testing.T, pool *pgxpool.Pool, jobID, executionID uuid.UUID, reason string) {
	t.Helper()
	var status, executionStatus, executionError string
	var dlqCount, logCount int
	if err := pool.QueryRow(context.Background(), `SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT status, error_message FROM job_executions WHERE id = $1`, executionID).Scan(&executionStatus, &executionError); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM dead_letter_entries WHERE job_id = $1 AND reason = $2`, jobID, reason).Scan(&dlqCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM job_logs WHERE job_id = $1 AND execution_id = $2`, jobID, executionID).Scan(&logCount); err != nil {
		t.Fatal(err)
	}
	if status != "dead_lettered" || executionStatus != "failed" || executionError != reason || dlqCount != 1 {
		t.Fatalf("status=%s execution=%s error=%q dlq=%d", status, executionStatus, executionError, dlqCount)
	}
	if reason == "permanent" && logCount != 1 {
		t.Fatalf("logs=%d, want 1 committed atomically", logCount)
	}
}
