package claim

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job represents a claimed job ready for execution.
type Job struct {
	ID             uuid.UUID
	QueueID        uuid.UUID
	ProjectID      uuid.UUID
	WorkerID       uuid.UUID
	ExecutionID    uuid.UUID
	Type           string
	Payload        []byte
	Priority       int
	AttemptCount   int
	MaxAttempts    int
	TimeoutSeconds int
	LeaseToken     string
	LeaseExpiresAt time.Time
	RetryPolicy    RetryPolicy
	PreviousStatus string
}

// LogEntry is a structured log record persisted with an execution outcome.
type LogEntry struct {
	Level   string
	Message string
	Fields  map[string]any
}

// ExecutionResult is the terminal result persisted by CompleteResult or FailResult.
type ExecutionResult struct {
	Output    map[string]any
	Logs      []LogEntry
	Retryable bool
	Error     string
}

// ClaimBatch claims jobs in one transaction. Eligible queue rows are locked in a
// stable order so concurrent workers share strict concurrency and rate budgets.
func ClaimBatch(ctx context.Context, pool *pgxpool.Pool, workerID uuid.UUID, batchSize int, leaseDuration time.Duration) ([]Job, error) {
	if batchSize <= 0 {
		return nil, nil
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	queueID, found, err := lockNextQueue(ctx, tx, true)
	if err != nil {
		return nil, err
	}
	if !found {
		// If every eligible queue is currently locked, wait for the highest-ranked
		// one. This lets concurrent workers share a busy queue over successive
		// transactions instead of returning empty batches indefinitely.
		queueID, found, err = lockNextQueue(ctx, tx, false)
		if err != nil {
			return nil, err
		}
	}
	if !found {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty claim: %w", err)
		}
		return nil, nil
	}
	queueIDArray := "{" + queueID.String() + "}"

	// Refill whole tokens using the database clock. Fractional elapsed time is
	// retained by advancing rate_refilled_at only by the time actually consumed.
	if _, err := tx.Exec(ctx, `
		WITH refill AS (
			SELECT id, rate_limit_per_minute, rate_tokens, rate_refilled_at,
			       floor(extract(epoch FROM (now() - COALESCE(rate_refilled_at, now())))
			             * rate_limit_per_minute / 60.0)::integer AS earned
			FROM queues
			WHERE id = ANY($1::uuid[]) AND rate_limit_per_minute IS NOT NULL
		)
		UPDATE queues q
		SET rate_tokens = LEAST(r.rate_limit_per_minute,
		                        COALESCE(r.rate_tokens, r.rate_limit_per_minute) + GREATEST(0, r.earned)),
		    rate_refilled_at = CASE
			WHEN r.rate_refilled_at IS NULL THEN now()
			WHEN COALESCE(r.rate_tokens, r.rate_limit_per_minute) + GREATEST(0, r.earned) >= r.rate_limit_per_minute THEN now()
			ELSE r.rate_refilled_at + (GREATEST(0, r.earned) * 60.0 / r.rate_limit_per_minute) * interval '1 second'
		END,
		    updated_at = now()
		FROM refill r
		WHERE q.id = r.id
	`, queueIDArray); err != nil {
		return nil, fmt.Errorf("refill queue rate tokens: %w", err)
	}

	rows, err := tx.Query(ctx, `
		WITH queue_capacity AS (
			SELECT q.id,
			       GREATEST(0, q.concurrency_limit - count(jr.id) FILTER (
			           WHERE jr.status IN ('claimed', 'running'))::integer) AS concurrency_slots,
			       CASE WHEN q.rate_limit_per_minute IS NULL THEN q.concurrency_limit
			            ELSE GREATEST(0, COALESCE(q.rate_tokens, 0)) END AS rate_slots,
			       q.priority
			FROM queues q
			LEFT JOIN jobs jr ON jr.queue_id = q.id
			WHERE q.id = ANY($1::uuid[]) AND q.pause_state = false
			GROUP BY q.id
		), candidates AS (
			SELECT picked.id, picked.previous_status, picked.score, picked.scheduled_at
			FROM queue_capacity qc
			CROSS JOIN LATERAL (
				SELECT j.id, j.status AS previous_status,
				       qc.priority + j.priority + LEAST(100,
				           floor(extract(epoch FROM (now() - j.scheduled_at)) / 60.0)::integer) AS score,
				       j.scheduled_at
				FROM jobs j
				WHERE j.queue_id = qc.id
				  AND j.status IN ('queued', 'retry_scheduled')
				  AND j.scheduled_at <= now()
				  AND j.lease_expires_at IS NULL
				ORDER BY score DESC, j.scheduled_at, j.id
				LIMIT LEAST(qc.concurrency_slots, qc.rate_slots)
				FOR UPDATE OF j SKIP LOCKED
			) picked
			ORDER BY picked.score DESC, picked.scheduled_at, picked.id
			LIMIT $2
		), updated AS (
			UPDATE jobs j
			SET status = 'claimed', claimed_by_worker_id = $3,
			    lease_token = gen_random_uuid()::text,
			    lease_expires_at = now() + $4::interval,
			    updated_at = now()
			FROM candidates c
			WHERE j.id = c.id
			RETURNING j.*, c.previous_status
		)
		SELECT u.id, u.queue_id, u.project_id, u.type, u.payload, u.priority,
		       u.attempt_count, u.max_attempts, u.timeout_seconds,
		       u.lease_token, u.lease_expires_at,
		       COALESCE(rp.strategy, 'fixed'),
		       COALESCE(rp.base_delay_seconds, 10),
		       COALESCE(rp.max_delay_seconds, 3600),
		       u.previous_status
		FROM updated u
		JOIN queues q ON q.id = u.queue_id
		LEFT JOIN retry_policies rp ON rp.id = COALESCE(u.retry_policy_id, q.retry_policy_id)
	`, queueIDArray, batchSize, workerID, leaseDuration.String())
	if err != nil {
		return nil, fmt.Errorf("claim query: %w", err)
	}

	var jobs []Job
	for rows.Next() {
		var job Job
		var strategy string
		var baseDelaySeconds, maxDelaySeconds int
		if err := rows.Scan(
			&job.ID, &job.QueueID, &job.ProjectID, &job.Type, &job.Payload,
			&job.Priority, &job.AttemptCount, &job.MaxAttempts, &job.TimeoutSeconds,
			&job.LeaseToken, &job.LeaseExpiresAt,
			&strategy, &baseDelaySeconds, &maxDelaySeconds, &job.PreviousStatus,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan claimed job: %w", err)
		}
		job.WorkerID = workerID
		job.ExecutionID = uuid.New()
		job.RetryPolicy = RetryPolicy{
			Strategy: strategy, BaseDelay: time.Duration(baseDelaySeconds) * time.Second,
			MaxDelay: time.Duration(maxDelaySeconds) * time.Second,
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate claimed jobs: %w", err)
	}
	rows.Close()

	for _, job := range jobs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO job_executions
			    (id, job_id, worker_id, attempt_number, status, started_at)
			VALUES ($1, $2, $3, $4, 'claimed', now())
		`, job.ExecutionID, job.ID, workerID, job.AttemptCount+1); err != nil {
			return nil, fmt.Errorf("insert claimed execution: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO job_state_events (id, job_id, from_status, to_status, worker_id, created_at)
			VALUES ($1, $2, $3, 'claimed', $4, now())
		`, uuid.New(), job.ID, job.PreviousStatus, workerID); err != nil {
			return nil, fmt.Errorf("insert claimed event: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE queues
		SET rate_tokens = rate_tokens - $1, updated_at = now()
		WHERE id = $2 AND rate_limit_per_minute IS NOT NULL
	`, len(jobs), queueID); err != nil {
		return nil, fmt.Errorf("consume queue rate tokens: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return jobs, nil
}

func lockNextQueue(ctx context.Context, tx pgx.Tx, skipLocked bool) (uuid.UUID, bool, error) {
	lockingClause := "FOR UPDATE OF q"
	if skipLocked {
		lockingClause += " SKIP LOCKED"
	}
	query := `
		WITH queue_usage AS (
			SELECT q.id,
			       count(jr.id) FILTER (WHERE jr.status IN ('claimed', 'running'))::integer AS active
			FROM queues q
			LEFT JOIN jobs jr ON jr.queue_id = q.id
			GROUP BY q.id
		), queue_heads AS (
			SELECT q.id, head.score, head.scheduled_at, head.job_id
			FROM queues q
			JOIN queue_usage u ON u.id = q.id AND u.active < q.concurrency_limit
			CROSS JOIN LATERAL (
				SELECT j.id AS job_id,
				       q.priority + j.priority + LEAST(100,
				           floor(extract(epoch FROM (now() - j.scheduled_at)) / 60.0)::integer) AS score,
				       j.scheduled_at
				FROM jobs j
				WHERE j.queue_id = q.id
				  AND j.status IN ('queued', 'retry_scheduled')
				  AND j.scheduled_at <= now()
				  AND j.lease_expires_at IS NULL
				ORDER BY score DESC, j.scheduled_at, j.id
				LIMIT 1
			) head
			WHERE q.pause_state = false
			  AND (
				q.rate_limit_per_minute IS NULL OR
				LEAST(q.rate_limit_per_minute,
				      COALESCE(q.rate_tokens, q.rate_limit_per_minute) +
				      GREATEST(0, floor(extract(epoch FROM
				          (now() - COALESCE(q.rate_refilled_at, now()))) *
				          q.rate_limit_per_minute / 60.0)::integer)) > 0
			  )
		)
		SELECT q.id
		FROM queues q
		JOIN queue_heads h ON h.id = q.id
		ORDER BY h.score DESC, h.scheduled_at, h.job_id, q.id
		LIMIT 1
		` + lockingClause

	var queueID uuid.UUID
	err := tx.QueryRow(ctx, query).Scan(&queueID)
	if err == pgx.ErrNoRows {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("lock next eligible queue: %w", err)
	}
	return queueID, true, nil
}

// MarkRunningFenced atomically transitions the job and its execution to running.
func MarkRunningFenced(ctx context.Context, pool *pgxpool.Pool, job Job) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin mark running: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, `
		UPDATE jobs SET status = 'running', updated_at = now()
		WHERE id = $1 AND claimed_by_worker_id = $2 AND lease_token = $3
		  AND lease_expires_at > now() AND status = 'claimed'
	`, job.ID, job.WorkerID, job.LeaseToken)
	if err != nil {
		return false, fmt.Errorf("mark running: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	tag, err = tx.Exec(ctx, `UPDATE job_executions SET status = 'running' WHERE id = $1 AND status = 'claimed'`, job.ExecutionID)
	if err != nil {
		return false, fmt.Errorf("mark execution running: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("mark execution running: current execution not found")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO job_state_events (id, job_id, from_status, to_status, worker_id, created_at)
		VALUES ($1, $2, 'claimed', 'running', $3, now())
	`, uuid.New(), job.ID, job.WorkerID); err != nil {
		return false, fmt.Errorf("insert running event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit mark running: %w", err)
	}
	return true, nil
}

// ExtendLeaseFenced extends a live claim using the database clock.
func ExtendLeaseFenced(ctx context.Context, pool *pgxpool.Pool, job Job, duration time.Duration) (bool, error) {
	tag, err := pool.Exec(ctx, `
		UPDATE jobs SET lease_expires_at = now() + $1::interval, updated_at = now()
		WHERE id = $2 AND claimed_by_worker_id = $3 AND lease_token = $4
		  AND lease_expires_at > now() AND status IN ('claimed', 'running')
	`, duration.String(), job.ID, job.WorkerID, job.LeaseToken)
	if err != nil {
		return false, fmt.Errorf("extend lease: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// CompleteResult commits the terminal job, execution, logs, and state event together.
func CompleteResult(ctx context.Context, pool *pgxpool.Pool, job Job, result ExecutionResult) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin complete: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = 'completed', completed_at = now(), claimed_by_worker_id = NULL,
		    lease_token = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND claimed_by_worker_id = $2 AND lease_token = $3
		  AND lease_expires_at > now() AND status = 'running'
	`, job.ID, job.WorkerID, job.LeaseToken)
	if err != nil {
		return false, fmt.Errorf("complete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	output, err := json.Marshal(result.Output)
	if err != nil {
		return false, fmt.Errorf("marshal execution output: %w", err)
	}
	tag, err = tx.Exec(ctx, `
		UPDATE job_executions
		SET status = 'completed', finished_at = now(), logs = $1, error_message = NULL
		WHERE id = $2 AND job_id = $3 AND worker_id = $4 AND status = 'running'
	`, string(output), job.ExecutionID, job.ID, job.WorkerID)
	if err != nil {
		return false, fmt.Errorf("complete execution: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("complete execution: current execution not found")
	}
	if err := insertLogs(ctx, tx, job, result.Logs); err != nil {
		return false, err
	}
	if err := insertStateEvent(ctx, tx, job, "running", "completed"); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit complete: %w", err)
	}
	return true, nil
}

// FailResult commits a retry or dead-letter transition and execution evidence together.
func FailResult(ctx context.Context, pool *pgxpool.Pool, job Job, result ExecutionResult) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin fail: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var attemptCount, maxAttempts int
	err = tx.QueryRow(ctx, `
		SELECT attempt_count, max_attempts FROM jobs
		WHERE id = $1 AND claimed_by_worker_id = $2 AND lease_token = $3
		  AND lease_expires_at > now() AND status = 'running'
		FOR UPDATE
	`, job.ID, job.WorkerID, job.LeaseToken).Scan(&attemptCount, &maxAttempts)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read job for fail: %w", err)
	}

	nextAttempt := attemptCount + 1
	toStatus := "retry_scheduled"
	deadLetter := !result.Retryable || nextAttempt >= maxAttempts
	if deadLetter {
		toStatus = "dead_lettered"
		tag, err := tx.Exec(ctx, `
			UPDATE jobs
			SET status = 'dead_lettered', attempt_count = $1,
			    claimed_by_worker_id = NULL, lease_token = NULL, lease_expires_at = NULL,
			    updated_at = now()
			WHERE id = $2 AND claimed_by_worker_id = $3 AND lease_token = $4
			  AND lease_expires_at > now() AND status = 'running'
		`, nextAttempt, job.ID, job.WorkerID, job.LeaseToken)
		if err != nil {
			return false, fmt.Errorf("dead-letter job: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return false, nil
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dead_letter_entries (id, job_id, reason, created_at)
			VALUES ($1, $2, $3, now())
		`, uuid.New(), job.ID, result.Error); err != nil {
			return false, fmt.Errorf("insert dead-letter entry: %w", err)
		}
	} else {
		delay := NextRetryAt(job.RetryPolicy, nextAttempt, time.Unix(0, 0)).Sub(time.Unix(0, 0))
		tag, err := tx.Exec(ctx, `
			UPDATE jobs
			SET status = 'retry_scheduled', attempt_count = $1,
			    scheduled_at = now() + $2::interval,
			    claimed_by_worker_id = NULL, lease_token = NULL, lease_expires_at = NULL,
			    updated_at = now()
			WHERE id = $3 AND claimed_by_worker_id = $4 AND lease_token = $5
			  AND lease_expires_at > now() AND status = 'running'
		`, nextAttempt, delay.String(), job.ID, job.WorkerID, job.LeaseToken)
		if err != nil {
			return false, fmt.Errorf("schedule retry: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return false, nil
		}
	}

	output, err := json.Marshal(result.Output)
	if err != nil {
		return false, fmt.Errorf("marshal failed execution output: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE job_executions
		SET status = 'failed', finished_at = now(), logs = $1, error_message = $2
		WHERE id = $3 AND job_id = $4 AND worker_id = $5 AND status = 'running'
	`, string(output), result.Error, job.ExecutionID, job.ID, job.WorkerID)
	if err != nil {
		return false, fmt.Errorf("fail execution: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("fail execution: current execution not found")
	}
	if err := insertLogs(ctx, tx, job, result.Logs); err != nil {
		return false, err
	}
	if err := insertStateEvent(ctx, tx, job, "running", toStatus); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit fail: %w", err)
	}
	return true, nil
}

func insertLogs(ctx context.Context, tx pgx.Tx, job Job, logs []LogEntry) error {
	for _, entry := range logs {
		fields, err := json.Marshal(entry.Fields)
		if err != nil {
			return fmt.Errorf("marshal log fields: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO job_logs (id, job_id, execution_id, level, message, payload, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, now())
		`, uuid.New(), job.ID, job.ExecutionID, entry.Level, entry.Message, string(fields)); err != nil {
			return fmt.Errorf("insert execution log: %w", err)
		}
	}
	return nil
}

func insertStateEvent(ctx context.Context, tx pgx.Tx, job Job, from, to string) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO job_state_events (id, job_id, from_status, to_status, worker_id, created_at)
		VALUES ($1, $2, $3, $4, $5, now())
	`, uuid.New(), job.ID, from, to, job.WorkerID); err != nil {
		return fmt.Errorf("insert %s event: %w", to, err)
	}
	return nil
}

// Compatibility wrappers retain the original worker-facing API while fenced
// result-aware call sites migrate independently.
func MarkRunning(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, leaseToken string) error {
	job, err := loadCurrentJob(ctx, pool, jobID, workerID, leaseToken)
	if err != nil {
		return err
	}
	ok, err := MarkRunningFenced(ctx, pool, job)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("stale lease token or job already transitioned: %s", jobID)
	}
	return nil
}

func Complete(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, leaseToken string) error {
	job, err := loadCurrentJob(ctx, pool, jobID, workerID, leaseToken)
	if err != nil {
		return err
	}
	ok, err := CompleteResult(ctx, pool, job, ExecutionResult{})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("stale lease token or already transitioned: %s", jobID)
	}
	return nil
}

func FailJob(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, leaseToken, reason string) error {
	job, err := loadCurrentJob(ctx, pool, jobID, workerID, leaseToken)
	if err != nil {
		return err
	}
	ok, err := FailResult(ctx, pool, job, ExecutionResult{Error: reason, Retryable: true})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("stale lease token or already transitioned: %s", jobID)
	}
	return nil
}

func ExtendLease(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, leaseToken string, duration time.Duration) error {
	job := Job{ID: jobID, WorkerID: workerID, LeaseToken: leaseToken}
	ok, err := ExtendLeaseFenced(ctx, pool, job, duration)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("stale lease, cannot extend: %s", jobID)
	}
	return nil
}

func loadCurrentJob(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, leaseToken string) (Job, error) {
	job := Job{ID: jobID, WorkerID: workerID, LeaseToken: leaseToken}
	var strategy string
	var baseSeconds, maxSeconds int
	err := pool.QueryRow(ctx, `
		SELECT e.id, j.queue_id, j.project_id, j.attempt_count, j.max_attempts,
		       COALESCE(rp.strategy, 'fixed'), COALESCE(rp.base_delay_seconds, 10),
		       COALESCE(rp.max_delay_seconds, 3600)
		FROM jobs j
		JOIN queues q ON q.id = j.queue_id
		JOIN job_executions e ON e.job_id = j.id AND e.attempt_number = j.attempt_count + 1
		LEFT JOIN retry_policies rp ON rp.id = COALESCE(j.retry_policy_id, q.retry_policy_id)
		WHERE j.id = $1 AND j.claimed_by_worker_id = $2 AND j.lease_token = $3
	`, jobID, workerID, leaseToken).Scan(
		&job.ExecutionID, &job.QueueID, &job.ProjectID, &job.AttemptCount, &job.MaxAttempts,
		&strategy, &baseSeconds, &maxSeconds,
	)
	if err == pgx.ErrNoRows {
		return Job{}, fmt.Errorf("stale lease token or job already transitioned: %s", jobID)
	}
	if err != nil {
		return Job{}, fmt.Errorf("load current claim: %w", err)
	}
	job.RetryPolicy = RetryPolicy{Strategy: strategy, BaseDelay: time.Duration(baseSeconds) * time.Second, MaxDelay: time.Duration(maxSeconds) * time.Second}
	return job, nil
}
