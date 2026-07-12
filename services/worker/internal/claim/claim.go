package claim

import (
	"context"
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
	Type           string
	Payload        []byte
	Priority       int
	AttemptCount   int
	MaxAttempts    int
	TimeoutSeconds int
	LeaseToken     string
	LeaseExpiresAt time.Time
}

// ClaimBatch atomically claims up to batchSize jobs using FOR UPDATE SKIP LOCKED.
// Paused queues and queues at their concurrency limit are excluded.
// Returns the list of claimed jobs with fresh lease tokens.
func ClaimBatch(ctx context.Context, pool *pgxpool.Pool, workerID uuid.UUID, batchSize int, leaseDuration time.Duration) ([]Job, error) {
	leaseToken := uuid.New().String()
	leaseExpiresAt := time.Now().UTC().Add(leaseDuration)

	const claimSQL = `
WITH candidates AS (
    SELECT j.id
    FROM jobs j
    JOIN queues q ON q.id = j.queue_id
    WHERE j.status = 'queued'
      AND q.pause_state = false
      AND (
        SELECT COUNT(*) FROM jobs running
        WHERE running.queue_id = q.id
          AND running.status IN ('claimed','running')
      ) < q.concurrency_limit
    ORDER BY
        j.priority DESC,
        j.scheduled_at ASC,
        j.id ASC
    LIMIT $4
    FOR UPDATE OF j SKIP LOCKED
)
UPDATE jobs
SET
    status             = 'claimed',
    claimed_by_worker_id = $1,
    lease_token        = $2,
    lease_expires_at   = $3,
    updated_at         = now()
FROM candidates
WHERE jobs.id = candidates.id
RETURNING
    jobs.id,
    jobs.queue_id,
    jobs.project_id,
    jobs.type,
    jobs.payload,
    jobs.priority,
    jobs.attempt_count,
    jobs.max_attempts,
    jobs.timeout_seconds,
    jobs.lease_token,
    jobs.lease_expires_at
`
	rows, err := pool.Query(ctx, claimSQL, workerID, leaseToken, leaseExpiresAt, batchSize)
	if err != nil {
		return nil, fmt.Errorf("claim query: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(
			&j.ID, &j.QueueID, &j.ProjectID,
			&j.Type, &j.Payload,
			&j.Priority, &j.AttemptCount, &j.MaxAttempts, &j.TimeoutSeconds,
			&j.LeaseToken, &j.LeaseExpiresAt,
		); err != nil {
			return nil, fmt.Errorf("scan claimed job: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// MarkRunning transitions a claimed job to running state. Validates lease token for fencing.
func MarkRunning(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, leaseToken string) error {
	tag, err := pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'running', updated_at = now()
		WHERE id = $1 AND claimed_by_worker_id = $2 AND lease_token = $3 AND status = 'claimed'
	`, jobID, workerID, leaseToken)
	if err != nil {
		return fmt.Errorf("mark running: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("stale lease token or job already transitioned: %s", jobID)
	}
	return nil
}

// Complete marks a job as successfully completed. Validates lease token for fencing.
func Complete(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, leaseToken string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = 'completed', completed_at = now(),
		    claimed_by_worker_id = NULL, lease_token = NULL, lease_expires_at = NULL,
		    updated_at = now()
		WHERE id = $1 AND claimed_by_worker_id = $2 AND lease_token = $3
		  AND status = 'running'
	`, jobID, workerID, leaseToken)
	if err != nil {
		return fmt.Errorf("complete update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("stale lease token or already transitioned: %s", jobID)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO job_state_events (job_id, from_status, to_status)
		VALUES ($1, 'running', 'completed')
	`, jobID); err != nil {
		return fmt.Errorf("state event: %w", err)
	}

	return tx.Commit(ctx)
}

// FailJob transitions a job from running to retry_scheduled or dead_lettered.
// Returns error if the lease token is stale (fencing).
func FailJob(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, leaseToken string, failReason string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var attemptCount, maxAttempts int
	err = tx.QueryRow(ctx, `
		SELECT attempt_count, max_attempts FROM jobs
		WHERE id = $1 AND claimed_by_worker_id = $2 AND lease_token = $3 AND status = 'running'
		FOR UPDATE
	`, jobID, workerID, leaseToken).Scan(&attemptCount, &maxAttempts)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("stale lease token or already transitioned: %s", jobID)
	}
	if err != nil {
		return fmt.Errorf("read job for fail: %w", err)
	}

	newAttemptCount := attemptCount + 1
	var newStatus, fromStatus string
	fromStatus = "running"

	if newAttemptCount >= maxAttempts {
		newStatus = "dead_lettered"
		_, err = tx.Exec(ctx, `
			UPDATE jobs
			SET status = 'dead_lettered', attempt_count = $1,
			    claimed_by_worker_id = NULL, lease_token = NULL, lease_expires_at = NULL,
			    updated_at = now()
			WHERE id = $2
		`, newAttemptCount, jobID)
		if err != nil {
			return fmt.Errorf("update to dead_lettered: %w", err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO dead_letter_entries (job_id, reason) VALUES ($1, $2)
		`, jobID, failReason)
		if err != nil {
			return fmt.Errorf("dead letter insert: %w", err)
		}
	} else {
		newStatus = "retry_scheduled"
		// Simple fixed 10s backoff; scheduler tick will pick up exponential policy
		retryAt := time.Now().UTC().Add(10 * time.Second)
		_, err = tx.Exec(ctx, `
			UPDATE jobs
			SET status = 'retry_scheduled', attempt_count = $1, scheduled_at = $2,
			    claimed_by_worker_id = NULL, lease_token = NULL, lease_expires_at = NULL,
			    updated_at = now()
			WHERE id = $3
		`, newAttemptCount, retryAt, jobID)
		if err != nil {
			return fmt.Errorf("update to retry_scheduled: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO job_state_events (job_id, from_status, to_status)
		VALUES ($1, $2, $3)
	`, jobID, fromStatus, newStatus); err != nil {
		return fmt.Errorf("state event: %w", err)
	}

	return tx.Commit(ctx)
}

// ExtendLease refreshes the lease expiration for an in-progress job.
func ExtendLease(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, leaseToken string, leaseDuration time.Duration) error {
	newExpiry := time.Now().UTC().Add(leaseDuration)
	tag, err := pool.Exec(ctx, `
		UPDATE jobs SET lease_expires_at = $1, updated_at = now()
		WHERE id = $2 AND claimed_by_worker_id = $3 AND lease_token = $4
		  AND status IN ('claimed', 'running')
	`, newExpiry, jobID, workerID, leaseToken)
	if err != nil {
		return fmt.Errorf("extend lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("stale lease, cannot extend: %s", jobID)
	}
	return nil
}
