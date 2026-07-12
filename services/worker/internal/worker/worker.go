package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pulsequeue/worker/internal/claim"
	"github.com/pulsequeue/worker/internal/config"
	"github.com/pulsequeue/worker/internal/handler"
)

// Worker orchestrates job claiming, execution, heartbeating, and graceful drain.
type Worker struct {
	id       uuid.UUID
	cfg      *config.Config
	pool     *pgxpool.Pool
	handlers handler.Registry
	log      *slog.Logger

	activeJobs sync.WaitGroup
	sem        chan struct{} // bounded goroutine pool

	// admissionMu makes the transition to draining atomic with respect to a
	// claim-and-dispatch cycle. A drain cannot start after a job is claimed but
	// before it is counted as active.
	admissionMu sync.Mutex
	draining    atomic.Bool
	jobsMu      sync.Mutex
	cancels     map[uuid.UUID]context.CancelFunc

	claimBatch               func(context.Context, *pgxpool.Pool, uuid.UUID, int, time.Duration) ([]claim.Job, error)
	runExecution             func(context.Context, claim.Job)
	markRunning              func(context.Context, *pgxpool.Pool, claim.Job) (bool, error)
	completeResult           func(context.Context, *pgxpool.Pool, claim.Job, claim.ExecutionResult) (bool, error)
	failResult               func(context.Context, *pgxpool.Pool, claim.Job, claim.ExecutionResult) (bool, error)
	extendLease              func(context.Context, *pgxpool.Pool, claim.Job, time.Duration) (bool, error)
	leaseInterval            func() time.Duration
	beforeDispatch           func()
	beforeDrainAdmissionLock func()
}

// New creates a new Worker.
func New(cfg *config.Config, pool *pgxpool.Pool, log *slog.Logger) *Worker {
	w := &Worker{
		id:       uuid.New(),
		cfg:      cfg,
		pool:     pool,
		handlers: handler.New(),
		log:      log,
		sem:      make(chan struct{}, cfg.Concurrency),
		cancels:  make(map[uuid.UUID]context.CancelFunc),
	}
	w.claimBatch = claim.ClaimBatch
	w.runExecution = w.executeJob
	w.markRunning = claim.MarkRunningFenced
	w.completeResult = claim.CompleteResult
	w.failResult = claim.FailResult
	w.extendLease = claim.ExtendLeaseFenced
	w.leaseInterval = func() time.Duration {
		interval := w.cfg.LeaseDuration() / 2
		if interval < time.Second {
			return time.Second
		}
		return interval
	}
	return w
}

// ID returns the worker's UUID.
func (w *Worker) ID() uuid.UUID { return w.id }

// BeginDrain prevents new claims while allowing in-flight executions to finish.
func (w *Worker) BeginDrain() {
	if w.beforeDrainAdmissionLock != nil {
		w.beforeDrainAdmissionLock()
	}
	w.admissionMu.Lock()
	defer w.admissionMu.Unlock()
	w.draining.Store(true)
}

// AcceptingClaims reports whether this worker may claim more work.
func (w *Worker) AcceptingClaims() bool {
	return !w.draining.Load()
}

// Register registers this worker with the control plane API.
func (w *Worker) Register(ctx context.Context) error {
	_, err := w.pool.Exec(ctx, `
		INSERT INTO workers (id, name, version, capacity, status)
		VALUES ($1, $2, $3, $4, 'ready')
		ON CONFLICT (id) DO UPDATE
		  SET name = EXCLUDED.name, version = EXCLUDED.version,
		      capacity = EXCLUDED.capacity, status = 'ready', updated_at = now()
	`, w.id, w.cfg.WorkerName, w.cfg.WorkerVersion, w.cfg.Concurrency)
	return err
}

// Deregister marks this worker as draining.
func (w *Worker) Deregister(ctx context.Context) error {
	_, err := w.pool.Exec(ctx, `
		UPDATE workers SET status = 'draining', updated_at = now() WHERE id = $1
	`, w.id)
	return err
}

// MarkOffline records that this process has finished its drain procedure.
func (w *Worker) MarkOffline(ctx context.Context) error {
	_, err := w.pool.Exec(ctx, `
		UPDATE workers SET status = 'offline', updated_at = now() WHERE id = $1
	`, w.id)
	return err
}

// Run is the main worker loop. It claims jobs, dispatches them to the goroutine pool,
// sends heartbeats, and extends leases. It returns when ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	defer w.BeginDrain()
	pollTicker := time.NewTicker(w.cfg.PollInterval())
	heartbeatTicker := time.NewTicker(w.cfg.HeartbeatInterval())
	defer pollTicker.Stop()
	defer heartbeatTicker.Stop()

	// Start LISTEN loop for pg_notify
	notifyCh := make(chan struct{}, 1)
	go w.listenNotify(ctx, notifyCh)

	for {
		select {
		case <-ctx.Done():
			return
		case <-pollTicker.C:
			w.claimAndDispatch(ctx)
		case <-notifyCh:
			w.claimAndDispatch(ctx)
		case <-heartbeatTicker.C:
			w.sendHeartbeat(ctx)
		}
	}
}

// Drain waits for all active jobs to finish (up to drain timeout).
func (w *Worker) Drain(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		w.activeJobs.Wait()
		close(done)
	}()
	select {
	case <-done:
		w.log.Info("graceful drain complete")
	case <-time.After(timeout):
		w.log.Warn("drain timeout exceeded, some jobs may not have completed")
		w.cancelAllExecutions()
		select {
		case <-done:
			w.log.Info("cancelled executions unwound before shutdown")
		case <-time.After(time.Second):
			w.log.Warn("cancelled executions did not unwind before shutdown grace period")
		}
	}
}

// listenNotify subscribes to PostgreSQL LISTEN jobs_available notifications.
func (w *Worker) listenNotify(ctx context.Context, notifyCh chan<- struct{}) {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		w.log.Error("acquire conn for LISTEN", "err", err)
		return
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN jobs_available"); err != nil {
		w.log.Error("LISTEN jobs_available", "err", err)
		return
	}
	w.log.Info("listening for pg_notify jobs_available")

	for {
		_, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Warn("pg_notify wait error", "err", err)
			return
		}
		select {
		case notifyCh <- struct{}{}:
		default:
		}
	}
}

// claimAndDispatch claims a batch of jobs and dispatches each to the goroutine pool.
func (w *Worker) claimAndDispatch(ctx context.Context) {
	w.admissionMu.Lock()
	defer w.admissionMu.Unlock()
	if !w.AcceptingClaims() {
		return
	}
	if ctx.Err() != nil {
		return
	}
	// Only claim up to available goroutine slots
	available := cap(w.sem) - len(w.sem)
	if available <= 0 {
		return
	}
	batchSize := available
	if batchSize > w.cfg.BatchSize {
		batchSize = w.cfg.BatchSize
	}

	jobs, err := w.claimBatch(ctx, w.pool, w.id, batchSize, w.cfg.LeaseDuration())
	if err != nil {
		if ctx.Err() == nil {
			w.log.Error("claim batch", "err", err)
		}
		return
	}
	if len(jobs) == 0 {
		return
	}
	if ctx.Err() != nil || !w.AcceptingClaims() {
		return
	}
	w.log.Info("claimed jobs", "count", len(jobs))

	for _, j := range jobs {
		if w.beforeDispatch != nil {
			w.beforeDispatch()
		}
		w.dispatch(j)
	}
}

// dispatch admits a job under admissionMu, then gives it a context that is
// independent from the claim loop's shutdown context.
func (w *Worker) dispatch(j claim.Job) {
	w.sem <- struct{}{} // acquire slot
	jobCtx, release := w.newExecutionContext(j.ID, timeoutForJob(j))
	w.activeJobs.Add(1)
	go func() {
		defer func() {
			release()
			<-w.sem // release slot
			w.activeJobs.Done()
		}()
		w.runExecution(jobCtx, j)
	}()
}

func timeoutForJob(j claim.Job) time.Duration {
	timeout := time.Duration(j.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		return 5 * time.Minute
	}
	return timeout
}

// executeJob runs a single job to completion, handling lease extension and result reporting.
func (w *Worker) executeJob(jobCtx context.Context, j claim.Job) {
	log := w.log.With("job_id", j.ID, "type", j.Type, "attempt", j.AttemptCount+1)
	log.Info("executing job")

	if ok, err := w.markRunning(jobCtx, w.pool, j); err != nil {
		log.Warn("mark running failed", "err", err)
		return
	} else if !ok {
		log.Warn("mark running rejected stale or expired lease")
		return
	}

	// Start lease extension in background
	stopExtend := make(chan struct{})
	go w.extendLeaseLoop(jobCtx, j, stopExtend)
	defer close(stopExtend)

	startTime := time.Now()

	// Look up and run handler
	h := w.handlers.Get(j.Type)
	var result handler.Result
	if h == nil {
		result = handler.Result{
			Success: false, Retryable: false,
			Error: fmt.Sprintf("unknown job type: %s", j.Type),
		}
	} else {
		result = h.Execute(jobCtx, j.Payload)
	}
	elapsed := time.Since(startTime)
	persisted := persistenceResult(result)
	finalizeCtx, releaseFinalize := w.finalizationContext()
	defer releaseFinalize()

	// Finalize
	if result.Success {
		log.Info("job completed", "elapsed_ms", elapsed.Milliseconds())
		if ok, err := w.completeResult(finalizeCtx, w.pool, j, persisted); err != nil {
			log.Error("complete job", "err", err)
		} else if !ok {
			log.Warn("complete job rejected stale or expired lease")
		}
	} else {
		log.Warn("job failed", "elapsed_ms", elapsed.Milliseconds(), "error", result.Error, "retryable", result.Retryable)
		if ok, err := w.failResult(finalizeCtx, w.pool, j, persisted); err != nil {
			log.Error("fail job", "err", err)
		} else if !ok {
			log.Warn("fail job rejected stale or expired lease")
		}
	}
}

func persistenceResult(result handler.Result) claim.ExecutionResult {
	logs := make([]claim.LogEntry, len(result.Logs))
	for i, entry := range result.Logs {
		logs[i] = claim.LogEntry{Level: entry.Level, Message: entry.Message, Fields: entry.Fields}
	}
	return claim.ExecutionResult{
		Output: result.Output, Logs: logs, Retryable: result.Retryable, Error: result.Error,
	}
}

// extendLeaseLoop periodically extends the job's lease until stopped.
func (w *Worker) extendLeaseLoop(ctx context.Context, j claim.Job, stop <-chan struct{}) {
	ticker := time.NewTicker(w.leaseInterval())
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ok, err := w.extendLease(ctx, w.pool, j, w.cfg.LeaseDuration()); err != nil {
				w.log.Warn("extend lease", "job_id", j.ID, "err", err)
			} else if !ok {
				w.log.Warn("extend lease rejected stale or expired lease", "job_id", j.ID)
				w.cancelExecution(j.ID)
				return
			}
		}
	}
}

func (w *Worker) finalizationContext() (context.Context, func()) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// newExecutionContext deliberately detaches handler lifetime from the claim-loop
// context. Shutdown stops claims, while a drain timeout or lost lease cancels only
// the affected active execution.
func (w *Worker) newExecutionContext(jobID uuid.UUID, timeout time.Duration) (context.Context, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	w.jobsMu.Lock()
	w.cancels[jobID] = cancel
	w.jobsMu.Unlock()
	return ctx, func() {
		cancel()
		w.jobsMu.Lock()
		delete(w.cancels, jobID)
		w.jobsMu.Unlock()
	}
}

func (w *Worker) cancelExecution(jobID uuid.UUID) bool {
	w.jobsMu.Lock()
	cancel, ok := w.cancels[jobID]
	w.jobsMu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

func (w *Worker) cancelAllExecutions() {
	w.jobsMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(w.cancels))
	for _, cancel := range w.cancels {
		cancels = append(cancels, cancel)
	}
	w.jobsMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// sendHeartbeat records a worker heartbeat row.
func (w *Worker) sendHeartbeat(ctx context.Context) {
	activeCount := len(w.sem)
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO worker_heartbeats (worker_id, status, active_jobs)
		VALUES ($1, 'ready', $2)
	`, w.id, activeCount); err != nil {
		if ctx.Err() == nil {
			w.log.Warn("heartbeat write failed", "err", err)
		}
	}
}

// waitForDB retries connecting to PostgreSQL until the context is cancelled.
func WaitForDB(ctx context.Context, connStr string, log *slog.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse db url: %w", err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	for {
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err := pool.Ping(ctx); err == nil {
				log.Info("connected to database")
				return pool, nil
			}
			pool.Close()
		}
		log.Info("waiting for database...", "err", err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for database")
		case <-time.After(2 * time.Second):
		}
	}
}
