package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pulsequeue/worker/internal/claim"
	"github.com/pulsequeue/worker/internal/config"
)

func newDrainTestWorker() *Worker {
	cfg := &config.Config{Concurrency: 1, BatchSize: 1, LeaseSeconds: 30}
	return New(cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func testJob() claim.Job {
	return claim.Job{ID: uuid.New(), TimeoutSeconds: 60}
}

func TestBeginDrainWaitsForClaimAdmissionBeforeDispatch(t *testing.T) {
	w := newDrainTestWorker()
	job := testJob()
	enteredDispatch := make(chan struct{})
	releaseDispatch := make(chan struct{})
	w.claimBatch = func(context.Context, *pgxpool.Pool, uuid.UUID, int, time.Duration) ([]claim.Job, error) {
		return []claim.Job{job}, nil
	}
	w.beforeDispatch = func() {
		close(enteredDispatch)
		<-releaseDispatch
	}
	w.runExecution = func(context.Context, claim.Job) {}

	claimed := make(chan struct{})
	go func() {
		w.claimAndDispatch(context.Background())
		close(claimed)
	}()
	<-enteredDispatch

	drainAttempted := make(chan struct{})
	w.beforeDrainAdmissionLock = func() { close(drainAttempted) }
	drainStarted := make(chan struct{})
	go func() {
		w.BeginDrain()
		close(drainStarted)
	}()
	<-drainAttempted
	select {
	case <-drainStarted:
		t.Fatal("drain completed while a claimed job had not been admitted")
	default:
	}

	close(releaseDispatch)
	<-claimed
	<-drainStarted
	w.Drain(time.Second)
}

func TestClaimAndDispatchDoesNotClaimAfterDrainCompletes(t *testing.T) {
	w := newDrainTestWorker()
	claimCalls := 0
	w.claimBatch = func(context.Context, *pgxpool.Pool, uuid.UUID, int, time.Duration) ([]claim.Job, error) {
		claimCalls++
		return []claim.Job{testJob()}, nil
	}
	w.runExecution = func(context.Context, claim.Job) {
		t.Fatal("dispatched work after drain completed")
	}

	w.BeginDrain()
	w.claimAndDispatch(context.Background())
	if claimCalls != 0 {
		t.Fatalf("claim batch invoked %d times after drain", claimCalls)
	}
}

func TestClaimLoopCancellationDoesNotCancelAdmittedExecution(t *testing.T) {
	w := newDrainTestWorker()
	job := testJob()
	w.claimBatch = func(context.Context, *pgxpool.Pool, uuid.UUID, int, time.Duration) ([]claim.Job, error) {
		return []claim.Job{job}, nil
	}
	started := make(chan context.Context, 1)
	releaseExecution := make(chan struct{})
	w.runExecution = func(jobCtx context.Context, _ claim.Job) {
		started <- jobCtx
		<-releaseExecution
	}

	claimLoopCtx, cancelClaimLoop := context.WithCancel(context.Background())
	defer cancelClaimLoop()
	go w.claimAndDispatch(claimLoopCtx)
	jobCtx := <-started
	cancelClaimLoop()

	select {
	case <-jobCtx.Done():
		t.Fatal("claim-loop cancellation cancelled an already-admitted execution")
	default:
	}
	close(releaseExecution)
	w.Drain(time.Second)
}

func TestDrainTimeoutCancelsRemainingExecutionsAndWaitsForUnwind(t *testing.T) {
	w := newDrainTestWorker()
	job := testJob()
	w.claimBatch = func(context.Context, *pgxpool.Pool, uuid.UUID, int, time.Duration) ([]claim.Job, error) {
		return []claim.Job{job}, nil
	}
	started := make(chan struct{})
	unwound := make(chan struct{})
	w.runExecution = func(jobCtx context.Context, _ claim.Job) {
		close(started)
		<-jobCtx.Done()
		close(unwound)
	}

	w.claimAndDispatch(context.Background())
	<-started
	w.Drain(10 * time.Millisecond)
	select {
	case <-unwound:
	case <-time.After(time.Second):
		t.Fatal("drain returned before a cancelled execution unwound")
	}
}

func TestLeaseExtensionLossCancelsOnlyOwningExecution(t *testing.T) {
	w := newDrainTestWorker()
	w.leaseInterval = func() time.Duration { return time.Millisecond }
	w.extendLease = func(context.Context, *pgxpool.Pool, claim.Job, time.Duration) (bool, error) {
		return false, nil
	}
	lostJob := testJob()
	otherJob := testJob()
	lostCtx, releaseLost := w.newExecutionContext(lostJob.ID, time.Minute)
	defer releaseLost()
	otherCtx, releaseOther := w.newExecutionContext(otherJob.ID, time.Minute)
	defer releaseOther()
	stop := make(chan struct{})
	defer close(stop)
	go w.extendLeaseLoop(lostCtx, lostJob, stop)

	select {
	case <-lostCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("zero-row lease extension did not cancel its owning execution")
	}
	select {
	case <-otherCtx.Done():
		t.Fatal("zero-row lease extension cancelled an unrelated execution")
	default:
	}
}

func TestCancelledHandlerUsesLiveContextForTerminalPersistence(t *testing.T) {
	w := newDrainTestWorker()
	markedRunning := make(chan struct{})
	w.markRunning = func(context.Context, *pgxpool.Pool, claim.Job) (bool, error) {
		close(markedRunning)
		return true, nil
	}
	finalized := make(chan struct{}, 1)
	w.failResult = func(ctx context.Context, _ *pgxpool.Pool, _ claim.Job, _ claim.ExecutionResult) (bool, error) {
		if ctx.Err() != nil {
			t.Error("terminal persistence received a cancelled handler context")
		}
		finalized <- struct{}{}
		return true, nil
	}
	jobCtx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		w.executeJob(jobCtx, claim.Job{
			ID: uuid.New(), Type: "chaos", TimeoutSeconds: 60,
			Payload: []byte(`{"mode":"timeout"}`),
		})
	}()
	<-markedRunning
	cancel()

	<-finalized
	<-finished
}
