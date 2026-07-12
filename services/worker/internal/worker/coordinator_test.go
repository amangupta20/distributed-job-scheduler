package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pulsequeue/worker/internal/config"
)

func newDrainTestWorker() *Worker {
	cfg := &config.Config{Concurrency: 1, BatchSize: 1, LeaseSeconds: 30}
	return New(cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestShutdownDoesNotCancelValidInFlightExecution(t *testing.T) {
	w := newDrainTestWorker()
	jobID := uuid.New()
	jobCtx, release := w.newExecutionContext(jobID, time.Minute)
	defer release()

	w.BeginDrain()
	shutdownCtx, stop := context.WithCancel(context.Background())
	stop()
	if shutdownCtx.Err() == nil {
		t.Fatal("test shutdown context was not cancelled")
	}

	select {
	case <-jobCtx.Done():
		t.Fatal("valid in-flight execution was cancelled by drain")
	default:
	}
}

func TestDrainTimeoutCancelsRemainingExecutions(t *testing.T) {
	w := newDrainTestWorker()
	jobCtx, release := w.newExecutionContext(uuid.New(), time.Minute)
	defer release()

	w.activeJobs.Add(1)
	done := make(chan struct{})
	go func() {
		defer w.activeJobs.Done()
		defer close(done)
		<-jobCtx.Done()
	}()

	w.Drain(10 * time.Millisecond)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drain timeout did not cancel remaining execution")
	}
}

func TestLeaseLossCancelsOnlyOwningExecution(t *testing.T) {
	w := newDrainTestWorker()
	lostID := uuid.New()
	otherID := uuid.New()
	lostCtx, releaseLost := w.newExecutionContext(lostID, time.Minute)
	defer releaseLost()
	otherCtx, releaseOther := w.newExecutionContext(otherID, time.Minute)
	defer releaseOther()

	if !w.cancelExecution(lostID) {
		t.Fatal("expected lease-loss cancellation to find active execution")
	}
	select {
	case <-lostCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("lost lease did not cancel owning execution")
	}
	select {
	case <-otherCtx.Done():
		t.Fatal("lost lease cancelled an unrelated execution")
	default:
	}
}

func TestDrainPreventsClaims(t *testing.T) {
	w := newDrainTestWorker()
	w.BeginDrain()
	if w.AcceptingClaims() {
		t.Fatal("worker accepted claims after drain began")
	}

	// A nil pool would panic if the guard allowed a claim attempt.
	w.claimAndDispatch(context.Background())
}
