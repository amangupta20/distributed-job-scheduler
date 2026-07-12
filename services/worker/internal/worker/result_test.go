package worker

import (
	"testing"

	"github.com/pulsequeue/worker/internal/handler"
)

func TestPersistenceResultPreservesRetryabilityAndLogs(t *testing.T) {
	got := persistenceResult(handler.Result{
		Success:   false,
		Retryable: false,
		Error:     "invalid input",
		Output:    map[string]any{"code": 400},
		Logs: []handler.LogEntry{{
			Level: "error", Message: "rejected", Fields: map[string]any{"field": "url"},
		}},
	})
	if got.Retryable {
		t.Fatal("non-retryable handler result became retryable")
	}
	if got.Error != "invalid input" || len(got.Logs) != 1 || got.Logs[0].Fields["field"] != "url" {
		t.Fatalf("result evidence was not preserved: %+v", got)
	}
}
