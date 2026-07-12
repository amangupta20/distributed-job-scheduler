package handler_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pulsequeue/worker/internal/handler"
)

func TestNoopHandler(t *testing.T) {
	h := &handler.NoopHandler{}
	res := h.Execute(context.Background(), []byte("{}"))
	if !res.Success {
		t.Errorf("expected noop to succeed, got error: %s", res.Error)
	}
}

func TestChaosHandlerSucceed(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"mode": "succeed"})
	h := &handler.ChaosHandler{}
	res := h.Execute(context.Background(), payload)
	if !res.Success {
		t.Errorf("expected chaos succeed mode to succeed")
	}
}

func TestChaosHandlerFail(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"mode": "fail"})
	h := &handler.ChaosHandler{}
	res := h.Execute(context.Background(), payload)
	if res.Success {
		t.Errorf("expected chaos fail mode to fail")
	}
	if res.Retryable {
		t.Errorf("expected permanent failure to be non-retryable")
	}
}

func TestChaosHandlerFailFirstN(t *testing.T) {
	h := &handler.ChaosHandler{}

	// First 2 attempts should fail
	for attempt := 0; attempt < 2; attempt++ {
		payload, _ := json.Marshal(map[string]any{
			"mode":            "fail_first_n",
			"fail_first_n":    2,
			"current_attempt": attempt,
		})
		res := h.Execute(context.Background(), payload)
		if res.Success {
			t.Errorf("attempt %d should have failed", attempt)
		}
	}

	// Third attempt should succeed
	payload, _ := json.Marshal(map[string]any{
		"mode":            "fail_first_n",
		"fail_first_n":    2,
		"current_attempt": 2,
	})
	res := h.Execute(context.Background(), payload)
	if !res.Success {
		t.Errorf("attempt 2 should have succeeded, got: %s", res.Error)
	}
}

func TestRegistryUnknownType(t *testing.T) {
	reg := handler.New()
	h := reg.Get("nonexistent")
	if h != nil {
		t.Errorf("expected nil handler for unknown type")
	}
}

func TestRegistryKnownTypes(t *testing.T) {
	reg := handler.New()
	for _, typ := range []string{"noop", "http", "chaos"} {
		if reg.Get(typ) == nil {
			t.Errorf("expected handler for type %s", typ)
		}
	}
}
