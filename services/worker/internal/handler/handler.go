package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Result is the uniform output of every job handler.
type Result struct {
	Success  bool
	Output   map[string]any
	Logs     []LogEntry
	Retryable bool
	Error    string
}

// LogEntry is a structured log line produced by a handler.
type LogEntry struct {
	Level   string
	Message string
	Fields  map[string]any
}

// Handler executes a job payload and returns a Result.
type Handler interface {
	Execute(ctx context.Context, payload []byte) Result
}

// Registry maps job type strings to Handler implementations.
type Registry map[string]Handler

// New returns a pre-populated handler registry with built-in types.
func New() Registry {
	return Registry{
		"noop":  &NoopHandler{},
		"http":  &HTTPHandler{},
		"chaos": &ChaosHandler{},
	}
}

// Get returns the handler for the given type, or nil if not found.
func (r Registry) Get(jobType string) Handler {
	return r[strings.ToLower(jobType)]
}

// ── Noop ──────────────────────────────────────────────────────────────────────

// NoopHandler immediately succeeds. Useful for benchmarks and deterministic tests.
type NoopHandler struct{}

func (h *NoopHandler) Execute(_ context.Context, payload []byte) Result {
	return Result{
		Success: true,
		Output:  map[string]any{"type": "noop"},
		Logs:    []LogEntry{{Level: "info", Message: "noop job executed"}},
	}
}

// ── HTTP ──────────────────────────────────────────────────────────────────────

type httpPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
	TimeoutSeconds int        `json:"timeout_seconds"`
}

// HTTPHandler performs a single outbound HTTP request.
type HTTPHandler struct{}

func (h *HTTPHandler) Execute(ctx context.Context, payload []byte) Result {
	var p httpPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return Result{Success: false, Error: fmt.Sprintf("invalid payload: %v", err), Retryable: false}
	}
	if p.Method == "" {
		p.Method = "POST"
	}
	if p.TimeoutSeconds <= 0 {
		p.TimeoutSeconds = 30
	}

	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(p.TimeoutSeconds)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, strings.ToUpper(p.Method), p.URL, strings.NewReader(p.Body))
	if err != nil {
		return Result{Success: false, Error: fmt.Sprintf("build request: %v", err), Retryable: false}
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return Result{Success: false, Error: fmt.Sprintf("http request: %v", err), Retryable: true}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	success := resp.StatusCode >= 200 && resp.StatusCode < 300

	return Result{
		Success:   success,
		Retryable: !success && resp.StatusCode >= 500,
		Output:    map[string]any{"status_code": resp.StatusCode, "body_preview": string(body[:min(len(body), 256)])},
		Logs:      []LogEntry{{Level: "info", Message: fmt.Sprintf("http %s %s -> %d", p.Method, p.URL, resp.StatusCode)}},
		Error:     func() string {
			if !success {
				return fmt.Sprintf("non-2xx status %d", resp.StatusCode)
			}
			return ""
		}(),
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ── Chaos ─────────────────────────────────────────────────────────────────────

type chaosPayload struct {
	Mode            string `json:"mode"` // succeed|fail|timeout|fail_first_n
	SleepMillis     int    `json:"sleep_ms"`
	FailFirstN      int    `json:"fail_first_n"`
	CurrentAttempt  int    `json:"current_attempt"`
}

// ChaosHandler supports synthetic failure scenarios for testing.
type ChaosHandler struct{}

func (h *ChaosHandler) Execute(ctx context.Context, payload []byte) Result {
	var p chaosPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return Result{Success: false, Error: fmt.Sprintf("invalid chaos payload: %v", err)}
	}

	if p.SleepMillis > 0 {
		select {
		case <-time.After(time.Duration(p.SleepMillis) * time.Millisecond):
		case <-ctx.Done():
			return Result{Success: false, Error: "timeout during sleep", Retryable: true}
		}
	}

	switch p.Mode {
	case "fail":
		return Result{Success: false, Error: "permanent failure (chaos)", Retryable: false}
	case "timeout":
		select {
		case <-ctx.Done():
			return Result{Success: false, Error: "chaos timeout", Retryable: true}
		}
	case "fail_first_n":
		if p.CurrentAttempt < p.FailFirstN {
			return Result{Success: false, Error: fmt.Sprintf("chaos fail attempt %d/%d", p.CurrentAttempt+1, p.FailFirstN), Retryable: true}
		}
		return Result{Success: true, Output: map[string]any{"mode": "fail_first_n", "succeeded_at_attempt": p.CurrentAttempt}}
	default:
		return Result{Success: true, Output: map[string]any{"mode": "succeed"}}
	}
}
