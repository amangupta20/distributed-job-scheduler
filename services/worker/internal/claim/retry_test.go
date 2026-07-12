package claim_test

import (
	"testing"
	"time"

	"github.com/pulsequeue/worker/internal/claim"
)

func TestNextRetryAtStrategies(t *testing.T) {
	now := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		policy   claim.RetryPolicy
		attempt  int
		expected time.Time
	}{
		{"fixed", claim.RetryPolicy{Strategy: "fixed", BaseDelay: 10 * time.Second, MaxDelay: time.Minute}, 3, now.Add(10 * time.Second)},
		{"linear", claim.RetryPolicy{Strategy: "linear", BaseDelay: 10 * time.Second, MaxDelay: time.Minute}, 3, now.Add(30 * time.Second)},
		{"exponential", claim.RetryPolicy{Strategy: "exponential", BaseDelay: 10 * time.Second, MaxDelay: time.Minute}, 3, now.Add(40 * time.Second)},
		{"exponential capped", claim.RetryPolicy{Strategy: "exponential", BaseDelay: 10 * time.Second, MaxDelay: time.Minute}, 10, now.Add(time.Minute)},
		{"overflow capped", claim.RetryPolicy{Strategy: "exponential", BaseDelay: time.Hour, MaxDelay: 24 * time.Hour}, 1000, now.Add(24 * time.Hour)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := claim.NextRetryAt(tt.policy, tt.attempt, now); !got.Equal(tt.expected) {
				t.Fatalf("NextRetryAt() = %v, want %v", got, tt.expected)
			}
		})
	}
}
