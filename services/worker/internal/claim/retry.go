package claim

import (
	"math"
	"time"
)

// RetryPolicy is the immutable retry configuration captured when a job is claimed.
type RetryPolicy struct {
	Strategy  string
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// NextRetryAt returns the next attempt time for a one-based failed attempt.
func NextRetryAt(policy RetryPolicy, attempt int, now time.Time) time.Time {
	if attempt < 1 {
		attempt = 1
	}
	base := policy.BaseDelay
	if base < 0 {
		base = 0
	}
	maximum := policy.MaxDelay
	if maximum <= 0 {
		maximum = base
	}

	multiplier := uint64(1)
	switch policy.Strategy {
	case "linear":
		multiplier = uint64(attempt)
	case "exponential":
		shift := attempt - 1
		if shift >= 63 {
			return now.Add(maximum)
		}
		multiplier = uint64(1) << shift
	}

	if base > 0 && multiplier > uint64(math.MaxInt64)/uint64(base) {
		return now.Add(maximum)
	}
	delay := time.Duration(uint64(base) * multiplier)
	if delay > maximum {
		delay = maximum
	}
	return now.Add(delay)
}
