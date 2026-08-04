// Package backoff provides utilities for calculating exponential backoff durations with jitter.
package backoff

import (
	"math"
	"time"
)

// CalculateBackoff returns the deterministic component: min(initial × 2^attempt, max).
func CalculateBackoff(initial, max time.Duration, attempt int) time.Duration {
	shift := math.Pow(2, float64(attempt))
	d := time.Duration(float64(initial) * shift)
	if d <= 0 || d > max {
		return max
	}
	return d
}

// ApplyJitter applies full jitter: uniform random in [0, calculated].
// randFn allows deterministic testing; production callers pass math/rand.Float64.
func ApplyJitter(calculated time.Duration, randFn func() float64) time.Duration {
	return time.Duration(float64(calculated) * randFn())
}
