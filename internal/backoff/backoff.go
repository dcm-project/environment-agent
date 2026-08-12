// Package backoff provides utilities for calculating exponential backoff durations with jitter.
package backoff

import "time"

// CalculateBackoff returns the deterministic component: min(initial × 2^attempt, max),
// computed via integer doubling (no float/overflow-dependent behavior regardless of
// attempt's magnitude). The d > max/2 check caps before doubling so d*2 never
// overflows int64, even for pathologically large attempt or max values.
func CalculateBackoff(initial, max time.Duration, attempt int) time.Duration {
	if initial <= 0 {
		return max
	}
	d := initial
	for i := 0; i < attempt && d < max; i++ {
		if d > max/2 {
			return max
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}

// ApplyJitter applies full jitter: uniform random in [0, calculated].
// randFn allows deterministic testing; production callers pass math/rand.Float64.
func ApplyJitter(calculated time.Duration, randFn func() float64) time.Duration {
	return time.Duration(float64(calculated) * randFn())
}
