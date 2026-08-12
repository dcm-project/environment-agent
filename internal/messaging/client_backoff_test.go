package messaging

import (
	"log/slog"
	"testing"
	"time"
)

// TestReconnectDelay_ExponentialWithJitter validates REQ-MSG-100: NATS
// reconnect delay uses the same exponential-backoff-with-full-jitter formula
// as REQ-DCM-050 (min(initial×2^attempt, max) with full jitter). (UT-MSG-050)
func TestReconnectDelay_ExponentialWithJitter(t *testing.T) {
	c := NewClient(ClientConfig{
		ReconnectInitialBackoff: 1 * time.Second,
		ReconnectMaxBackoff:     30 * time.Second,
	}, slog.Default())

	// Deterministic jitter: randFn always returns 1.0 -> delay == calculated ceiling.
	c.randFn = func() float64 { return 1.0 }

	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{5, 30 * time.Second}, // capped: 1s*2^5=32s > 30s max
		{10, 30 * time.Second},
	}
	for _, tc := range cases {
		if got := c.reconnectDelay(tc.attempts); got != tc.want {
			t.Errorf("reconnectDelay(%d) = %v, want %v", tc.attempts, got, tc.want)
		}
	}
}

// TestReconnectDelay_JitterBounded validates the jitter component stays
// within [0, calculated] and is not a constant, so successive attempts
// don't stampede the broker in lockstep (UT-MSG-051, UT-MSG-052).
func TestReconnectDelay_JitterBounded(t *testing.T) {
	c := NewClient(ClientConfig{
		ReconnectInitialBackoff: 1 * time.Second,
		ReconnectMaxBackoff:     30 * time.Second,
	}, slog.Default())

	seq := []float64{0.0, 0.5, 0.9}
	i := 0
	c.randFn = func() float64 {
		v := seq[i%len(seq)]
		i++
		return v
	}

	if got := c.reconnectDelay(2); got != 0 { // UT-MSG-051
		t.Errorf("reconnectDelay with randFn=0.0 = %v, want 0", got)
	}
	if got := c.reconnectDelay(2); got != 2*time.Second { // UT-MSG-052
		t.Errorf("reconnectDelay with randFn=0.5 at attempt=2 (calculated=4s) = %v, want 2s", got)
	}
}

// TestReconnectDelay_DefaultsWhenUnset validates that a zero-value
// ClientConfig (as used by many existing test call sites) still produces a
// sane, non-zero, capped backoff via defaultReconnectInitialBackoff/MaxBackoff.
// (UT-MSG-053)
func TestReconnectDelay_DefaultsWhenUnset(t *testing.T) {
	c := NewClient(ClientConfig{}, slog.Default())
	c.randFn = func() float64 { return 1.0 }

	if got := c.reconnectDelay(0); got != defaultReconnectInitialBackoff {
		t.Errorf("reconnectDelay(0) with unset config = %v, want %v", got, defaultReconnectInitialBackoff)
	}
	if got := c.reconnectDelay(20); got != defaultReconnectMaxBackoff {
		t.Errorf("reconnectDelay(20) with unset config = %v, want %v (capped)", got, defaultReconnectMaxBackoff)
	}
}
