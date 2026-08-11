package messaging

import (
	"context"
	"log/slog"
	"testing"
)

// TestAttemptSetup_SetupDoneShortCircuits is a regression test for a
// finding that the reconnect consumer-tracking slice grows unbounded.
// Investigation showed this was a false positive: attemptSetup's setupDone
// guard returns before setupStreamsAndConsume ever runs again, so a NATS
// reconnect (which re-fires ConnectHandler/ReconnectHandler -> doSetup)
// cannot re-append to c.consumers. This test proves the short-circuit fires
// BEFORE any consumer/conn access — passing a nil *nats.Conn is safe here
// only because of that guard, which is exactly the property being verified.
func TestAttemptSetup_SetupDoneShortCircuits(t *testing.T) {
	c := NewClient(ClientConfig{AgentName: "test-agent", TopicName: "t"}, slog.Default())
	c.setupDone = true // simulate: initial setup already completed once

	done := c.attemptSetup(context.Background(), nil)
	if !done {
		t.Fatal("attemptSetup must report done immediately when setupDone is already true")
	}
	if len(c.consumers) != 0 {
		t.Fatalf("consumers slice must not grow when setup is short-circuited, got %d entries", len(c.consumers))
	}
}

// TestBeginConsuming_ConsumingFlagShortCircuits complements the above:
// even if setupStreamsAndConsume were ever re-entered (e.g. a future
// DeferConsume/StartConsuming race), beginConsuming has its own independent
// consuming-flag guard preventing a second append to c.consumers.
func TestBeginConsuming_ConsumingFlagShortCircuits(t *testing.T) {
	c := NewClient(ClientConfig{}, slog.Default())
	c.consuming = true // simulate: live consumption already started once

	if err := c.beginConsuming(); err != nil {
		t.Fatalf("beginConsuming must be a no-op once consuming is true, got err: %v", err)
	}
	if len(c.consumers) != 0 {
		t.Fatalf("consumers slice must not grow on a repeated beginConsuming call, got %d entries", len(c.consumers))
	}
}
