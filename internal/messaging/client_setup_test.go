package messaging

import (
	"context"
	"log/slog"
	"testing"
)

// TestAttemptSetup_SetupDoneShortCircuits verifies attemptSetup's setupDone
// guard returns before setupStreamsAndConsume runs again, so a NATS
// reconnect can't re-append to c.consumers. A nil *nats.Conn is safe here
// only because that guard fires first.
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
