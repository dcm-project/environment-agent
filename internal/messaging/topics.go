package messaging

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var validTopicNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// RequestStreamName is the JetStream stream owned by the control-plane,
// binding the `dcm.agent.>` wildcard subject. Per the CP/agent alignment
// review (F2), the agent MUST NOT create or own this stream — it only
// creates durable consumers on it, filtered to its own subjects (Main and
// Cancel below). The control plane may not have created this stream yet
// when the agent starts; callers must tolerate and retry (see
// Client.createRequestConsumer).
const RequestStreamName = "dcm-agent-requests"

// requestSubjectPrefix is prepended to the agent's base topic name to form
// the control-plane-facing request subject. Registration requires
// `topic_name` to match `^dcm\.agent\..+` (control-plane openapi.yaml).
const requestSubjectPrefix = "dcm.agent."

// TopicNames holds the subjects derived from the agent's base topic name,
// plus the deterministic consumer/stream names used to consume them.
//
//   - Main and Cancel are subjects under the control-plane-owned
//     `dcm.agent.>` wildcard (bound by RequestStreamName). The agent creates
//     durable consumers on RequestStreamName filtered to these subjects —
//     it does NOT create streams for them.
//   - Retry is agent-internal (never published to by the control plane);
//     the agent creates and owns its own stream for it.
type TopicNames struct {
	// Base is the unprefixed topic name (AGENT_TOPIC_NAME override, or
	// AGENT_NAME). Used to derive deterministic consumer/stream names.
	Base string
	// Main is the control-plane-facing subject for create/delete requests,
	// advertised to DCM as topic_name on registration.
	Main string
	// Cancel is the control-plane-facing subject for cancel requests
	// (REQ-MSG-030/050): Main + ".cancel". Still part of the dcm.agent.>
	// wildcard, so it is CP-owned like Main, not agent-owned.
	Cancel string
	// Retry is the agent-internal subject used to hold requests while an SP
	// is unhealthy.
	Retry string
}

// DeriveTopicNames derives the main/cancel/retry subjects from the agent name
// and an optional override. If topicNameOverride is non-empty, it takes
// precedence over agentName as the base.
func DeriveTopicNames(agentName, topicNameOverride string) TopicNames {
	base := agentName
	if topicNameOverride != "" {
		base = topicNameOverride
	}
	main := requestSubjectPrefix + base
	return TopicNames{
		Base:   base,
		Main:   main,
		Cancel: main + ".cancel",
		Retry:  base + ".retry",
	}
}

// MainConsumer is the deterministic durable consumer name for Main, bound to
// RequestStreamName.
func (t TopicNames) MainConsumer() string { return t.Base + "-consumer" }

// CancelConsumer is the deterministic durable consumer name for Cancel,
// bound to RequestStreamName.
func (t TopicNames) CancelConsumer() string { return t.Base + "-cancel-consumer" }

// RetryStream is the agent-owned JetStream stream name backing Retry.
func (t TopicNames) RetryStream() string { return t.Base + "-retry" }

// RetryConsumer is the deterministic durable consumer name for RetryStream.
func (t TopicNames) RetryConsumer() string { return t.Base + "-retry-consumer" }

// ValidateTopicName validates that the given name conforms to NATS subject
// token rules. Applied to the unprefixed base name (TopicNames.Base) — the
// "dcm.agent." prefix applied to Main is a trusted static constant, not
// user input.
//
// It also rejects a base that already starts with the reserved
// requestSubjectPrefix. Without this, AGENT_TOPIC_NAME=dcm.agent.foo would
// derive Main=dcm.agent.dcm.agent.foo (double-prefixed, never matched by CP)
// and, worse, Retry=dcm.agent.foo.retry — a subject that falls *inside* the
// CP-owned dcm.agent.> wildcard, so the agent's own retry stream creation
// would silently compete with the CP's stream for that subject (F2).
func ValidateTopicName(name string) error {
	if name == "" {
		return errors.New("topic name must not be empty")
	}
	if len(name) > 255 {
		return errors.New("topic name exceeds 255 characters")
	}
	if !validTopicNameRe.MatchString(name) {
		return errors.New("topic name contains invalid characters (allowed: alphanumeric, hyphens, dots, underscores)")
	}
	// The exact base "dcm.agent" (no trailing dot) doesn't match HasPrefix
	// against "dcm.agent." below, but would still derive Retry="dcm.agent.retry"
	// — a subject inside the CP-owned dcm.agent.> wildcard — so it must be
	// rejected explicitly, not just names with the dotted prefix.
	reservedBase := strings.TrimSuffix(requestSubjectPrefix, ".")
	if name == reservedBase || strings.HasPrefix(name, requestSubjectPrefix) {
		return fmt.Errorf("topic name must not start with the reserved %q prefix (it is added automatically)", requestSubjectPrefix)
	}
	return nil
}
