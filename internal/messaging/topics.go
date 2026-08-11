package messaging

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var validTopicNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// RequestStreamName is the JetStream stream owned by the control plane,
// binding the `dcm.agent.>` wildcard subject (F2). The agent must not create
// or own this stream — only durable consumers on it, filtered to Main/Cancel
// below. See Client.createRequestConsumer for the startup-race tolerance.
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
// Also rejects a base that already starts with the reserved
// requestSubjectPrefix: otherwise Main would be double-prefixed, and the
// derived Retry subject would fall inside the CP-owned dcm.agent.> wildcard
// (F2), colliding with the CP's own stream.
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
	// The exact base "dcm.agent" (no trailing dot) needs an explicit check:
	// it wouldn't match HasPrefix below but would still derive a colliding
	// Retry subject.
	reservedBase := strings.TrimSuffix(requestSubjectPrefix, ".")
	if name == reservedBase || strings.HasPrefix(name, requestSubjectPrefix) {
		return fmt.Errorf("topic name must not start with the reserved %q prefix (it is added automatically)", requestSubjectPrefix)
	}
	return nil
}
