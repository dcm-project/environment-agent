package messaging

import (
	"errors"
	"regexp"
)

var validTopicNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// TopicNames holds the derived main, retry, and cancel topic names.
type TopicNames struct {
	Main   string
	Retry  string
	Cancel string
}

// DeriveTopicNames derives main/retry/cancel topic names from the agent name
// and an optional override. If topicNameOverride is non-empty, it takes precedence.
func DeriveTopicNames(agentName, topicNameOverride string) TopicNames {
	base := agentName
	if topicNameOverride != "" {
		base = topicNameOverride
	}
	return TopicNames{
		Main:   base,
		Retry:  base + ".retry",
		Cancel: base + ".cancel",
	}
}

// ValidateTopicName validates that the given name conforms to NATS subject token rules.
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
	return nil
}
