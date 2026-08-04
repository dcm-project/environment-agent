package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dcm-project/environment-agent/internal/cloudevent"
)

const nakDelay = 500 * time.Millisecond

type ceResourcePayload struct {
	ResourceID string `json:"resourceId"`
}

func (c *Client) handleCancelMessage(msg jetstream.Msg) {
	// TODO(topic8): MISSING-1 — acking malformed CE violates REQ-MSG-115; should nak instead
	var event cloudevents.Event
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		c.logger.Warn("failed to parse cancel CE", "error", err)
		if c.cancelHandler != nil {
			_ = c.cancelHandler(context.Background(), msg.Data())
		}
		return
	}

	var payload ceResourcePayload
	_ = json.Unmarshal(event.Data(), &payload)
	if payload.ResourceID != "" {
		c.denyList.Store(payload.ResourceID, struct{}{})
	}

	if c.cancelHandler != nil {
		_ = c.cancelHandler(context.Background(), msg.Data())
	}
}

func (c *Client) handleMainMessage(msg jetstream.Msg) {
	// TODO(topic8): MISSING-1 — acking malformed CE violates REQ-MSG-115; should nak instead
	var event cloudevents.Event
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		c.logger.Warn("failed to parse main CE", "error", err)
		if c.mainHandler != nil {
			if herr := c.mainHandler(context.Background(), msg.Data()); herr != nil {
				_ = msg.NakWithDelay(nakDelay)
				return
			}
		}
		_ = msg.Ack()
		return
	}

	var payload ceResourcePayload
	_ = json.Unmarshal(event.Data(), &payload)
	resourceID := payload.ResourceID

	if _, loaded := c.denyList.LoadAndDelete(resourceID); loaded {
		_ = msg.Ack()
		return
	}

	if c.mainHandler != nil {
		if err := c.mainHandler(context.Background(), msg.Data()); err != nil {
			_ = msg.NakWithDelay(nakDelay)
			return
		}
	}

	if err := c.publishResponseCE(event.Type(), resourceID); err != nil {
		c.logger.Error("failed to publish response CE", "error", err)
		_ = msg.NakWithDelay(nakDelay)
		return
	}

	_ = msg.Ack()
}

func (c *Client) publishResponseCE(incomingType, resourceID string) error {
	var responseType string
	switch {
	case strings.Contains(incomingType, "create"):
		responseType = TypeCreationAcked
	case strings.Contains(incomingType, "delete"):
		responseType = TypeDeletionAcked
	default:
		responseType = TypeAcked
	}

	var status string
	switch responseType {
	case TypeCreationAcked:
		status = "PROVISIONING"
	case TypeDeletionAcked:
		status = "DELETING"
	default:
		status = "ACKNOWLEDGED"
	}

	event, err := cloudevent.NewCloudEvent(c.cfg.AgentName, responseType)
	if err != nil {
		return fmt.Errorf("failed to create response CE: %w", err)
	}
	if err := event.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
		"agentName":  c.cfg.AgentName,
		"topicName":  c.topics.Main,
		"resourceId": resourceID,
		"status":     status,
	}); err != nil {
		return fmt.Errorf("failed to set response CE data: %w", err)
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal response CE: %w", err)
	}

	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	return conn.Publish(SubjectResponses, data)
}
