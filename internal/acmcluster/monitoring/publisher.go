package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/nats-io/nats.go"
)

// StatusEvent carries a status change for a single cluster instance.
type StatusEvent struct {
	InstanceID string
	Status     v1alpha1.ClusterStatus
	Message    string
}

// StatusPublisher publishes status events. Decouples event detection from delivery.
type StatusPublisher interface {
	Publish(ctx context.Context, event StatusEvent) error
	Close() error
}

var _ StatusPublisher = (*NATSPublisher)(nil)

// NATSPublisher implements StatusPublisher using a NATS connection.
type NATSPublisher struct {
	conn         *nats.Conn
	providerName string
	subject      string
}

// NewNATSPublisher creates a NATSPublisher connected to the given NATS URL.
// The connection uses unlimited reconnection attempts and retries on failed
// initial connect, so the SP can start even when NATS is unreachable.
func NewNATSPublisher(natsURL, providerName string, logger *slog.Logger) (*NATSPublisher, error) {
	conn, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Error("NATS disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected", "url", nc.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS at %s: %w", natsURL, err)
	}
	return &NATSPublisher{
		conn:         conn,
		providerName: providerName,
		subject:      "dcm.cluster",
	}, nil
}

// Publish sends a status event as a CloudEvent to the configured NATS subject.
func (p *NATSPublisher) Publish(_ context.Context, event StatusEvent) error {
	ce, err := NewStatusCloudEvent(p.providerName, event.InstanceID, event.Status, event.Message)
	if err != nil {
		return fmt.Errorf("constructing cloud event: %w", err)
	}
	data, err := json.Marshal(ce)
	if err != nil {
		return fmt.Errorf("marshaling cloud event: %w", err)
	}
	return p.conn.Publish(p.subject, data)
}

// Close closes the underlying NATS connection.
func (p *NATSPublisher) Close() error {
	p.conn.Close()
	return nil
}
