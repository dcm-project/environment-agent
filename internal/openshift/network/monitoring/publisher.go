package monitoring

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/nats-io/nats.go"
)

// StatusEvent represents a network status change to be published.
type StatusEvent struct {
	InstanceID string
	Status     v1alpha1.NetworkStatus
	Message    string
}

// StatusPublisher abstracts event publishing so that the transport layer
// can be swapped (e.g., NATS, mock, in-memory).
type StatusPublisher interface {
	Publish(ctx context.Context, event StatusEvent) error
	Close() error
}

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
	if logger == nil {
		panic("NATS publisher: logger must not be nil")
	}
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
		subject:      "dcm.network",
	}, nil
}

// Publish sends a status event as a CloudEvent to the configured NATS subject.
func (p *NATSPublisher) Publish(ctx context.Context, event StatusEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := NewStatusCloudEvent(p.subject, p.providerName, event.InstanceID, event.Status, event.Message)
	if err != nil {
		return fmt.Errorf("constructing cloud event: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.conn.Publish(p.subject, data); err != nil {
		return fmt.Errorf("publishing to NATS subject %q: %w", p.subject, err)
	}
	return nil
}

// Close flushes pending messages and closes the underlying NATS connection.
func (p *NATSPublisher) Close() error {
	if err := p.conn.Drain(); err != nil {
		p.conn.Close() // Force close if drain fails
		return fmt.Errorf("draining NATS connection: %w", err)
	}
	return nil
}
