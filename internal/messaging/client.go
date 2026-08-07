// Package messaging provides NATS/JetStream messaging client and topic management.
package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/dcm"
	"github.com/dcm-project/environment-agent/internal/health"
)

var (
	_ health.MessagingStatus  = (*Client)(nil)
	_ dcm.ConsumerLagProvider = (*Client)(nil)
)

const (
	drainTimeout   = 5 * time.Second
	drainBatchWait = 200 * time.Millisecond
	drainBatchSize = 100
)

// MessageHandler is the callback type for processing consumed messages.
type MessageHandler func(ctx context.Context, msg []byte) error

// ClientConfig holds the NATS client configuration.
// TopicName, if set, must be a pre-validated topic name (via ValidateTopicName).
// If empty, the agent name is used as the base for topic derivation.
type ClientConfig struct {
	URL            string
	TopicName      string
	AgentName      string
	AckWait        time.Duration
	CancelAckWait  time.Duration
	MaxDeliver     int
	HandlerTimeout time.Duration
	NakDelay       time.Duration
}

// Client manages NATS/JetStream connectivity and topic consumption.
type Client struct {
	cfg    ClientConfig
	logger *slog.Logger
	topics TopicNames

	conn      *nats.Conn
	js        jetstream.JetStream
	mainCons  jetstream.Consumer
	consumers []jetstream.ConsumeContext
	mu        sync.Mutex

	connected atomic.Bool
	stopped   atomic.Bool

	// ponytail: set-before-Start contract — not safe for concurrent mutation
	mainHandler   MessageHandler
	cancelHandler MessageHandler

	setupMu   sync.Mutex
	setupDone bool
	stopOnce  sync.Once
}

// NewClient creates a new messaging client. Does NOT connect to NATS.
func NewClient(cfg ClientConfig, logger *slog.Logger) *Client {
	return &Client{
		cfg:    cfg,
		logger: logger,
		topics: DeriveTopicNames(cfg.AgentName, cfg.TopicName),
	}
}

// Start connects to NATS, creates streams/consumers, and begins consuming.
// Non-blocking: returns nil immediately even if NATS is unreachable.
func (c *Client) Start(_ context.Context) error {
	if c.mainHandler == nil || c.cancelHandler == nil {
		return fmt.Errorf("handlers must be set before Start")
	}
	setupCtx := context.Background()

	conn, err := nats.Connect(c.cfg.URL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.ConnectHandler(func(nc *nats.Conn) {
			c.connected.Store(true)
			c.logger.Info("NATS connected", "url", nc.ConnectedUrl())
			go c.doSetup(setupCtx, nc)
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			c.connected.Store(false)
			c.logger.Warn("NATS disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.connected.Store(true)
			c.logger.Info("NATS reconnected", "url", nc.ConnectedUrl())
			go c.doSetup(setupCtx, nc)
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	if conn.IsConnected() {
		c.connected.Store(true)
		c.doSetup(setupCtx, conn)
	}

	return nil
}

func (c *Client) doSetup(ctx context.Context, conn *nats.Conn) {
	if c.stopped.Load() {
		return
	}
	c.setupMu.Lock()
	defer c.setupMu.Unlock()
	if c.setupDone {
		return
	}
	if c.setupStreamsAndConsume(ctx, conn) {
		c.setupDone = true
	}
}

func (c *Client) setupStreamsAndConsume(ctx context.Context, conn *nats.Conn) bool {
	js, err := jetstream.New(conn)
	if err != nil {
		c.logger.Error("failed to create JetStream context", "error", err)
		return false
	}

	mainS, retryS, cancelS, err := c.initStreams(ctx, js)
	if err != nil {
		c.logger.Error("failed to initialize streams", "error", err)
		return false
	}

	mainCons, cancelCons, err := c.initConsumers(ctx, mainS, retryS, cancelS)
	if err != nil {
		c.logger.Error("failed to initialize consumers", "error", err)
		return false
	}

	c.drainCancelTopic(ctx, cancelCons)

	cancelCC, err := cancelCons.Consume(func(msg jetstream.Msg) {
		c.handleCancelMessage(msg)
	})
	if err != nil {
		c.logger.Error("failed to start cancel consumer", "error", err)
		return false
	}

	mainCC, err := mainCons.Consume(func(msg jetstream.Msg) {
		c.handleMainMessage(msg)
	})
	if err != nil {
		cancelCC.Stop()
		c.logger.Error("failed to start main consumer", "error", err)
		return false
	}

	c.mu.Lock()
	if c.stopped.Load() {
		c.mu.Unlock()
		cancelCC.Stop()
		mainCC.Stop()
		return false
	}
	c.conn = conn
	c.js = js
	c.consumers = append(c.consumers, cancelCC, mainCC)
	c.mu.Unlock()
	return true
}

func (c *Client) initStreams(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, jetstream.Stream, jetstream.Stream, error) {
	mainS, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: c.topics.Main, Subjects: []string{c.topics.Main},
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create main stream: %w", err)
	}
	retryS, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: c.topics.Main + "-retry", Subjects: []string{c.topics.Retry},
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create retry stream: %w", err)
	}
	cancelS, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: c.topics.Main + "-cancel", Subjects: []string{c.topics.Cancel},
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create cancel stream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "dcm-responses", Subjects: []string{cloudevent.SubjectResponses},
	}); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create responses stream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "dcm-health", Subjects: []string{cloudevent.SubjectHealth},
	}); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create health stream: %w", err)
	}
	return mainS, retryS, cancelS, nil
}

func (c *Client) initConsumers(ctx context.Context, mainS, retryS, cancelS jetstream.Stream) (jetstream.Consumer, jetstream.Consumer, error) {
	mainCons, err := mainS.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: c.topics.Main + "-consumer", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: c.cfg.AckWait, MaxDeliver: c.cfg.MaxDeliver,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create main consumer: %w", err)
	}
	// Retry consumer: used by retry.Processor via JetStream Consumer binding
	if _, err := retryS.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: c.topics.Main + "-retry-consumer", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: c.cfg.AckWait, MaxDeliver: c.cfg.MaxDeliver,
	}); err != nil {
		return nil, nil, fmt.Errorf("failed to create retry consumer: %w", err)
	}
	cancelCons, err := cancelS.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: c.topics.Main + "-cancel-consumer", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: c.cfg.CancelAckWait,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create cancel consumer: %w", err)
	}

	c.mu.Lock()
	c.mainCons = mainCons
	c.mu.Unlock()

	return mainCons, cancelCons, nil
}

func (c *Client) drainCancelTopic(_ context.Context, cancelCons jetstream.Consumer) {
	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
	defer drainCancel()

	for drainCtx.Err() == nil {
		batch, err := cancelCons.Fetch(drainBatchSize, jetstream.FetchMaxWait(drainBatchWait))
		if err != nil {
			return
		}
		count := 0
		for msg := range batch.Messages() {
			c.handleCancelMessage(msg)
			count++
		}
		if count == 0 {
			return
		}
	}
}

// Stop gracefully shuts down the client.
func (c *Client) Stop() {
	c.stopOnce.Do(func() {
		c.stopped.Store(true)

		c.mu.Lock()
		consumers := c.consumers
		c.consumers = nil
		conn := c.conn
		c.mu.Unlock()

		for _, cc := range consumers {
			cc.Stop()
		}
		if conn != nil {
			conn.Close()
		}
		c.connected.Store(false)
	})
}

// IsConnected returns the cached connectivity state (no I/O).
func (c *Client) IsConnected() bool { return c.connected.Load() }

// ConsumerLag returns the current consumer lag from JetStream.
func (c *Client) ConsumerLag() int64 {
	c.mu.Lock()
	cons := c.mainCons
	c.mu.Unlock()

	if cons == nil {
		return 0
	}
	return int64(cons.CachedInfo().NumPending)
}

// TopicName returns the main topic name advertised to DCM.
func (c *Client) TopicName() string { return c.topics.Main }

// JS returns the JetStream context for direct stream access by the retry processor.
func (c *Client) JS() jetstream.JetStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.js
}

// Publish publishes raw bytes to the given NATS subject via JetStream.
func (c *Client) Publish(ctx context.Context, subject string, data []byte) error {
	c.mu.Lock()
	js := c.js
	c.mu.Unlock()

	if js == nil {
		return errors.New("jetstream not initialized")
	}
	_, err := js.Publish(ctx, subject, data)
	return err
}

// SetMainHandler sets the handler for messages on the main topic.
// Must be called before Start.
func (c *Client) SetMainHandler(h MessageHandler) { c.mainHandler = h }

// SetCancelHandler sets the handler for messages on the cancel topic.
// Must be called before Start.
func (c *Client) SetCancelHandler(h MessageHandler) { c.cancelHandler = h }
