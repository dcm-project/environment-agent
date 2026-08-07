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

	// requestStreamRetryInterval/Timeout bound the retry loop for creating
	// durable consumers on the control-plane-owned RequestStreamName. The CP
	// may not have created that stream yet when this agent starts (startup
	// order isn't guaranteed) — F2 of the CP/agent alignment review.
	requestStreamRetryInterval = 2 * time.Second
	requestStreamRetryTimeout  = 30 * time.Second
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
	retrying  bool // guards against spawning more than one background retry goroutine
	stopOnce  sync.Once
	stopCh    chan struct{}
}

// NewClient creates a new messaging client. Does NOT connect to NATS.
func NewClient(cfg ClientConfig, logger *slog.Logger) *Client {
	return &Client{
		cfg:    cfg,
		logger: logger,
		topics: DeriveTopicNames(cfg.AgentName, cfg.TopicName),
		stopCh: make(chan struct{}),
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

// doSetup creates streams/consumers and starts consuming. It is invoked from
// the initial synchronous connect path in Start (bounded, tests rely on this
// completing quickly when the CP stream already exists), and from every
// ConnectHandler/ReconnectHandler (already backgrounded by `go`).
//
// A single attempt can itself block for up to requestStreamRetryTimeout if
// the control-plane hasn't created RequestStreamName yet (startup-order
// race, F2) — createRequestConsumer retries internally on that bound. If
// that one attempt still fails, doSetup does NOT retry inline (Start's
// synchronous caller must not block indefinitely — see its "non-blocking"
// contract). Instead it hands off to retrySetupInBackground: once the
// initial NATS connection is up, no further ConnectHandler/ReconnectHandler
// will fire to give doSetup another chance, so without a background retry
// the agent would silently sit forever with setupDone still false and no
// consumer ever started.
func (c *Client) doSetup(ctx context.Context, conn *nats.Conn) {
	if c.attemptSetup(ctx, conn) {
		return
	}
	c.setupMu.Lock()
	alreadyRetrying := c.retrying
	c.retrying = true
	c.setupMu.Unlock()
	if !alreadyRetrying {
		go c.retrySetupInBackground(ctx, conn)
	}
}

// attemptSetup runs setupStreamsAndConsume at most once. Returns true if
// setup is done (either just now, already done, or the client is stopped —
// all three mean "nothing left for the caller to do").
func (c *Client) attemptSetup(ctx context.Context, conn *nats.Conn) bool {
	c.setupMu.Lock()
	defer c.setupMu.Unlock()
	if c.setupDone || c.stopped.Load() {
		return true
	}
	if c.setupStreamsAndConsume(ctx, conn) {
		c.setupDone = true
		return true
	}
	return false
}

// retrySetupInBackground keeps retrying attemptSetup on a bounded interval,
// indefinitely, until it succeeds or the client is stopped. At most one
// instance runs at a time (guarded by the `retrying` flag in doSetup).
func (c *Client) retrySetupInBackground(ctx context.Context, conn *nats.Conn) {
	defer func() {
		c.setupMu.Lock()
		c.retrying = false
		c.setupMu.Unlock()
	}()
	c.logger.Error("messaging setup failed, will keep retrying in background",
		"retry_after", requestStreamRetryTimeout)
	for {
		select {
		case <-c.stopCh:
			return
		case <-time.After(requestStreamRetryTimeout):
		}
		if c.attemptSetup(ctx, conn) {
			return
		}
	}
}

func (c *Client) setupStreamsAndConsume(ctx context.Context, conn *nats.Conn) bool {
	js, err := jetstream.New(conn)
	if err != nil {
		c.logger.Error("failed to create JetStream context", "error", err)
		return false
	}

	retryS, err := c.initInternalStreams(ctx, js)
	if err != nil {
		c.logger.Error("failed to initialize internal streams", "error", err)
		return false
	}

	mainCons, cancelCons, err := c.initConsumers(ctx, js, retryS)
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

// initInternalStreams creates the agent-owned streams only. The request
// subjects (dcm.agent.>) and response subject (dcm.agents.responses) are
// owned by the control-plane (F2) — this agent must not create streams for
// them; see initConsumers (durable consumers on the CP's stream) and
// Publish (direct publish to the response subject, no stream needed).
func (c *Client) initInternalStreams(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	retryS, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: c.topics.RetryStream(), Subjects: []string{c.topics.Retry},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create retry stream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "dcm-health", Subjects: []string{cloudevent.SubjectHealth},
	}); err != nil {
		return nil, fmt.Errorf("failed to create health stream: %w", err)
	}
	return retryS, nil
}

func (c *Client) initConsumers(ctx context.Context, js jetstream.JetStream, retryS jetstream.Stream) (jetstream.Consumer, jetstream.Consumer, error) {
	mainCons, err := c.createRequestConsumer(ctx, js, c.topics.MainConsumer(), c.topics.Main, c.cfg.AckWait, c.cfg.MaxDeliver)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create main consumer: %w", err)
	}
	// Cancel consumer intentionally has no MaxDeliver limit — cancels must
	// never be dropped by delivery-count exhaustion, only by Term on parse failure.
	cancelCons, err := c.createRequestConsumer(ctx, js, c.topics.CancelConsumer(), c.topics.Cancel, c.cfg.CancelAckWait, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create cancel consumer: %w", err)
	}
	// Retry consumer: agent-owned stream, used by retry.Processor via JetStream Consumer binding
	if _, err := retryS.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: c.topics.RetryConsumer(), AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: c.cfg.AckWait, MaxDeliver: c.cfg.MaxDeliver,
	}); err != nil {
		return nil, nil, fmt.Errorf("failed to create retry consumer: %w", err)
	}

	c.mu.Lock()
	c.mainCons = mainCons
	c.mu.Unlock()

	return mainCons, cancelCons, nil
}

// createRequestConsumer creates a durable consumer on the control-plane-owned
// RequestStreamName, filtered to the given subject. Retries with a bounded
// interval within this call because the CP may not have created that stream
// yet at agent startup (F2). If it's still missing after
// requestStreamRetryTimeout, this returns an error — the caller (doSetup)
// retries the whole setup indefinitely, so this bound just controls how
// often the (possibly noisy) "not found" case is re-logged, not whether the
// agent ever gives up.
func (c *Client) createRequestConsumer(ctx context.Context, js jetstream.JetStream, durable, filterSubject string, ackWait time.Duration, maxDeliver int) (jetstream.Consumer, error) {
	cfg := jetstream.ConsumerConfig{
		Durable: durable, FilterSubject: filterSubject,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait, MaxDeliver: maxDeliver,
	}
	deadline := time.Now().Add(requestStreamRetryTimeout)
	for {
		cons, err := js.CreateOrUpdateConsumer(ctx, RequestStreamName, cfg)
		if err == nil {
			return cons, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for stream %q: %w", RequestStreamName, err)
		}
		// ErrStreamNotFound is the expected transient case during startup
		// races (CP hasn't created RequestStreamName yet) — log at Warn.
		// Anything else (bad FilterSubject, permissions, ...) is a genuine
		// misconfiguration that retrying won't fix; log at Error so it's
		// distinguishable in operational alerting, even though we still
		// retry (crashing the agent for what might be a transient auth
		// blip would be worse than a noisy retry loop).
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			c.logger.Warn("request stream not created yet, retrying consumer creation",
				"stream", RequestStreamName, "durable", durable, "error", err)
		} else {
			c.logger.Error("unexpected error creating durable consumer, retrying",
				"stream", RequestStreamName, "durable", durable, "error", err)
		}
		select {
		case <-c.stopCh:
			return nil, errors.New("client stopped")
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(requestStreamRetryInterval):
		}
	}
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
		close(c.stopCh)

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
// For dcm.agents.responses, this relies on the control-plane's stream
// binding that subject (F2) — the agent does not create its own stream for
// it, JetStream auto-routes the publish to whichever stream binds the subject.
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

// PublishWithMsgID publishes with a Nats-Msg-Id header for JetStream
// server-side dedup, using the CE's own id (F34). Used for response CEs so
// that a *future* publish-retry mechanism (re-publishing the same
// already-built bytes+id after a failed attempt) can't cause duplicate
// delivery to the control-plane's response consumer. There is no such retry
// today, so this is currently inert — see cloudevent.PublishCE's doc comment
// for why re-invoking PublishCE itself does not get this benefit.
func (c *Client) PublishWithMsgID(ctx context.Context, subject, msgID string, data []byte) error {
	c.mu.Lock()
	js := c.js
	c.mu.Unlock()

	if js == nil {
		return errors.New("jetstream not initialized")
	}
	_, err := js.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
	return err
}

// SetMainHandler sets the handler for messages on the main topic.
// Must be called before Start.
func (c *Client) SetMainHandler(h MessageHandler) { c.mainHandler = h }

// SetCancelHandler sets the handler for messages on the cancel topic.
// Must be called before Start.
func (c *Client) SetCancelHandler(h MessageHandler) { c.cancelHandler = h }
