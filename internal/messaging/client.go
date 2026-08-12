// Package messaging provides NATS/JetStream messaging client and topic management.
package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dcm-project/environment-agent/internal/backoff"
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

	// shutdownDrainTimeout bounds how long Stop waits for in-flight message
	// handlers to finish (via ConsumeContext.Drain) before force-closing the
	// NATS connection. See Stop's doc comment.
	shutdownDrainTimeout = 5 * time.Second

	// requestStreamRetryInterval/Timeout bound the retry loop for creating
	// durable consumers on the control-plane-owned RequestStreamName. The CP
	// may not have created that stream yet when this agent starts (startup
	// order isn't guaranteed, per REQ-MSG-051).
	requestStreamRetryInterval = 2 * time.Second
	requestStreamRetryTimeout  = 30 * time.Second

	// defaultReconnectInitialBackoff/MaxBackoff are used when ClientConfig
	// leaves the reconnect backoff bounds unset (zero value) — used by tests
	// that construct a bare ClientConfig. See reconnectDelay.
	defaultReconnectInitialBackoff = 1 * time.Second
	defaultReconnectMaxBackoff     = 30 * time.Second
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
	// ReconnectInitialBackoff/ReconnectMaxBackoff bound the exponential
	// backoff-with-jitter formula used between NATS reconnect attempts —
	// same formula as REQ-DCM-050 (min(initial×2^attempt, max) with full
	// jitter), per REQ-MSG-100's cross-reference to it. Zero/unset falls
	// back to defaultReconnectInitialBackoff/MaxBackoff.
	ReconnectInitialBackoff time.Duration
	ReconnectMaxBackoff     time.Duration
	// DeferConsume, if true, makes Start set up streams/consumers and run the
	// cancel-topic drain, but NOT begin live main/cancel consumption — the
	// caller must call StartConsuming() once ready. Lets a caller drain the
	// retry topic before live consumption starts, avoiding a message-stealing
	// race between the two. Defaults to false (consume immediately).
	DeferConsume bool
}

// Client manages NATS/JetStream connectivity and topic consumption.
type Client struct {
	cfg    ClientConfig
	logger *slog.Logger
	topics TopicNames

	conn       *nats.Conn
	js         jetstream.JetStream
	mainCons   jetstream.Consumer
	cancelCons jetstream.Consumer // retained for beginConsuming when DeferConsume is set
	consumers  []jetstream.ConsumeContext
	consuming  bool // guarded by mu; true once the live Consume() loops have started
	mu         sync.Mutex

	connected atomic.Bool
	stopped   atomic.Bool
	// consumeRequested latches a StartConsuming() call that arrives before
	// setup has finished (e.g. NATS still connecting) so setup starts
	// consuming automatically once it completes. Only relevant when
	// DeferConsume is true.
	consumeRequested atomic.Bool

	// mainHandler/cancelHandler must be set before Start; not safe for
	// concurrent mutation afterward.
	mainHandler   MessageHandler
	cancelHandler MessageHandler

	setupMu   sync.Mutex
	setupDone bool
	retrying  bool // guards against spawning more than one background retry goroutine
	stopOnce  sync.Once
	stopCh    chan struct{}

	// randFn is overridable in unit tests for deterministic reconnectDelay
	// assertions; production always uses math/rand's global source.
	randFn func() float64

	// onSetupReady, if set, fires exactly once, synchronously from within
	// setupStreamsAndConsume, the first time JetStream/durable-consumer setup
	// succeeds. Only meaningful when DeferConsume is true; the callback owns
	// calling StartConsuming() once it's done with any pre-consumption work.
	// Needed because Start is non-blocking (AC-MSG-050), so a caller can't
	// just run setup-dependent logic synchronously after Start returns.
	onSetupReady func()
}

// NewClient creates a new messaging client. Does NOT connect to NATS.
func NewClient(cfg ClientConfig, logger *slog.Logger) *Client {
	return &Client{
		cfg:    cfg,
		logger: logger,
		topics: DeriveTopicNames(cfg.AgentName, cfg.TopicName),
		stopCh: make(chan struct{}),
		randFn: rand.Float64,
	}
}

// SetOnSetupReady registers a callback invoked exactly once, the first time
// JetStream/durable-consumer setup succeeds. Must be called before Start
// (same set-before-Start contract as SetMainHandler/SetCancelHandler) —
// setup can complete as early as Start's own synchronous connect attempt.
func (c *Client) SetOnSetupReady(fn func()) {
	c.onSetupReady = fn
}

// reconnectDelay computes the NATS reconnect wait for the given attempt
// count using exponential backoff with full jitter — the same formula as
// REQ-DCM-050, which REQ-MSG-100 explicitly cross-references. Passed to
// nats.CustomReconnectDelay in Start.
func (c *Client) reconnectDelay(attempts int) time.Duration {
	initial, maxWait := c.cfg.ReconnectInitialBackoff, c.cfg.ReconnectMaxBackoff
	if initial <= 0 {
		initial = defaultReconnectInitialBackoff
	}
	if maxWait <= 0 {
		maxWait = defaultReconnectMaxBackoff
	}
	calculated := backoff.CalculateBackoff(initial, maxWait, attempts)
	return backoff.ApplyJitter(calculated, c.randFn)
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
		nats.CustomReconnectDelay(c.reconnectDelay),
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

// doSetup creates streams/consumers and starts consuming. Invoked from
// Start's initial connect attempt and from every ConnectHandler/
// ReconnectHandler. A single attempt can block for up to
// requestStreamRetryTimeout waiting on RequestStreamName (REQ-MSG-051); if it still
// fails, doSetup hands off to retrySetupInBackground rather than retrying
// inline, since Start itself must stay non-blocking.
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

	c.mu.Lock()
	if c.stopped.Load() {
		c.mu.Unlock()
		return false
	}
	c.conn = conn
	c.js = js
	c.mainCons = mainCons
	c.cancelCons = cancelCons
	c.mu.Unlock()

	return c.finishSetup()
}

// finishSetup runs the "what happens once streams/consumers exist" decision:
// either fire onSetupReady (DeferConsume, first time) or begin consuming
// immediately (default, non-deferred behavior). Split out from
// setupStreamsAndConsume so it can be unit-tested directly against a client
// with js/mainCons/cancelCons already populated, without needing a live NATS
// connection.
func (c *Client) finishSetup() bool {
	if c.cfg.DeferConsume && !c.consumeRequested.Load() {
		// onSetupReady's callback is expected to call StartConsuming() synchronously.
		if c.onSetupReady != nil {
			c.onSetupReady()
		}
		// Must report failure (not success) if the callback's StartConsuming
		// didn't actually start consuming, so attemptSetup retries instead of
		// latching setupDone and permanently stranding the client.
		if c.consumeRequested.Load() && !c.isConsuming() {
			return false
		}
		return true
	}
	if err := c.beginConsuming(); err != nil {
		return false
	}
	return true
}

func (c *Client) isConsuming() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consuming
}

// beginConsuming starts the live pull-consumer loops for main+cancel against
// the already-created durable consumers. Idempotent and concurrency-safe:
// c.mu is held for the whole check-then-act sequence (the Consume() calls
// themselves are non-blocking) so two overlapping callers can't both start a
// duplicate consume loop on the same durable consumer.
func (c *Client) beginConsuming() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consuming || c.stopped.Load() {
		return nil
	}
	mainCons, cancelCons := c.mainCons, c.cancelCons
	if mainCons == nil || cancelCons == nil {
		// Setup hasn't created the durable consumers yet; StartConsuming's
		// consumeRequested latch (or the default non-deferred path) ensures
		// this runs again once setupStreamsAndConsume finishes.
		return nil
	}

	cancelCC, err := cancelCons.Consume(func(msg jetstream.Msg) {
		c.handleCancelMessage(msg)
	})
	if err != nil {
		c.logger.Error("failed to start cancel consumer", "error", err)
		return err
	}
	mainCC, err := mainCons.Consume(func(msg jetstream.Msg) {
		c.handleMainMessage(msg)
	})
	if err != nil {
		cancelCC.Stop()
		c.logger.Error("failed to start main consumer", "error", err)
		return err
	}

	if c.stopped.Load() {
		cancelCC.Stop()
		mainCC.Stop()
		return nil
	}
	c.consumers = append(c.consumers, cancelCC, mainCC)
	c.consuming = true
	c.logger.Info("messaging ready, main/cancel consumption started")
	return nil
}

// StartConsuming begins live consumption of the main and cancel topics. Only
// meaningful when ClientConfig.DeferConsume is true (a no-op otherwise, since
// Start already began consuming). Safe to call before setup has finished
// connecting/creating consumers (e.g. NATS still reconnecting) — the request
// is latched and consuming begins automatically once setup completes.
func (c *Client) StartConsuming() {
	c.consumeRequested.Store(true)
	_ = c.beginConsuming()
}

// initInternalStreams creates the agent-owned streams only. The request
// subjects (dcm.agent.>) and response subject (dcm.agents.responses) are
// owned by the control-plane (REQ-MSG-048) — this agent must not create streams for
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
	// Retry consumer: agent-owned stream, used by retry.Processor via JetStream Consumer binding.
	// Intentionally has no MaxDeliver limit — see DD-410. Retry-topic residency is bounded by
	// SP health-state transitions (REQ-HMN-150), not by delivery count: a MaxDeliver ceiling
	// here would let cancel-purge Naks for unrelated resources (router.purgeFromRetryTopic)
	// prematurely exhaust a message that was never actually offered to its SP that many times.
	if _, err := retryS.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: c.topics.RetryConsumer(), AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: c.cfg.AckWait,
	}); err != nil {
		return nil, nil, fmt.Errorf("failed to create retry consumer: %w", err)
	}

	c.mu.Lock()
	c.mainCons = mainCons
	c.mu.Unlock()

	return mainCons, cancelCons, nil
}

// createRequestConsumer creates a durable consumer on the control-plane-owned
// RequestStreamName, filtered to the given subject. Retries internally for up
// to requestStreamRetryTimeout since the CP may not have created that stream
// yet at agent startup (REQ-MSG-051); the caller (doSetup) retries the whole setup
// indefinitely beyond that.
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
			// Retry rather than abort: an incomplete deny-list before
			// main-topic processing begins would violate REQ-MSG-090.
			// drainTimeout still bounds the total time this can take.
			c.logger.Warn("cancel topic drain fetch failed, retrying", "error", err)
			select {
			case <-drainCtx.Done():
				return
			case <-time.After(drainBatchWait):
			}
			continue
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
//
// Uses ConsumeContext.Drain (not Stop) so an in-flight handler gets to finish
// (ack/nak/publish) instead of being discarded and redelivered on restart.
// Bounded by shutdownDrainTimeout so a hung handler can't block shutdown.
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
			cc.Drain()
		}
		for _, cc := range consumers {
			select {
			case <-cc.Closed():
			case <-time.After(shutdownDrainTimeout):
			}
		}
		// Wait for any in-flight handler invocation (and its Ack/Nak) to
		// finish before closing the connection, otherwise a concurrent
		// Ack/Nak publish can race with conn.Close() and be lost, leaving
		// the message stuck until the full AckWait timeout instead of
		// being promptly redelivered.
		for _, cc := range consumers {
			select {
			case <-cc.Closed():
			case <-time.After(drainTimeout):
			}
		}
		if conn != nil {
			conn.Close()
		}
		c.connected.Store(false)
		c.logger.Info("messaging stopped")
	})
}

// IsConnected returns the cached connectivity state (no I/O).
func (c *Client) IsConnected() bool { return c.connected.Load() }

// consumerLagInfoTimeout bounds the live JetStream round-trip ConsumerLag
// makes to fetch current consumer state. CachedInfo() is not used here
// because it reflects only whatever was fetched at consumer creation/bind
// time and is never refreshed as messages are delivered — it would report a
// perpetually stale (typically zero) lag.
const consumerLagInfoTimeout = 5 * time.Second

// ConsumerLag returns the current consumer lag from JetStream: the number of
// messages not yet acknowledged on the main-subject durable consumer, per
// REQ-DCM-150 (NumPending: not yet delivered; NumAckPending: delivered but
// not yet acked — both are "unacknowledged").
func (c *Client) ConsumerLag() int64 {
	c.mu.Lock()
	cons := c.mainCons
	c.mu.Unlock()

	if cons == nil {
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), consumerLagInfoTimeout)
	defer cancel()
	info, err := cons.Info(ctx)
	if err != nil {
		c.logger.Warn("failed to fetch consumer info for lag reporting, using last-known value", "error", err)
		cached := cons.CachedInfo()
		return int64(cached.NumPending) + int64(cached.NumAckPending)
	}
	return int64(info.NumPending) + int64(info.NumAckPending)
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
// binding that subject (REQ-MSG-048) — the agent does not create its own stream for
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
// server-side dedup, using the CE's own id (REQ-MSG-135). Used for response
// CEs to guard against duplicate delivery on a future publish-retry.
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
