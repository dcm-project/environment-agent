// Package dcm implements DCM registration and heartbeat lifecycle management.
package dcm

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	"github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/backoff"
)

var ErrNonRetryable = errors.New("non-retryable DCM error")

// registrarPanicRestartDelay bounds how long the supervisor pauses before
// restarting run() after a recovered panic — long enough to avoid a tight
// crash loop if the panic is deterministic (e.g. a consistently-panicking
// dependency), short enough that a one-off panic doesn't meaningfully delay
// recovery. Deliberately a fixed constant, not derived from
// InitialBackoff/MaxBackoff: those tune the DCM HTTP registration retry
// cadence (an expected, normal failure mode), whereas this only fires on the
// "should never happen" path of a panicking dependency.
const registrarPanicRestartDelay = 1 * time.Second

// ServiceTypeLister returns the set of currently advertisable service types
// (backed by SPs in Ready or Unhealthy state — NOT Unavailable).
type ServiceTypeLister interface {
	AdvertisableServiceTypes() []string
}

// ConsumerLagProvider returns the current consumer lag for heartbeat payloads.
type ConsumerLagProvider interface {
	ConsumerLag() int64
}

// ResourceCapacityProvider optionally returns resource availability for registration.
// Returns nil when not available (REQ-DCM-030 is SHOULD, not MUST).
type ResourceCapacityProvider interface {
	ResourceCapacity() *v1alpha1.ResourceCapacity
}

// RegistrarConfig holds the configuration for DCM registration.
type RegistrarConfig struct {
	AgentName                 string
	Environment               string
	Cost                      string
	TopicName                 string
	RegistrationURL           string
	InitialBackoff            time.Duration
	MaxBackoff                time.Duration
	HeartbeatInterval         time.Duration
	PrerequisiteRetryInterval time.Duration
}

// Registrar handles DCM registration and heartbeat lifecycle.
type Registrar struct {
	client           *dcmClient
	config           RegistrarConfig
	lister           ServiceTypeLister
	lagProvider      ConsumerLagProvider
	resourceProvider ResourceCapacityProvider
	logger           *slog.Logger

	mu         sync.Mutex
	agentID    string
	registered bool

	startOnce sync.Once
	done      chan struct{}
	notifyCh  chan struct{}
}

// NewRegistrar creates a Registrar. Returns error if config is invalid.
func NewRegistrar(
	cfg RegistrarConfig,
	lister ServiceTypeLister,
	lagProvider ConsumerLagProvider,
	resourceProvider ResourceCapacityProvider,
	logger *slog.Logger,
) (*Registrar, error) {
	client, err := newDCMClient(cfg.RegistrationURL)
	if err != nil {
		return nil, err
	}
	return &Registrar{
		client:           client,
		config:           cfg,
		lister:           lister,
		lagProvider:      lagProvider,
		resourceProvider: resourceProvider,
		logger:           logger,
		done:             make(chan struct{}),
		notifyCh:         make(chan struct{}, 1),
	}, nil
}

// Start begins the async registration + heartbeat loop. Non-blocking, idempotent.
func (r *Registrar) Start(ctx context.Context) {
	r.startOnce.Do(func() {
		go r.runSupervised(ctx)
	})
}

// runSupervised wraps run() in a panic-recovering supervisor loop. An
// unrecovered panic from run() (e.g. a panicking ServiceTypeLister
// dependency) must not crash the whole agent process — DCM registration is a
// background concern and must not be able to take down request routing / the
// HTTP server with it. Recovering the panic but still
// letting the goroutine return would only solve half the problem: DCM
// registration/heartbeating would then be silently, permanently stalled for
// the rest of the process lifetime with no user-visible symptom besides "DCM
// never sees this agent again". So on panic, run() is restarted (after
// registrarPanicRestartDelay) instead of letting the goroutine exit —
// re-entering run() re-runs the prerequisite wait and re-registers from
// scratch, which is safe: registration is idempotent per REQ-DCM-080 even if
// the agent was already registered before the panic.
func (r *Registrar) runSupervised(ctx context.Context) {
	defer close(r.done)
	for {
		if ctx.Err() != nil {
			return
		}
		if r.runRecovering(ctx) {
			// run() returned normally — only happens on ctx cancellation
			// (see run()'s own control flow), so the supervisor is done too.
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(registrarPanicRestartDelay):
		}
	}
}

// runRecovering calls run() with panic recovery. Returns true if run()
// returned normally (no panic), false if a panic was recovered — the caller
// decides whether/when to restart based on the return value.
func (r *Registrar) runRecovering(ctx context.Context) (completedNormally bool) {
	completedNormally = true
	defer func() {
		if rec := recover(); rec != nil {
			completedNormally = false
			r.logger.Error("panic in DCM registrar goroutine, restarting",
				"panic", rec, "stack", string(debug.Stack()))
		}
	}()
	r.run(ctx)
	return completedNormally
}

// Done returns a channel closed when the registrar goroutine exits.
func (r *Registrar) Done() <-chan struct{} {
	return r.done
}

// NotifyServiceTypeChange signals that the advertisable service types may have changed.
func (r *Registrar) NotifyServiceTypeChange() {
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
}

// AgentID returns the DCM-assigned agent ID, or ("", false) if not yet registered.
func (r *Registrar) AgentID() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.agentID, r.registered
}

func (r *Registrar) run(ctx context.Context) {
	// Prerequisite gate: wait for non-empty service types.
	// Retries periodically to recover from transient lister errors that return
	// empty without triggering a notification.
	retryInterval := r.config.PrerequisiteRetryInterval
	if retryInterval <= 0 {
		retryInterval = 5 * time.Second
	}
	retryTicker := time.NewTicker(retryInterval)
	defer retryTicker.Stop()

	for {
		types := r.lister.AdvertisableServiceTypes()
		if len(types) > 0 {
			break
		}
		r.logger.Info("waiting for advertisable service types before registering")
		select {
		case <-ctx.Done():
			return
		case <-r.notifyCh:
		case <-retryTicker.C:
		}
	}
	retryTicker.Stop()

	// Registration loop with backoff
	if !r.doRegistration(ctx) {
		// Non-retryable failure or context cancelled — block until shutdown
		<-ctx.Done()
		return
	}

	// Post-registration: heartbeat + service-type update loop
	ticker := time.NewTicker(r.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sendHeartbeat(ctx)
		case <-r.notifyCh:
			r.reRegister(ctx)
		}
	}
}

// doRegistration attempts registration with backoff. Returns true on success, false on permanent failure or cancellation.
func (r *Registrar) doRegistration(ctx context.Context) bool {
	attempt := 0
	for {
		if ctx.Err() != nil {
			return false
		}

		agentID, err := r.client.register(ctx, r.buildPayload())
		if err == nil {
			r.mu.Lock()
			r.agentID = agentID
			r.registered = true
			r.mu.Unlock()
			r.logger.Info("registered with DCM", "agent_id", agentID)
			return true
		}

		if ctx.Err() != nil {
			return false
		}

		if errors.Is(err, ErrNonRetryable) {
			r.logger.Error("non-retryable DCM registration error", "error", err)
			return false
		}

		// Determine backoff duration
		wait := r.computeBackoff(err, attempt)
		attempt++
		r.logger.Warn("DCM registration failed, retrying", "error", err, "attempt", attempt, "backoff", wait)

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-r.notifyCh:
			timer.Stop()
			attempt = 0
			continue
		case <-timer.C:
		}
	}
}

func (r *Registrar) computeBackoff(err error, attempt int) time.Duration {
	var rle *RateLimitError
	if errors.As(err, &rle) && rle.HasRetryAfter {
		// ponytail: Retry-After intentionally NOT capped by MaxBackoff — server directive per REQ-DCM-060.
		// DCM is our own trusted control plane; cap at MaxBackoff if trust boundary changes.
		if rle.RetryAfter <= 0 {
			return 0
		}
		return rle.RetryAfter
	}
	calculated := backoff.CalculateBackoff(r.config.InitialBackoff, r.config.MaxBackoff, attempt)
	return backoff.ApplyJitter(calculated, rand.Float64)
}

func (r *Registrar) sendHeartbeat(ctx context.Context) {
	r.mu.Lock()
	id := r.agentID
	r.mu.Unlock()

	payload := heartbeatPayload{
		Timestamp:   time.Now().UTC(),
		ConsumerLag: r.lagProvider.ConsumerLag(),
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := r.client.heartbeat(reqCtx, id, payload); err != nil {
		r.logger.Warn("heartbeat failed", "error", err)
	}
}

func (r *Registrar) reRegister(ctx context.Context) {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	agentID, err := r.client.register(reqCtx, r.buildPayload())
	if err != nil {
		r.logger.Warn("re-registration failed", "error", err)
		return
	}
	r.mu.Lock()
	r.agentID = agentID
	r.registered = true
	r.mu.Unlock()
}

func (r *Registrar) buildPayload() registrationPayload {
	types := r.lister.AdvertisableServiceTypes()
	if types == nil {
		types = make([]string, 0)
	}

	p := registrationPayload{
		Name:         r.config.AgentName,
		Environment:  r.config.Environment,
		Cost:         r.config.Cost,
		TopicName:    r.config.TopicName,
		ServiceTypes: types,
	}

	if r.resourceProvider != nil {
		if rc := r.resourceProvider.ResourceCapacity(); rc != nil {
			p.ResourcesAvailable = rc
		}
	}
	return p
}
