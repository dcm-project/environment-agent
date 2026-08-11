// Package routing dispatches resource operation requests to service providers.
package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/dcm-project/environment-agent/internal/backoff"
	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
)

// claimedResourcesSetMaxSize bounds the claimed-resources-set LRU to prevent unbounded memory
// growth. Under extreme load (>100k unique concurrent resources) the oldest entries
// evict, which means a cancel for an evicted resourceId would receive cancel-ack
// instead of cancel-rejected.
const claimedResourcesSetMaxSize = 100000

// Router dispatches resource operation CEs to the appropriate SP.
type Router struct {
	registry            *provider.Registry
	healthTracker       provider.HealthTracker
	store               store.Store
	forwarder           SPForwarder
	publisher           Publisher
	rcMu                sync.RWMutex       // protects retryConsumer for late-bind via SetRetryConsumer
	retryConsumer       RetryTopicConsumer // set after construction via SetRetryConsumer
	denyList            *ResourceSet
	claimedResourcesSet *ResourceSet
	config              config.RoutingConfig
	logger              *slog.Logger
	agentName           string
	topicName           string
	retryTopic          string
}

// RouterDeps holds all dependencies for the Router.
type RouterDeps struct {
	Registry      *provider.Registry
	HealthTracker provider.HealthTracker
	Store         store.Store
	Forwarder     SPForwarder // nil-safe; checked at use site
	Publisher     Publisher
	RetryConsumer RetryTopicConsumer // nil-safe; late-bound via SetRetryConsumer
	DenyList      *ResourceSet
	Config        config.RoutingConfig
	Logger        *slog.Logger
	AgentName     string
	// TopicName is the CP-facing subject advertised on registration, used
	// only for CE correlation (ResponseContext.TopicName).
	TopicName string
	// RetryTopic is the agent-internal subject used to hold requests while
	// an SP is unhealthy (distinct from TopicName since the CP/agent
	// alignment migration prefixes TopicName with "dcm.agent.").
	RetryTopic string
}

// NewRouter creates a Router with the given dependencies.
// Panics if required dependencies (Registry, HealthTracker, Store, Publisher,
// DenyList, Logger) are nil.
func NewRouter(deps RouterDeps) *Router {
	if deps.Registry == nil {
		panic("routing.NewRouter: Registry must not be nil")
	}
	if deps.HealthTracker == nil {
		panic("routing.NewRouter: HealthTracker must not be nil")
	}
	if deps.Store == nil {
		panic("routing.NewRouter: Store must not be nil")
	}
	if deps.Publisher == nil {
		panic("routing.NewRouter: Publisher must not be nil")
	}
	if deps.DenyList == nil {
		panic("routing.NewRouter: DenyList must not be nil")
	}
	if deps.Logger == nil {
		panic("routing.NewRouter: Logger must not be nil")
	}
	return &Router{
		registry:            deps.Registry,
		healthTracker:       deps.HealthTracker,
		store:               deps.Store,
		forwarder:           deps.Forwarder,
		publisher:           deps.Publisher,
		retryConsumer:       deps.RetryConsumer,
		denyList:            deps.DenyList,
		claimedResourcesSet: NewResourceSet(claimedResourcesSetMaxSize),
		config:              deps.Config,
		logger:              deps.Logger,
		agentName:           deps.AgentName,
		topicName:           deps.TopicName,
		retryTopic:          deps.RetryTopic,
	}
}

func (r *Router) responseCtx(resourceID string) ResponseContext {
	return ResponseContext{ResourceID: resourceID, AgentName: r.agentName, TopicName: r.topicName}
}

// publishCE publishes an outcome CE. ceID is the inbound CE id that
// triggered this publish (empty when no single inbound CE applies), included
// in the logs so redeliveries can be correlated back to their trigger.
func (r *Router) publishCE(ctx context.Context, ceType, resourceID, ceID string, data any) {
	if err := cloudevent.PublishCE(ctx, r.publisher.PublishWithMsgID, cloudevent.SubjectResponses, r.agentName, ceType, data); err != nil {
		r.logger.Warn("failed to publish CE", "ce_type", ceType, "resource_id", resourceID, "ce_id", ceID, "error", err)
		return
	}
	r.logger.Info("published CE", "ce_type", ceType, "resource_id", resourceID, "ce_id", ceID)
}

// SetRetryConsumer late-binds the retry consumer after the messaging client
// starts (JS context ready). Thread-safe for concurrent HandleCancel reads.
func (r *Router) SetRetryConsumer(rc RetryTopicConsumer) {
	r.rcMu.Lock()
	r.retryConsumer = rc
	r.rcMu.Unlock()
}

// ClaimedResourcesSet returns the claimed-resources-set for sharing with the retry Processor.
func (r *Router) ClaimedResourcesSet() *ResourceSet { return r.claimedResourcesSet }

// HandleRequest routes a creation or deletion CE to the appropriate SP.
func (r *Router) HandleRequest(ctx context.Context, msg []byte) error {
	isCreate, payload, drop := r.parseRequestCE(msg)
	if drop {
		return nil
	}

	if payload.ResourceID == "" || payload.ServiceType == "" {
		r.logger.Warn("CE missing required fields", "resource_id", payload.ResourceID, "service_type", payload.ServiceType, "ce_id", payload.EventID)
		r.publishCE(ctx, cloudevent.TypeError, "", payload.EventID, ErrorData{
			ResponseContext: r.responseCtx(""),
			Error:           ErrorInvalidPayload, Details: "resourceId and serviceType are required",
		})
		return nil
	}

	if isCreate && r.denyList.Consume(payload.ResourceID) {
		r.logger.Info("create request dropped, resource in deny list", "resource_id", payload.ResourceID)
		return nil
	}

	sp, status, ok, storeErr := r.resolveProvider(ctx, payload.ServiceType)
	if storeErr != nil {
		r.logger.Warn("transient store error during provider resolution", "error", storeErr,
			"resource_id", payload.ResourceID, "service_type", payload.ServiceType, "ce_id", payload.EventID)
		return storeErr
	}
	if !ok {
		r.publishCE(ctx, cloudevent.TypeError, payload.ResourceID, payload.EventID, ErrorData{
			ResponseContext: r.responseCtx(payload.ResourceID),
			Error:           ErrorUnsupportedServiceType, Details: "provider not found for service type: " + payload.ServiceType,
		})
		return nil
	}

	if status == v1alpha1.Unavailable {
		r.publishCE(ctx, cloudevent.TypeError, payload.ResourceID, payload.EventID, ErrorData{
			ResponseContext: r.responseCtx(payload.ResourceID),
			Error:           ErrorSPUnavailable, Details: "provider unavailable for service type: " + payload.ServiceType,
		})
		return nil
	}
	if status == v1alpha1.Unhealthy {
		if err := r.publisher.Publish(ctx, r.retryTopic, msg); err != nil {
			return err
		}
		r.publishCE(ctx, cloudevent.TypeRequestQueued, payload.ResourceID, payload.EventID, RequestQueuedData{
			ResponseContext: r.responseCtx(payload.ResourceID),
			ServiceType:     payload.ServiceType, Status: "QUEUED",
		})
		return nil
	}

	if r.forwarder == nil {
		r.publishCE(ctx, cloudevent.TypeError, payload.ResourceID, payload.EventID, ErrorData{
			ResponseContext: r.responseCtx(payload.ResourceID),
			Error:           ErrorSPUnavailable, Details: "provider unavailable for service type: " + payload.ServiceType,
		})
		return nil
	}

	return r.forwardWithRetry(ctx, sp, isCreate, payload)
}

// parseRequestCE unmarshals a CE and returns the parsed fields.
// Returns drop=true for malformed or unknown CE types (caller should return nil to ack-drop).
func (r *Router) parseRequestCE(msg []byte) (isCreate bool, payload inboundPayload, drop bool) {
	var event cloudevents.Event
	if jsonErr := json.Unmarshal(msg, &event); jsonErr != nil {
		r.logger.Warn("dropping malformed CE", "error", jsonErr)
		return false, payload, true
	}

	switch event.Type() {
	case cloudevent.TypeRequestCreate:
		isCreate = true
	case cloudevent.TypeRequestDelete:
		isCreate = false
	default:
		r.logger.Warn("dropping unknown CE type", "ce_type", event.Type(), "ce_id", event.ID())
		return false, payload, true
	}

	if jsonErr := json.Unmarshal(event.Data(), &payload); jsonErr != nil {
		r.logger.Warn("dropping CE with unparseable data", "error", jsonErr, "ce_id", event.ID(), "ce_type", event.Type())
		return false, payload, true
	}
	payload.EventID = event.ID()
	return isCreate, payload, false
}

func (r *Router) resolveProvider(ctx context.Context, serviceType string) (*store.StoredProvider, v1alpha1.ProviderStatus, bool, error) {
	return ResolveProvider(ctx, r.registry, r.store, r.healthTracker, r.logger, serviceType)
}

// forwardWithRetry forwards a request to the SP with retry logic and
// claimed-resources-set tracking.
func (r *Router) forwardWithRetry(ctx context.Context, sp *store.StoredProvider, isCreate bool, payload inboundPayload) error {
	// claimedResourcesSet is a cancel-rejection ledger that persists across the
	// resource lifecycle (create → delete). AddIfAbsent returning false for
	// a delete-after-create is legitimate; SPs handle idempotency (REQ-SP-010).
	newlyAdded := r.claimedResourcesSet.AddIfAbsent(payload.ResourceID)

	// TOCTOU mitigation: re-check denyList after claiming the claimedResourcesSet
	// slot. A concurrent HandleCancel may have added to denyList between
	// the first check in HandleRequest and here (across resolveProvider I/O).
	if isCreate && r.denyList.Consume(payload.ResourceID) {
		if newlyAdded {
			r.claimedResourcesSet.Remove(payload.ResourceID)
		}
		r.logger.Info("create request dropped, resource in deny list", "resource_id", payload.ResourceID)
		return nil
	}

	success := false
	defer func() {
		if !success && newlyAdded {
			r.claimedResourcesSet.Remove(payload.ResourceID)
		}
	}()

	fwdErr := r.attemptForward(ctx, sp, isCreate, payload)
	if fwdErr == nil {
		success = true
		if isCreate {
			r.publishCE(ctx, cloudevent.TypeCreationAcked, payload.ResourceID, payload.EventID, CreationAckData{
				ResponseContext: r.responseCtx(payload.ResourceID), Status: "PROVISIONING",
			})
		} else {
			r.publishCE(ctx, cloudevent.TypeDeletionAcked, payload.ResourceID, payload.EventID, DeletionAckData{
				ResponseContext: r.responseCtx(payload.ResourceID), Status: "DELETING",
			})
		}
		return nil
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	r.logger.Warn("SP error", append([]any{
		"resource_id", payload.ResourceID, "service_type", payload.ServiceType,
		"ce_id", payload.EventID, "provider_id", sp.ID,
	}, SafeErrorAttrs(fwdErr)...)...)
	if !IsRetryable(fwdErr) {
		r.publishCE(ctx, cloudevent.TypeError, payload.ResourceID, payload.EventID, ErrorData{
			ResponseContext: r.responseCtx(payload.ResourceID),
			Error:           ErrorNonRetryable, Details: "service provider returned non-retryable error for service type: " + payload.ServiceType,
		})
	} else {
		r.publishCE(ctx, cloudevent.TypeError, payload.ResourceID, payload.EventID, ErrorData{
			ResponseContext: r.responseCtx(payload.ResourceID),
			Error:           ErrorRetryExhausted, Details: "service provider error after retry exhaustion for service type: " + payload.ServiceType,
		})
	}
	return nil
}

// attemptForward executes the SP call with retries. Returns nil on success or
// the raw SP error (retryable after exhaustion, or non-retryable). Returns
// ctx.Err() on context cancellation. No CEs are published — the caller owns
// all response-event logic.
func (r *Router) attemptForward(ctx context.Context, sp *store.StoredProvider, isCreate bool, payload inboundPayload) error {
	params := ForwardParams{
		ResourceID: payload.ResourceID, ServiceType: payload.ServiceType,
		Spec: payload.Spec, EventID: payload.EventID, IsCreate: isCreate,
	}
	maxAttempts := r.config.RetryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var fwdErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		fwdErr = ForwardToSP(ctx, r.forwarder, sp, params)
		if fwdErr == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if !IsRetryable(fwdErr) {
			return fwdErr
		}
		if attempt < maxAttempts-1 {
			r.logger.Warn("SP call failed, retrying", append([]any{
				"resource_id", payload.ResourceID, "service_type", payload.ServiceType,
				"attempt", attempt + 1, "max_attempts", maxAttempts,
				"ce_id", payload.EventID, "provider_id", sp.ID,
			}, SafeErrorAttrs(fwdErr)...)...)
			delay := backoff.ApplyJitter(
				backoff.CalculateBackoff(r.config.RetryBackoff, r.config.RetryMaxBackoff, attempt),
				rand.Float64,
			)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return fwdErr
}

// HandleCancel processes a cancel CE for a given resourceId.
func (r *Router) HandleCancel(ctx context.Context, msg []byte) error {
	var event cloudevents.Event
	if err := json.Unmarshal(msg, &event); err != nil {
		r.logger.Warn("dropping malformed cancel CE", "error", err)
		return nil
	}

	if event.Type() != cloudevent.TypeRequestCancel {
		r.logger.Warn("dropping non-cancel CE on cancel topic", "ce_type", event.Type(), "ce_id", event.ID())
		return nil
	}

	var payload inboundPayload
	if err := json.Unmarshal(event.Data(), &payload); err != nil {
		r.logger.Warn("dropping cancel CE with unparseable data", "error", err, "ce_id", event.ID(), "ce_type", event.Type())
		return nil
	}
	payload.EventID = event.ID()

	if payload.ResourceID == "" || payload.ServiceType == "" {
		r.logger.Warn("cancel CE missing required fields", "resource_id", payload.ResourceID, "service_type", payload.ServiceType, "ce_id", payload.EventID)
		r.publishCE(ctx, cloudevent.TypeError, "", payload.EventID, ErrorData{
			ResponseContext: r.responseCtx(""),
			Error:           ErrorInvalidPayload, Details: "resourceId and serviceType are required",
		})
		return nil
	}

	if r.claimedResourcesSet.Contains(payload.ResourceID) {
		r.publishCE(ctx, cloudevent.TypeCancelRejected, payload.ResourceID, payload.EventID, CancelRejectedData{
			ResponseContext: r.responseCtx(payload.ResourceID),
			Reason:          "resource already claimed",
		})
		return nil
	}

	r.rcMu.RLock()
	rc := r.retryConsumer
	r.rcMu.RUnlock()

	// Deny-early: visible to concurrent ProcessOnTransition before retry purge
	r.denyList.Add(payload.ResourceID)

	if rc != nil {
		if err := r.purgeFromRetryTopic(ctx, rc, payload.ResourceID); err != nil {
			return err
		}
	}

	r.publishCE(ctx, cloudevent.TypeCancelAcked, payload.ResourceID, payload.EventID, CancelAckData{
		ResponseContext: r.responseCtx(payload.ResourceID),
		ServiceType:     payload.ServiceType,
	})
	return nil
}

// purgeFromRetryTopic drains all retry messages, acking those matching the
// cancelled resourceID and Nak'ing the rest back in place. Non-matching
// messages are Nak'd on the same JetStream message rather than
// acked-and-republished, since they already live on this stream and don't
// need to move. The retry-subject consumer has no MaxDeliver limit
// (DD-410), so this choice doesn't affect delivery-count accounting.
func (r *Router) purgeFromRetryTopic(ctx context.Context, rc RetryTopicConsumer, resourceID string) error {
	messages, err := rc.FetchRetryMessages(ctx)
	if err != nil {
		return err
	}

	var matched, requeued int
	for _, m := range messages {
		if m.ResourceID == resourceID {
			if err := m.AckFunc(); err != nil {
				return fmt.Errorf("failed to ack cancelled message: %w", err)
			}
			matched++
			continue
		}
		if err := m.NakFunc(); err != nil {
			return fmt.Errorf("failed to nak non-matching message: %w", err)
		}
		requeued++
	}
	r.logger.Info("retry topic purged for cancel", "resource_id", resourceID, "matched", matched, "requeued", requeued)
	return nil
}
