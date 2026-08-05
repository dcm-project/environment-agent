package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/dcm-project/environment-agent/internal/backoff"
	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
)

// dispatchedSetMaxSize bounds the dispatched-set LRU to prevent unbounded memory
// growth. Under extreme load (>100k unique concurrent resources) the oldest entries
// evict, which means a cancel for an evicted resourceId would receive cancel-ack
// instead of cancel-rejected. A TTL-based approach would be more precise but is
// deferred to the retry/lifecycle redesign (Topic 9).
const dispatchedSetMaxSize = 100000

// Router dispatches resource operation CEs to the appropriate SP.
type Router struct {
	registry      *provider.Registry
	healthTracker provider.HealthTracker
	store         store.Store
	forwarder     SPForwarder
	publisher     Publisher
	retryConsumer RetryTopicConsumer
	denyList      *DenyList
	dispatchedSet *DenyList
	config        config.RoutingConfig
	logger        *slog.Logger
	agentName     string
	topicName     string
}

// NewRouter creates a Router with the given dependencies.
// Panics if required dependencies (registry, healthTracker, store, publisher,
// denyList) are nil. forwarder and retryConsumer are nil-safe (checked at use site).
func NewRouter(
	registry *provider.Registry,
	healthTracker provider.HealthTracker,
	st store.Store,
	forwarder SPForwarder,
	publisher Publisher,
	retryConsumer RetryTopicConsumer,
	denyList *DenyList,
	cfg config.RoutingConfig,
	logger *slog.Logger,
	agentName string,
	topicName string,
) *Router {
	if registry == nil {
		panic("routing.NewRouter: registry must not be nil")
	}
	if healthTracker == nil {
		panic("routing.NewRouter: healthTracker must not be nil")
	}
	if st == nil {
		panic("routing.NewRouter: store must not be nil")
	}
	if publisher == nil {
		panic("routing.NewRouter: publisher must not be nil")
	}
	if denyList == nil {
		panic("routing.NewRouter: denyList must not be nil")
	}
	if logger == nil {
		panic("routing.NewRouter: logger must not be nil")
	}
	return &Router{
		registry:      registry,
		healthTracker: healthTracker,
		store:         st,
		forwarder:     forwarder,
		publisher:     publisher,
		retryConsumer: retryConsumer,
		denyList:      denyList,
		dispatchedSet: NewDenyList(dispatchedSetMaxSize),
		config:        cfg,
		logger:        logger,
		agentName:     agentName,
		topicName:     topicName,
	}
}

// publishCE is the private wrapper that publishes a CE and logs on failure.
func (r *Router) publishCE(ctx context.Context, ceType string, data any) {
	if err := cloudevent.PublishCE(ctx, r.publisher.Publish, cloudevent.SubjectResponses, r.agentName, ceType, data); err != nil {
		r.logger.Warn("failed to publish CE", "type", ceType, "error", err)
	}
}

// HandleRequest routes a creation or deletion CE to the appropriate SP.
func (r *Router) HandleRequest(ctx context.Context, msg []byte) error {
	isCreate, payload, drop := r.parseRequestCE(msg)
	if drop {
		return nil
	}

	if payload.ResourceID == "" || payload.ServiceType == "" {
		r.logger.Warn("CE missing required fields", "resourceId", payload.ResourceID, "serviceType", payload.ServiceType)
		r.publishCE(ctx, cloudevent.TypeError, ErrorData{
			AgentName: r.agentName, TopicName: r.topicName,
			Error: ErrorInvalidPayload, Details: "resourceId and serviceType are required",
		})
		return nil
	}

	if isCreate && r.denyList.Consume(payload.ResourceID) {
		return nil
	}

	sp, status, ok := r.resolveProvider(ctx, payload.ServiceType)
	if !ok {
		r.publishCE(ctx, cloudevent.TypeError, ErrorData{
			ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName,
			Error: ErrorUnsupportedServiceType, Details: "provider not found for service type: " + payload.ServiceType,
		})
		return nil
	}

	if status == v1alpha1.Unavailable {
		r.publishCE(ctx, cloudevent.TypeError, ErrorData{
			ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName,
			Error: ErrorSPUnavailable, Details: "provider unavailable for service type: " + payload.ServiceType,
		})
		return nil
	}
	if status == v1alpha1.Unhealthy {
		if err := r.publisher.Publish(ctx, r.topicName+".retry", msg); err != nil {
			return err
		}
		r.publishCE(ctx, cloudevent.TypeRequestQueued, RequestQueuedData{
			ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName,
			ServiceType: payload.ServiceType, Status: "QUEUED",
		})
		return nil
	}

	if r.forwarder == nil {
		r.publishCE(ctx, cloudevent.TypeError, ErrorData{
			ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName,
			Error: ErrorSPUnavailable, Details: "provider unavailable for service type: " + payload.ServiceType,
		})
		return nil
	}

	return r.forwardWithRetry(ctx, sp, isCreate, payload, msg)
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
	case "dcm.request.create":
		isCreate = true
	case "dcm.request.delete":
		isCreate = false
	default:
		r.logger.Warn("dropping unknown CE type", "type", event.Type())
		return false, payload, true
	}

	if jsonErr := json.Unmarshal(event.Data(), &payload); jsonErr != nil {
		r.logger.Warn("dropping CE with unparseable data", "error", jsonErr)
		return false, payload, true
	}
	return isCreate, payload, false
}

// resolveProvider looks up the provider for a service type and returns its store
// record and health status. Returns ok=false if no provider is found.
func (r *Router) resolveProvider(ctx context.Context, serviceType string) (*store.StoredProvider, v1alpha1.ProviderStatus, bool) {
	providerName, found := r.registry.Lookup(serviceType)
	if !found {
		return nil, "", false
	}

	sp, storeErr := r.store.GetByName(ctx, providerName)
	if storeErr != nil || sp == nil {
		return nil, "", false
	}

	if sp.ID == "" {
		r.logger.Error("provider has empty ID (data corruption)", "name", providerName, "serviceType", serviceType)
		return sp, v1alpha1.Unavailable, true
	}

	state, found := r.healthTracker.GetState(sp.ID)
	if !found {
		return sp, v1alpha1.Unavailable, true
	}
	return sp, state.Status, true
}

// forwardWithRetry dispatches a request to the SP with retry logic and
// dispatched-set tracking.
func (r *Router) forwardWithRetry(ctx context.Context, sp *store.StoredProvider, isCreate bool, payload inboundPayload, msg []byte) error {
	_ = msg // retained for future use (redelivery data)

	newlyAdded := r.dispatchedSet.AddIfAbsent(payload.ResourceID)

	// NOTE: We intentionally do NOT short-circuit when newlyAdded==false.
	// dispatchedSet is a cancel-rejection ledger that persists across the
	// resource lifecycle (create → delete). A delete-after-create is a
	// legitimate operation that shares the same resourceId. Message-level
	// dedup against JetStream redelivery is the SP's responsibility
	// (idempotent create/delete). See also: ack-error logging in handlers.go.

	// TOCTOU mitigation: re-check denyList after claiming the dispatchedSet
	// slot. A concurrent HandleCancel may have added to denyList between
	// the first check in HandleRequest and here (across resolveProvider I/O).
	if isCreate && r.denyList.Consume(payload.ResourceID) {
		if newlyAdded {
			r.dispatchedSet.Remove(payload.ResourceID)
		}
		return nil
	}

	success := false
	defer func() {
		if !success && newlyAdded {
			r.dispatchedSet.Remove(payload.ResourceID)
		}
	}()

	embedded := sp.Type == "embedded"
	// Spec allows 0; treat as "one attempt, no retries"
	maxAttempts := r.config.RetryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var fwdErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if isCreate {
			fwdErr = r.forwarder.CreateResource(ctx, sp.Endpoint, embedded, CreateResourceRequest(payload))
		} else {
			fwdErr = r.forwarder.DeleteResource(ctx, sp.Endpoint, embedded, payload.ResourceID)
		}
		if fwdErr == nil {
			break
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if !IsRetryable(fwdErr) {
			r.logger.Warn("SP error", "error", fwdErr, "resourceId", payload.ResourceID, "serviceType", payload.ServiceType)
			r.publishCE(ctx, cloudevent.TypeError, ErrorData{
				ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName,
				Error: ErrorNonRetryable, Details: "service provider returned non-retryable error for service type: " + payload.ServiceType,
			})
			return nil
		}
		if attempt < maxAttempts-1 {
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
	if fwdErr != nil {
		r.logger.Warn("SP error", "error", fwdErr, "resourceId", payload.ResourceID, "serviceType", payload.ServiceType)
		r.publishCE(ctx, cloudevent.TypeError, ErrorData{
			ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName,
			Error: ErrorRetryExhausted, Details: "service provider error after retry exhaustion for service type: " + payload.ServiceType,
		})
		return nil
	}

	success = true
	if isCreate {
		r.publishCE(ctx, cloudevent.TypeCreationAcked, CreationAckData{
			ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName, Status: "PROVISIONING",
		})
	} else {
		r.publishCE(ctx, cloudevent.TypeDeletionAcked, DeletionAckData{
			ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName, Status: "DELETING",
		})
	}
	return nil
}

// HandleCancel processes a cancel CE for a given resourceId.
func (r *Router) HandleCancel(ctx context.Context, msg []byte) error {
	var event cloudevents.Event
	if err := json.Unmarshal(msg, &event); err != nil {
		r.logger.Warn("dropping malformed cancel CE", "error", err)
		return nil
	}

	var payload inboundPayload
	if err := json.Unmarshal(event.Data(), &payload); err != nil {
		r.logger.Warn("dropping cancel CE with unparseable data", "error", err)
		return nil
	}

	if payload.ResourceID == "" || payload.ServiceType == "" {
		r.logger.Warn("cancel CE missing required fields", "resourceId", payload.ResourceID, "serviceType", payload.ServiceType)
		r.publishCE(ctx, cloudevent.TypeError, ErrorData{
			AgentName: r.agentName, TopicName: r.topicName,
			Error: ErrorInvalidPayload, Details: "resourceId and serviceType are required",
		})
		return nil
	}

	if r.dispatchedSet.Contains(payload.ResourceID) {
		r.publishCE(ctx, cloudevent.TypeCancelRejected, CancelRejectedData{
			ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName,
			Reason: "resource already dispatched",
		})
		return nil
	}

	if r.retryConsumer == nil {
		r.denyList.Add(payload.ResourceID)
		r.publishCE(ctx, cloudevent.TypeCancelAcked, CancelAckData{
			ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName,
			ServiceType: payload.ServiceType,
		})
		return nil
	}

	messages, err := r.retryConsumer.FetchRetryMessages(ctx)
	if err != nil {
		return err
	}

	found := false
	for _, m := range messages {
		if m.ResourceID == payload.ResourceID {
			if err := m.AckFunc(); err != nil {
				r.denyList.Add(payload.ResourceID)
				return fmt.Errorf("failed to ack cancelled message: %w", err)
			}
			found = true
			continue
		}
		if err := r.retryConsumer.RepublishToRetry(ctx, m.Data); err != nil {
			if found {
				r.denyList.Add(payload.ResourceID)
			}
			return fmt.Errorf("failed to republish non-matching message: %w", err)
		}
		if err := m.AckFunc(); err != nil {
			if found {
				r.denyList.Add(payload.ResourceID)
			}
			return fmt.Errorf("failed to ack republished message: %w", err)
		}
	}

	r.denyList.Add(payload.ResourceID)
	r.publishCE(ctx, cloudevent.TypeCancelAcked, CancelAckData{
		ResourceID: payload.ResourceID, AgentName: r.agentName, TopicName: r.topicName,
		ServiceType: payload.ServiceType,
	})
	return nil
}
