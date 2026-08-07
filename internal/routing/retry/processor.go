package retry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/nats-io/nats.go/jetstream"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/messaging"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"
	"github.com/dcm-project/environment-agent/internal/routing"
)

const (
	fetchBatchSize       = 100
	fetchMaxWait         = 200 * time.Millisecond
	storeErrorRetryDelay = 10 * time.Second
)

// ProcessorConfig holds retry processor tuning knobs.
//
// MaxDeliver-exceeded handling lives solely in messaging.Client
// (handleMainMessage) now — the main-topic consume loop that used to
// duplicate that logic here (Processor.Start/handleMessage) was dead code
// (never wired from main.go) and has been removed.
type ProcessorConfig struct {
	HandlerTimeout time.Duration
	NakDelay       time.Duration // zero falls back to routing.DefaultNakDelay
}

// JSProvider returns the current JetStream context. Resolved at call time so
// the retry processor works even when NATS connects after construction.
type JSProvider func() jetstream.JetStream

// ProcessorDeps holds all dependencies for the retry processor.
type ProcessorDeps struct {
	Registry            *provider.Registry
	HealthTracker       *provider.InMemoryHealthTracker
	Store               store.Store
	Forwarder           routing.SPForwarder
	Publisher           routing.Publisher
	JSProvider          JSProvider
	DenyList            *routing.ResourceSet
	ClaimedResourcesSet *routing.ResourceSet // nil-safe; shared with Router for REQ-RTE-180
	Config              ProcessorConfig
	Logger              *slog.Logger
	AgentName           string
	// Topics carries the derived subjects/consumer names shared with
	// messaging.Client, so both packages agree on where requests/cancels
	// live (CP-owned messaging.RequestStreamName) and where retries live
	// (agent-owned RetryStream). See messaging.DeriveTopicNames.
	Topics messaging.TopicNames
}

// Processor handles retry-topic consumption triggered by SP health transitions and restarts.
type Processor struct {
	deps    ProcessorDeps
	retryMu sync.Mutex // protects ONLY JetStream Fetch calls

	mu      sync.Mutex // lifecycle: stopped, wg.Add serialization
	stopped bool
	wg      sync.WaitGroup
}

// NewProcessor creates a retry processor. Panics on nil required deps.
func NewProcessor(deps ProcessorDeps) *Processor {
	if deps.Registry == nil {
		panic("retry.NewProcessor: Registry must not be nil")
	}
	if deps.HealthTracker == nil {
		panic("retry.NewProcessor: HealthTracker must not be nil")
	}
	if deps.Store == nil {
		panic("retry.NewProcessor: Store must not be nil")
	}
	if deps.Publisher == nil {
		panic("retry.NewProcessor: Publisher must not be nil")
	}
	if deps.DenyList == nil {
		panic("retry.NewProcessor: DenyList must not be nil")
	}
	if deps.Logger == nil {
		panic("retry.NewProcessor: Logger must not be nil")
	}
	return &Processor{deps: deps}
}

// Stop marks the processor stopped and waits for in-flight transition
// goroutines (spawned by RunTransition) to finish. Idempotent.
func (p *Processor) Stop() {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()

	p.wg.Wait()
}

// RunTransition is the goroutine-safe entry point called from health monitor
// callbacks. It spawns ProcessOnTransition in a new goroutine with panic recovery.
func (p *Processor) RunTransition(ctx context.Context, providerID string, from, to v1alpha1.ProviderStatus) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.wg.Add(1)
	p.mu.Unlock()

	go func() {
		defer p.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				p.deps.Logger.Error("panic in transition processor", "panic", r, "providerID", providerID)
			}
		}()
		if err := p.ProcessOnTransition(ctx, providerID, from, to); err != nil {
			p.deps.Logger.Error("transition processing failed", "error", err, "providerID", providerID, "to", to)
		}
	}()
}

// ProcessOnTransition handles retry-topic processing triggered by an SP health transition.
// Only processes retry messages for Ready (forward) and Unavailable (reject) transitions.
// Unhealthy transitions leave retry messages untouched for future recovery.
func (p *Processor) ProcessOnTransition(ctx context.Context, providerID string, _, to v1alpha1.ProviderStatus) error {
	if to != v1alpha1.Ready && to != v1alpha1.Unavailable {
		return nil
	}

	sp, err := p.deps.Store.GetByID(ctx, providerID)
	if err != nil {
		return fmt.Errorf("ProcessOnTransition: store lookup failed for provider %s: %w", providerID, err)
	}
	if sp == nil {
		p.deps.Logger.Warn("ProcessOnTransition: provider not found in store", "providerID", providerID)
		return nil
	}

	p.retryMu.Lock()
	msgs, err := p.fetchAllFromConsumer(ctx, p.deps.Topics.RetryStream(), p.deps.Topics.RetryConsumer())
	p.retryMu.Unlock()
	if err != nil {
		return err
	}

	items := p.parseMessages(msgs)
	items = p.dedupCreateDeletePairs(ctx, items)
	p.processTransitionItems(ctx, sp, to, items)
	return nil
}

// processTransitionItems dispatches surviving (non-deduped) retry items after a
// provider health transition.
func (p *Processor) processTransitionItems(ctx context.Context, sp *store.StoredProvider, to v1alpha1.ProviderStatus, items []parsedMessage) {
	heartbeatAll(items)
	for _, item := range items {
		isCreate := item.ceType == cloudevent.TypeRequestCreate
		if isCreate && p.deps.DenyList.Contains(item.resourceID) {
			_ = item.msg.Ack()
			continue
		}
		if item.serviceType != sp.ServiceType {
			_ = item.msg.InProgress()
			p.routeMessage(ctx, item.msg, item.ceResult)
			continue
		}
		if to == v1alpha1.Unavailable {
			p.publishCE(ctx, cloudevent.TypeError, routing.ErrorData{
				ResponseContext: p.responseCtx(item.resourceID),
				Error:           routing.ErrorSPUnavailable, Details: "provider unavailable for service type: " + item.serviceType,
			})
			_ = item.msg.Ack()
			continue
		}
		_, currentStatus, ok, storeErr := p.resolveProviderForServiceType(ctx, item.serviceType)
		if storeErr != nil {
			p.deps.Logger.Warn("transient store error during transition processing", "error", storeErr, "resourceId", item.resourceID)
			_ = item.msg.NakWithDelay(storeErrorRetryDelay)
			continue
		}
		if !ok || currentStatus == v1alpha1.Unavailable {
			p.publishCE(ctx, cloudevent.TypeError, routing.ErrorData{
				ResponseContext: p.responseCtx(item.resourceID),
				Error:           routing.ErrorSPUnavailable, Details: "provider unavailable for service type: " + item.serviceType,
			})
			_ = item.msg.Ack()
			continue
		}
		_ = item.msg.InProgress()
		if !p.forwardRequest(ctx, sp, item.ceResult) {
			if err := p.deps.Publisher.Publish(ctx, p.deps.Topics.Retry, item.msg.Data()); err == nil {
				_ = item.msg.Ack()
			}
			continue
		}
		_ = item.msg.Ack()
	}
}

// ProcessOnRestart drains cancel topic → main topic → retry topic on agent startup.
//
// Phases 1 (cancel drain) and 2 (main drain) are no-ops in production when
// messaging.Client is active, because the Client's own cancel drain and main
// consume have already claimed those messages. Phase 3 (retry drain) is the
// primary useful path on restart — it re-processes queued retries.
func (p *Processor) ProcessOnRestart(ctx context.Context) error {
	if err := p.drainCancelsToDenyList(ctx); err != nil {
		return err
	}
	if err := p.drainMainTopic(ctx); err != nil {
		return err
	}
	return p.drainRetryTopicWithDedup(ctx)
}

func (p *Processor) drainCancelsToDenyList(ctx context.Context) error {
	cancelMsgs, err := p.fetchAllFromConsumer(ctx, messaging.RequestStreamName, p.deps.Topics.CancelConsumer())
	if err != nil {
		return err
	}
	for _, m := range cancelMsgs {
		res, ok := p.parseCE(m.Data())
		if !ok {
			_ = m.Term()
			continue
		}
		p.deps.DenyList.Add(res.resourceID)
		_ = m.Ack()
	}
	return nil
}

func (p *Processor) drainMainTopic(ctx context.Context) error {
	mainMsgs, err := p.fetchAllFromConsumer(ctx, messaging.RequestStreamName, p.deps.Topics.MainConsumer())
	if err != nil {
		return err
	}
	heartbeatAllMsgs(mainMsgs)
	for _, m := range mainMsgs {
		res, ok := p.parseCE(m.Data())
		if !ok {
			_ = m.Term()
			continue
		}
		if res.ceType == cloudevent.TypeRequestCreate && p.deps.DenyList.Contains(res.resourceID) {
			_ = m.Ack()
			continue
		}
		_ = m.InProgress()
		p.routeMessage(ctx, m, res)
	}
	return nil
}

// drainRetryTopicWithDedup drains the retry topic with create+delete dedup (REQ-RCM-090).
func (p *Processor) drainRetryTopicWithDedup(ctx context.Context) error {
	p.retryMu.Lock()
	retryMsgs, err := p.fetchAllFromConsumer(ctx, p.deps.Topics.RetryStream(), p.deps.Topics.RetryConsumer())
	p.retryMu.Unlock()
	if err != nil {
		return err
	}

	items := p.parseMessages(retryMsgs)
	items = p.dedupCreateDeletePairs(ctx, items)
	heartbeatAll(items)
	for _, item := range items {
		if item.ceType == cloudevent.TypeRequestCreate && p.deps.DenyList.Contains(item.resourceID) {
			_ = item.msg.Ack()
			continue
		}
		_ = item.msg.InProgress()
		p.routeMessage(ctx, item.msg, item.ceResult)
	}
	return nil
}

// publishAckCE emits the appropriate creation-acked or deletion-acked CE.
func (p *Processor) publishAckCE(ctx context.Context, res ceResult) {
	if res.ceType == cloudevent.TypeRequestDelete {
		p.publishCE(ctx, cloudevent.TypeDeletionAcked, routing.DeletionAckData{
			ResponseContext: p.responseCtx(res.resourceID), Status: "DELETING",
		})
	} else {
		p.publishCE(ctx, cloudevent.TypeCreationAcked, routing.CreationAckData{
			ResponseContext: p.responseCtx(res.resourceID), Status: "PROVISIONING",
		})
	}
}

const fetchTimeout = 5 * time.Second

// FetchRetryMessages implements routing.RetryTopicConsumer. Uses an internal 5s
// timeout to bound JetStream Fetch in cancel-path contexts.
func (p *Processor) FetchRetryMessages(ctx context.Context) ([]routing.RetryMessage, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	p.retryMu.Lock()
	msgs, err := p.fetchAllFromConsumer(fetchCtx, p.deps.Topics.RetryStream(), p.deps.Topics.RetryConsumer())
	p.retryMu.Unlock()

	if err != nil {
		return nil, err
	}

	var result []routing.RetryMessage
	for _, m := range msgs {
		res, ok := p.parseCE(m.Data())
		if !ok {
			_ = m.Term()
			continue
		}
		msg := m // capture for closure
		result = append(result, routing.RetryMessage{
			Data:        m.Data(),
			ResourceID:  res.resourceID,
			ServiceType: res.serviceType,
			AckFunc:     func() error { return msg.Ack() },
		})
	}
	return result, nil
}

// RepublishToRetry implements routing.RetryTopicConsumer.
func (p *Processor) RepublishToRetry(ctx context.Context, data []byte) error {
	return p.deps.Publisher.Publish(ctx, p.deps.Topics.Retry, data)
}

type ceResult struct {
	resourceID  string
	serviceType string
	eventID     string
	ceType      string
	spec        json.RawMessage
}

// parsedMessage pairs a parsed CE result with its JetStream message handle.
type parsedMessage struct {
	msg jetstream.Msg
	ceResult
}

// parseMessages unmarshals CEs from raw JetStream messages, terminating any
// that fail to parse and returning the successfully parsed ones.
func (p *Processor) parseMessages(msgs []jetstream.Msg) []parsedMessage {
	var items []parsedMessage
	for _, m := range msgs {
		res, ok := p.parseCE(m.Data())
		if !ok {
			_ = m.Term()
			continue
		}
		items = append(items, parsedMessage{msg: m, ceResult: res})
	}
	return items
}

// dedupCreateDeletePairs identifies resourceIDs that have both create and delete
// messages. It acks all messages in such pairs, publishes a deletion-acked CE,
// and returns only the items that survived deduplication.
//
// Trade-off: Acking deduped messages removes them from the stream. If the ack
// succeeds but the deletion-ack CE publish fails, the resource appears to the
// control plane as still pending. This is an at-least-once trade-off; SPs are
// required to be idempotent (REQ-SP-010).
func (p *Processor) dedupCreateDeletePairs(ctx context.Context, items []parsedMessage) []parsedMessage {
	type dedupEntry struct {
		creates []int
		deletes []int
	}
	dedupMap := make(map[string]*dedupEntry)
	for i, item := range items {
		e, ok := dedupMap[item.resourceID]
		if !ok {
			e = &dedupEntry{}
			dedupMap[item.resourceID] = e
		}
		switch item.ceType {
		case cloudevent.TypeRequestCreate:
			e.creates = append(e.creates, i)
		case cloudevent.TypeRequestDelete:
			e.deletes = append(e.deletes, i)
		}
	}
	deduped := make(map[int]bool)
	for resID, e := range dedupMap {
		if len(e.creates) > 0 && len(e.deletes) > 0 {
			for _, idx := range e.creates {
				_ = items[idx].msg.Ack()
				deduped[idx] = true
			}
			for _, idx := range e.deletes {
				_ = items[idx].msg.Ack()
				deduped[idx] = true
			}
			p.publishCE(ctx, cloudevent.TypeDeletionAcked, routing.DeletionAckData{
				ResponseContext: p.responseCtx(resID), Status: "DELETED",
			})
			p.deps.Logger.Info("dedup cancelled create+delete pair", "resourceId", resID)
		}
	}

	if len(deduped) == 0 {
		return items
	}
	surviving := make([]parsedMessage, 0, len(items)-len(deduped))
	for i, item := range items {
		if !deduped[i] {
			surviving = append(surviving, item)
		}
	}
	return surviving
}

func (p *Processor) parseCE(data []byte) (ceResult, bool) {
	var event cloudevents.Event
	if err := json.Unmarshal(data, &event); err != nil {
		p.deps.Logger.Warn("dropping malformed CE", "error", err)
		return ceResult{}, false
	}
	switch event.Type() {
	case cloudevent.TypeRequestCreate, cloudevent.TypeRequestDelete, cloudevent.TypeRequestCancel:
	default:
		p.deps.Logger.Warn("dropping unknown CE type", "type", event.Type())
		return ceResult{}, false
	}
	var payload struct {
		ResourceID  string          `json:"resource_id"`
		ServiceType string          `json:"service_type"`
		Spec        json.RawMessage `json:"spec,omitempty"`
	}
	if err := json.Unmarshal(event.Data(), &payload); err != nil {
		p.deps.Logger.Warn("dropping CE with unparseable data", "error", err)
		return ceResult{}, false
	}
	return ceResult{
		resourceID:  payload.ResourceID,
		serviceType: payload.ServiceType,
		eventID:     event.ID(),
		ceType:      event.Type(),
		spec:        payload.Spec,
	}, true
}

func (p *Processor) responseCtx(resourceID string) routing.ResponseContext {
	return routing.ResponseContext{ResourceID: resourceID, AgentName: p.deps.AgentName, TopicName: p.deps.Topics.Main}
}

func (p *Processor) publishCE(ctx context.Context, ceType string, data any) {
	if err := cloudevent.PublishCE(ctx, p.deps.Publisher.PublishWithMsgID, cloudevent.SubjectResponses, p.deps.AgentName, ceType, data); err != nil {
		p.deps.Logger.Warn("failed to publish CE", "type", ceType, "error", err)
	}
}

func (p *Processor) resolveProviderForServiceType(ctx context.Context, serviceType string) (*store.StoredProvider, v1alpha1.ProviderStatus, bool, error) {
	return routing.ResolveProvider(ctx, p.deps.Registry, p.deps.Store, p.deps.HealthTracker, p.deps.Logger, serviceType)
}

func (p *Processor) forwardRequest(ctx context.Context, sp *store.StoredProvider, res ceResult) bool {
	if p.deps.Forwarder == nil {
		p.publishCE(ctx, cloudevent.TypeError, routing.ErrorData{
			ResponseContext: p.responseCtx(res.resourceID),
			Error:           routing.ErrorSPUnavailable, Details: "forwarder not configured",
		})
		return true
	}

	var newlyAdded bool
	if p.deps.ClaimedResourcesSet != nil {
		newlyAdded = p.deps.ClaimedResourcesSet.AddIfAbsent(res.resourceID)
	}

	// TOCTOU mitigation: re-check denyList after claiming the claimedResourcesSet
	// slot. A concurrent HandleCancel may have added to denyList between the
	// ProcessOnTransition denyList.Contains check and here. Only creates are
	// deny-listed (REQ-RTE-150/160).
	isCreate := res.ceType == cloudevent.TypeRequestCreate
	if isCreate && p.deps.DenyList.Contains(res.resourceID) {
		if newlyAdded {
			p.deps.ClaimedResourcesSet.Remove(res.resourceID)
		}
		return true
	}

	var fwdCtx context.Context
	var fwdCancel context.CancelFunc
	if p.deps.Config.HandlerTimeout > 0 {
		fwdCtx, fwdCancel = context.WithTimeout(ctx, p.deps.Config.HandlerTimeout)
	} else {
		fwdCtx, fwdCancel = context.WithCancel(ctx)
	}
	defer fwdCancel()

	err := routing.ForwardToSP(fwdCtx, p.deps.Forwarder, sp, routing.ForwardParams{
		ResourceID: res.resourceID, ServiceType: res.serviceType,
		Spec: res.spec, EventID: res.eventID, IsCreate: res.ceType != cloudevent.TypeRequestDelete,
	})
	if err != nil {
		p.deps.Logger.Warn("forward failed during transition processing", "resourceId", res.resourceID, "error", err)
		if newlyAdded {
			p.deps.ClaimedResourcesSet.Remove(res.resourceID)
		}
		return false
	}

	p.publishAckCE(ctx, res)
	return true
}

func (p *Processor) routeMessage(ctx context.Context, msg jetstream.Msg, res ceResult) {
	sp, status, ok, storeErr := p.resolveProviderForServiceType(ctx, res.serviceType)
	if storeErr != nil {
		p.deps.Logger.Warn("transient store error during routing", "error", storeErr, "resourceId", res.resourceID)
		_ = msg.NakWithDelay(storeErrorRetryDelay)
		return
	}
	if !ok {
		if err := p.deps.Publisher.Publish(ctx, p.deps.Topics.Retry, msg.Data()); err == nil {
			_ = msg.Ack()
		}
		return
	}
	switch status {
	case v1alpha1.Ready:
		if !p.forwardRequest(ctx, sp, res) {
			if err := p.deps.Publisher.Publish(ctx, p.deps.Topics.Retry, msg.Data()); err == nil {
				_ = msg.Ack()
			}
			return
		}
		_ = msg.Ack()
	case v1alpha1.Unavailable:
		p.publishCE(ctx, cloudevent.TypeError, routing.ErrorData{
			ResponseContext: p.responseCtx(res.resourceID),
			Error:           routing.ErrorSPUnavailable, Details: "provider unavailable for service type: " + res.serviceType,
		})
		_ = msg.Ack()
	default:
		if err := p.deps.Publisher.Publish(ctx, p.deps.Topics.Retry, msg.Data()); err == nil {
			_ = msg.Ack()
		}
	}
}

// js resolves the current JetStream context from the provider.
func (p *Processor) js() jetstream.JetStream {
	if p.deps.JSProvider == nil {
		return nil
	}
	return p.deps.JSProvider()
}

func (p *Processor) fetchAllFromConsumer(ctx context.Context, streamName, consumerName string) ([]jetstream.Msg, error) {
	js := p.js()
	if js == nil {
		return nil, nil
	}
	cons, err := js.Consumer(ctx, streamName, consumerName)
	if err != nil {
		return nil, err
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return nil, err
	}
	pending := info.NumPending + uint64(info.NumAckPending)
	if pending == 0 {
		return nil, nil
	}

	var collected []jetstream.Msg
	for uint64(len(collected)) < pending {
		if ctx.Err() != nil {
			break
		}
		batch, fetchErr := cons.Fetch(fetchBatchSize, jetstream.FetchMaxWait(fetchMaxWait))
		if fetchErr != nil {
			break
		}
		count := 0
		for msg := range batch.Messages() {
			collected = append(collected, msg)
			count++
		}
		if count == 0 {
			break
		}
	}
	return collected, nil //nolint:nilerr // fetchErr from Fetch timeout is expected, not a failure
}

// heartbeatAll resets AckWait on every message in the batch. Called once before
// the processing loop to give all messages a fresh AckWait window. Individual
// messages are heartbeated again (inline) right before potentially-long work.
func heartbeatAll(items []parsedMessage) {
	for i := range items {
		_ = items[i].msg.InProgress()
	}
}

// heartbeatAllMsgs is like heartbeatAll but for raw JetStream messages.
func heartbeatAllMsgs(msgs []jetstream.Msg) {
	for i := range msgs {
		_ = msgs[i].InProgress()
	}
}
