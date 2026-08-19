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
	fetchBatchSize = 100
	fetchMaxWait   = 200 * time.Millisecond
	// storeErrorRetryDelay is the NakWithDelay backoff used whenever a
	// retry-topic message can't be resolved yet (transient store error,
	// forward failure, provider not found/not ready). See RetryMessage.NakFunc
	// (routing/types.go) for why this Naks in place instead of republishing.
	storeErrorRetryDelay = 10 * time.Second
)

// ProcessorConfig holds retry processor tuning knobs.
type ProcessorConfig struct {
	HandlerTimeout time.Duration
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
	InFlightSet         *routing.KeyLock     // nil-safe; shared with Router, blocks concurrent double-dispatch (REQ-RTE-210)
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
				p.deps.Logger.Error("panic in transition processor", "panic", r, "provider_id", providerID)
			}
		}()
		if err := p.ProcessOnTransition(ctx, providerID, from, to); err != nil {
			p.deps.Logger.Error("transition processing failed", "error", err, "provider_id", providerID, "to", to)
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
	p.deps.Logger.Info("processing retry topic for transition", "provider_id", providerID, "to", to)

	sp, err := p.deps.Store.GetByID(ctx, providerID)
	if err != nil {
		return fmt.Errorf("ProcessOnTransition: store lookup failed for provider %s: %w", providerID, err)
	}
	if sp == nil {
		p.deps.Logger.Warn("ProcessOnTransition: provider not found in store", "provider_id", providerID)
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
	p.processTransitionItems(ctx, sp, items)
	p.deps.Logger.Info("processed retry topic for transition", "provider_id", providerID, "to", to, "items", len(items))
	return nil
}

// processTransitionItems dispatches surviving (non-deduped) retry items after a
// provider health transition.
//
// Re-resolves the CURRENT provider status rather than trusting the stale `to`
// parameter (RunTransition backgrounds this call) — an item is only rejected
// when the SP is *currently* Unavailable (REQ-RCM-040).
func (p *Processor) processTransitionItems(ctx context.Context, sp *store.StoredProvider, items []parsedMessage) {
	heartbeatAll(items)
	for _, item := range items {
		isCreate := item.ceType == cloudevent.TypeRequestCreate
		if isCreate && p.deps.DenyList.Contains(item.resourceID) {
			p.deps.Logger.Info("create request dropped, resource in deny list", "resource_id", item.resourceID)
			_ = item.msg.Ack()
			continue
		}
		if item.serviceType != sp.ServiceType {
			_ = item.msg.InProgress()
			p.routeMessage(ctx, item.msg, item.ceResult, true)
			continue
		}
		_, currentStatus, ok, storeErr := p.resolveProviderForServiceType(ctx, item.serviceType)
		if storeErr != nil {
			p.deps.Logger.Warn("transient store error during transition processing", "error", storeErr,
				"resource_id", item.resourceID, "ce_id", item.eventID, "service_type", item.serviceType)
			_ = item.msg.NakWithDelay(storeErrorRetryDelay)
			continue
		}
		if !ok || currentStatus == v1alpha1.Unavailable {
			p.publishCE(ctx, cloudevent.TypeError, item.resourceID, item.eventID, routing.ErrorData{
				ResponseContext: p.responseCtx(item.resourceID),
				Error:           routing.ErrorSPUnavailable, Details: "provider unavailable for service type: " + item.serviceType,
			})
			_ = item.msg.Ack()
			continue
		}
		_ = item.msg.InProgress()
		if !p.forwardRequest(ctx, sp, item.ceResult) {
			// item.msg already lives on the retry topic: Nak it in place
			// (not ack+republish) so JetStream's delivery count increments
			// instead of resetting to 1 on every failed attempt.
			_ = item.msg.NakWithDelay(storeErrorRetryDelay)
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
	p.deps.Logger.Info("drained cancel topic on restart", "messages", len(cancelMsgs))
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
			p.deps.Logger.Info("create request dropped, resource in deny list", "resource_id", res.resourceID)
			_ = m.Ack()
			continue
		}
		_ = m.InProgress()
		p.routeMessage(ctx, m, res, false)
	}
	p.deps.Logger.Info("drained main topic on restart", "messages", len(mainMsgs))
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
			p.deps.Logger.Info("create request dropped, resource in deny list", "resource_id", item.resourceID)
			_ = item.msg.Ack()
			continue
		}
		_ = item.msg.InProgress()
		p.routeMessage(ctx, item.msg, item.ceResult, true)
	}
	p.deps.Logger.Info("drained retry topic on restart", "fetched", len(retryMsgs), "processed", len(items))
	return nil
}

func (p *Processor) publishAckCE(ctx context.Context, res ceResult) {
	if res.ceType == cloudevent.TypeRequestDelete {
		p.publishCE(ctx, cloudevent.TypeDeletionAcked, res.resourceID, res.eventID, routing.DeletionAckData{
			ResponseContext: p.responseCtx(res.resourceID), Status: "DELETING",
		})
	} else {
		p.publishCE(ctx, cloudevent.TypeCreationAcked, res.resourceID, res.eventID, routing.CreationAckData{
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
		msg := m
		result = append(result, routing.RetryMessage{
			Data:        m.Data(),
			ResourceID:  res.resourceID,
			ServiceType: res.serviceType,
			AckFunc:     func() error { return msg.Ack() },
			NakFunc:     func() error { return msg.NakWithDelay(storeErrorRetryDelay) },
		})
	}
	return result, nil
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
// that fail to parse, and returning the successfully parsed, still-live ones.
// The retry-subject consumer has no MaxDeliver limit (DD-410), so there is no
// delivery-count guard here — unlike messaging.Client.handleMainMessage's
// main-topic path.
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
// required to be idempotent (REQ-RCM-210/220).
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
			p.publishCE(ctx, cloudevent.TypeDeletionAcked, resID, "", routing.DeletionAckData{
				ResponseContext: p.responseCtx(resID), Status: "DELETED",
			})
			p.deps.Logger.Info("dedup cancelled create+delete pair", "resource_id", resID)
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
		p.deps.Logger.Warn("dropping unknown CE type", "ce_type", event.Type(), "ce_id", event.ID())
		return ceResult{}, false
	}
	var payload struct {
		ResourceID  string          `json:"resource_id"`
		ServiceType string          `json:"service_type"`
		Spec        json.RawMessage `json:"spec,omitempty"`
	}
	if err := json.Unmarshal(event.Data(), &payload); err != nil {
		p.deps.Logger.Warn("dropping CE with unparseable data", "error", err, "ce_id", event.ID(), "ce_type", event.Type())
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

// publishCE publishes an outcome CE. ceID is the inbound CE id that
// triggered this publish (empty when no single inbound CE applies), included
// in the logs so redeliveries can be correlated back to their trigger.
func (p *Processor) publishCE(ctx context.Context, ceType, resourceID, ceID string, data any) {
	if err := cloudevent.PublishCE(ctx, p.deps.Publisher.PublishWithMsgID, cloudevent.SubjectResponses, p.deps.AgentName, ceType, data); err != nil {
		p.deps.Logger.Warn("failed to publish CE", "ce_type", ceType, "resource_id", resourceID, "ce_id", ceID, "error", err)
		return
	}
	p.deps.Logger.Info("published CE", "ce_type", ceType, "resource_id", resourceID, "ce_id", ceID)
}

func (p *Processor) resolveProviderForServiceType(ctx context.Context, serviceType string) (*store.StoredProvider, v1alpha1.ProviderStatus, bool, error) {
	return routing.ResolveProvider(ctx, p.deps.Registry, p.deps.Store, p.deps.HealthTracker, p.deps.Logger, serviceType)
}

func (p *Processor) forwardRequest(ctx context.Context, sp *store.StoredProvider, res ceResult) bool {
	if p.deps.Forwarder == nil {
		p.publishCE(ctx, cloudevent.TypeError, res.resourceID, res.eventID, routing.ErrorData{
			ResponseContext: p.responseCtx(res.resourceID),
			Error:           routing.ErrorSPUnavailable, Details: "forwarder not configured",
		})
		return true
	}

	// Transient double-dispatch guard (REQ-RTE-210): shared with Router so a
	// main-topic forward and a retry-topic forward for the same resourceId
	// can't race each other into calling the SP twice. Released
	// unconditionally below, so it never blocks a later legitimate attempt.
	if p.deps.InFlightSet != nil {
		if !p.deps.InFlightSet.AddIfAbsent(res.resourceID) {
			p.deps.Logger.Info("forward already in flight for resource, deferring", "resource_id", res.resourceID, "ce_id", res.eventID)
			return false
		}
		defer p.deps.InFlightSet.Remove(res.resourceID)
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
		p.deps.Logger.Info("create request dropped, resource in deny list", "resource_id", res.resourceID)
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
		p.deps.Logger.Warn("forward failed during transition processing", append([]any{
			"resource_id", res.resourceID, "ce_id", res.eventID, "provider_id", sp.ID, "service_type", res.serviceType,
		}, routing.SafeErrorAttrs(err)...)...)
		if newlyAdded {
			p.deps.ClaimedResourcesSet.Remove(res.resourceID)
		}
		return false
	}

	p.publishAckCE(ctx, res)
	return true
}

// routeMessage resolves a provider for res and either forwards, rejects, or
// re-queues msg for a later attempt. fromRetryTopic must be true when msg was
// fetched from the retry-topic consumer (RunTransition, drainRetryTopicWithDedup)
// and false when it's still on the main topic (drainMainTopic) — see
// requeueToRetryTopic for why the distinction matters.
func (p *Processor) routeMessage(ctx context.Context, msg jetstream.Msg, res ceResult, fromRetryTopic bool) {
	sp, status, ok, storeErr := p.resolveProviderForServiceType(ctx, res.serviceType)
	if storeErr != nil {
		p.deps.Logger.Warn("transient store error during routing", "error", storeErr,
			"resource_id", res.resourceID, "ce_id", res.eventID, "service_type", res.serviceType)
		_ = msg.NakWithDelay(storeErrorRetryDelay)
		return
	}
	if !ok {
		p.requeueToRetryTopic(ctx, msg, fromRetryTopic)
		return
	}
	switch status {
	case v1alpha1.Ready:
		if !p.forwardRequest(ctx, sp, res) {
			p.requeueToRetryTopic(ctx, msg, fromRetryTopic)
			return
		}
		_ = msg.Ack()
	case v1alpha1.Unavailable:
		p.publishCE(ctx, cloudevent.TypeError, res.resourceID, res.eventID, routing.ErrorData{
			ResponseContext: p.responseCtx(res.resourceID),
			Error:           routing.ErrorSPUnavailable, Details: "provider unavailable for service type: " + res.serviceType,
		})
		_ = msg.Ack()
	default:
		p.requeueToRetryTopic(ctx, msg, fromRetryTopic)
	}
}

// requeueToRetryTopic re-queues a message that isn't ready to forward yet.
// fromRetryTopic Naks in place (see RetryMessage.NakFunc); otherwise the
// message is still on the main topic and must move streams for the first
// time: publish a fresh copy to the retry topic, then ack the original.
func (p *Processor) requeueToRetryTopic(ctx context.Context, msg jetstream.Msg, fromRetryTopic bool) {
	if fromRetryTopic {
		_ = msg.NakWithDelay(storeErrorRetryDelay)
		return
	}
	if err := p.deps.Publisher.Publish(ctx, p.deps.Topics.Retry, msg.Data()); err == nil {
		_ = msg.Ack()
	}
}

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
			// A request-level failure (e.g. subscribe/pull-request send
			// failed) — never returned for the expected "no more messages"
			// case, which surfaces via batch.Error() below instead.
			return collected, fetchErr
		}
		count := 0
		for msg := range batch.Messages() {
			collected = append(collected, msg)
			count++
		}
		// batch.Error() is nil for the expected FetchMaxWait timeout /
		// no-messages cases (nats.go filters those internally); a non-nil
		// value here is a genuine mid-fetch failure that must not be masked
		// as "done fetching".
		if batchErr := batch.Error(); batchErr != nil {
			return collected, batchErr
		}
		if count == 0 {
			break
		}
	}
	return collected, nil //nolint:nilerr // ctx.Err() above breaks the loop early on caller cancellation/deadline; that's not a fetch failure, so partial results are returned with a nil error
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
