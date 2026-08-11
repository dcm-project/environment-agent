// Package routingtest provides shared test fakes, CE builders, and assertion
// helpers for routing and retry integration tests.
package routingtest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/gomega" //nolint:staticcheck // Gomega DSL

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// CEWaitTimeout is the default wait for CE assertions.
const CEWaitTimeout = 5 * time.Second

// --- CE builders ---

// BuildCreateCE constructs a creation request CloudEvent with a random CE ID.
func BuildCreateCE(resourceID, serviceType string) []byte {
	return BuildCreateCEWithID(uuid.New().String(), resourceID, serviceType)
}

// BuildCreateCEWithID constructs a creation request CloudEvent with a specific CE ID.
func BuildCreateCEWithID(ceID, resourceID, serviceType string) []byte {
	event := cloudevents.NewEvent()
	event.SetID(ceID)
	event.SetSource(cloudevent.SourceControlPlane)
	event.SetType(cloudevent.TypeRequestCreate)
	event.SetTime(time.Now())
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]any{
		"resource_id":  resourceID,
		"service_type": serviceType,
		"spec":         map[string]any{"size": "small"},
	})
	data, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	return data
}

// BuildDeleteCE constructs a deletion request CloudEvent.
func BuildDeleteCE(resourceID, serviceType string) []byte {
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource(cloudevent.SourceControlPlane)
	event.SetType(cloudevent.TypeRequestDelete)
	event.SetTime(time.Now())
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]any{
		"resource_id":  resourceID,
		"service_type": serviceType,
	})
	data, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	return data
}

// BuildCancelCE constructs a cancel request CloudEvent.
func BuildCancelCE(resourceID, serviceType string) []byte {
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource(cloudevent.SourceControlPlane)
	event.SetType(cloudevent.TypeRequestCancel)
	event.SetTime(time.Now())
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]any{
		"resource_id":  resourceID,
		"service_type": serviceType,
	})
	data, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	return data
}

// --- Fakes ---

// CreateCall records a single CreateResource invocation.
type CreateCall struct {
	Endpoint string
	Embedded bool
	Req      routing.CreateResourceRequest
}

// DeleteCall records a single DeleteResource invocation.
type DeleteCall struct {
	Endpoint   string
	Embedded   bool
	ResourceID string
}

// FakeSPForwarder records calls to CreateResource/DeleteResource.
type FakeSPForwarder struct {
	mu          sync.Mutex
	createCalls []CreateCall
	deleteCalls []DeleteCall
	CreateErr   error
	DeleteErr   error
	FailFirst   int // when > 0, return CreateErr only for first N calls; 0 means always
}

func (f *FakeSPForwarder) CreateResource(_ context.Context, endpoint string, embedded bool, req routing.CreateResourceRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls = append(f.createCalls, CreateCall{Endpoint: endpoint, Embedded: embedded, Req: req})
	if f.CreateErr != nil {
		if f.FailFirst == 0 || len(f.createCalls) <= f.FailFirst {
			return f.CreateErr
		}
	}
	return nil
}

func (f *FakeSPForwarder) DeleteResource(_ context.Context, endpoint string, embedded bool, req routing.DeleteResourceRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, DeleteCall{Endpoint: endpoint, Embedded: embedded, ResourceID: req.ResourceID})
	return f.DeleteErr
}

func (f *FakeSPForwarder) CreateCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.createCalls)
}

func (f *FakeSPForwarder) DeleteCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deleteCalls)
}

// SetCreateErr updates the CreateErr field thread-safely.
func (f *FakeSPForwarder) SetCreateErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.CreateErr = err
}

// GetCreateCalls returns a snapshot of recorded create calls.
func (f *FakeSPForwarder) GetCreateCalls() []CreateCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]CreateCall, len(f.createCalls))
	copy(cp, f.createCalls)
	return cp
}

// NATSPublisher wraps a NATS JetStream connection as a routing.Publisher.
type NATSPublisher struct {
	JS jetstream.JetStream
}

func (p *NATSPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := p.JS.Publish(ctx, subject, data)
	return err
}

func (p *NATSPublisher) PublishWithMsgID(ctx context.Context, subject, msgID string, data []byte) error {
	_, err := p.JS.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
	return err
}

// FakeStore implements store.Store for test purposes.
type FakeStore struct {
	Providers    map[string]*store.StoredProvider
	GetByNameErr error // when non-nil, GetByName returns this error
}

func NewFakeStore() *FakeStore {
	return &FakeStore{Providers: make(map[string]*store.StoredProvider)}
}

func (s *FakeStore) Save(_ context.Context, p store.StoredProvider) error {
	s.Providers[p.Name] = &p
	return nil
}

func (s *FakeStore) Delete(_ context.Context, name string) error {
	delete(s.Providers, name)
	return nil
}

func (s *FakeStore) List(_ context.Context) ([]store.StoredProvider, error) {
	result := make([]store.StoredProvider, 0, len(s.Providers))
	for _, p := range s.Providers {
		result = append(result, *p)
	}
	return result, nil
}

func (s *FakeStore) GetByID(_ context.Context, id string) (*store.StoredProvider, error) {
	for _, p := range s.Providers {
		if p.ID == id {
			return p, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (s *FakeStore) GetByName(_ context.Context, name string) (*store.StoredProvider, error) {
	if s.GetByNameErr != nil {
		return nil, s.GetByNameErr
	}
	p, ok := s.Providers[name]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return p, nil
}

// FakeRetryConsumer records retry topic operations.
type FakeRetryConsumer struct {
	mu       sync.Mutex
	Messages []routing.RetryMessage
	FetchErr error
}

func (f *FakeRetryConsumer) FetchRetryMessages(_ context.Context) ([]routing.RetryMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Messages, f.FetchErr
}

// --- Assertion helpers ---

// ExpectResponseCE reads one CE from the response subscription and returns it.
func ExpectResponseCE(sub *nats.Subscription) cloudevents.Event {
	msg, err := sub.NextMsg(CEWaitTimeout)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "expected a response CE on dcm.agents.responses")
	var event cloudevents.Event
	ExpectWithOffset(1, json.Unmarshal(msg.Data, &event)).To(Succeed())
	return event
}

// ExpectNoResponseCE asserts no CE is received within the given timeout.
func ExpectNoResponseCE(sub *nats.Subscription, timeout time.Duration) {
	msg, err := sub.NextMsg(timeout)
	ExpectWithOffset(1, err).To(MatchError(nats.ErrTimeout), "expected no response CE but got one")
	ExpectWithOffset(1, msg).To(BeNil())
}

// --- Provider setup ---

// RegisterSP registers an external SP in the registry, store, and health tracker.
func RegisterSP(ctx context.Context, reg *provider.Registry, ht *provider.InMemoryHealthTracker, st *FakeStore, name, serviceType string, status v1alpha1.ProviderStatus) string {
	providerID := uuid.New().String()
	ExpectWithOffset(1, reg.Claim(name, serviceType)).To(Succeed())
	ExpectWithOffset(1, st.Save(ctx, store.StoredProvider{
		ID:            providerID,
		Name:          name,
		Endpoint:      "http://mock:8080",
		ServiceType:   serviceType,
		SchemaVersion: "v1alpha1",
		Type:          "external",
		CreateTime:    time.Now(),
		UpdateTime:    time.Now(),
	})).To(Succeed())
	ht.SetState(providerID, status, time.Now())
	return providerID
}

// RegisterEmbeddedSP registers an embedded SP in the registry, store, and health tracker.
func RegisterEmbeddedSP(ctx context.Context, reg *provider.Registry, ht *provider.InMemoryHealthTracker, st *FakeStore, name, serviceType string, status v1alpha1.ProviderStatus) string {
	providerID := uuid.New().String()
	ExpectWithOffset(1, reg.Claim(name, serviceType)).To(Succeed())
	ExpectWithOffset(1, st.Save(ctx, store.StoredProvider{
		ID:            providerID,
		Name:          name,
		ServiceType:   serviceType,
		SchemaVersion: "v1alpha1",
		Type:          "embedded",
		CreateTime:    time.Now(),
		UpdateTime:    time.Now(),
	})).To(Succeed())
	ht.SetState(providerID, status, time.Now())
	return providerID
}
