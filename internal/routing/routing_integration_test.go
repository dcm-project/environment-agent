package routing_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// fakeSPForwarder records calls to CreateResource/DeleteResource.
type fakeSPForwarder struct {
	mu          sync.Mutex
	createCalls []createCall
	deleteCalls []deleteCall
	createErr   error
	deleteErr   error
}

type createCall struct {
	Endpoint string
	Embedded bool
	Req      routing.CreateResourceRequest
}

type deleteCall struct {
	Endpoint   string
	Embedded   bool
	ResourceID string
}

func (f *fakeSPForwarder) CreateResource(_ context.Context, endpoint string, embedded bool, req routing.CreateResourceRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls = append(f.createCalls, createCall{Endpoint: endpoint, Embedded: embedded, Req: req})
	return f.createErr
}

func (f *fakeSPForwarder) DeleteResource(_ context.Context, endpoint string, embedded bool, resourceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, deleteCall{Endpoint: endpoint, Embedded: embedded, ResourceID: resourceID})
	return f.deleteErr
}

func (f *fakeSPForwarder) CreateCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.createCalls)
}

func (f *fakeSPForwarder) DeleteCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deleteCalls)
}

// fakeRetryConsumer records retry topic operations.
type fakeRetryConsumer struct {
	mu           sync.Mutex
	messages     []routing.RetryMessage
	acked        []routing.RetryMessage
	republished  [][]byte
	fetchErr     error
	ackErr       error
	republishErr error
}

func (f *fakeRetryConsumer) FetchRetryMessages(_ context.Context) ([]routing.RetryMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.messages, f.fetchErr
}

func (f *fakeRetryConsumer) AckRetryMessage(_ context.Context, msg routing.RetryMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, msg)
	return f.ackErr
}

func (f *fakeRetryConsumer) RepublishToRetry(_ context.Context, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.republished = append(f.republished, data)
	return f.republishErr
}

// natsPublisher wraps a NATS JetStream connection as a routing.Publisher.
type natsPublisher struct {
	js jetstream.JetStream
}

func (p *natsPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := p.js.Publish(ctx, subject, data)
	return err
}

// fakeStore implements store.Store for test purposes.
type fakeStore struct {
	providers map[string]*store.StoredProvider
}

func newFakeStore() *fakeStore {
	return &fakeStore{providers: make(map[string]*store.StoredProvider)}
}

func (s *fakeStore) Save(_ context.Context, p store.StoredProvider) error {
	s.providers[p.Name] = &p
	return nil
}

func (s *fakeStore) Delete(_ context.Context, name string) error {
	delete(s.providers, name)
	return nil
}

func (s *fakeStore) List(_ context.Context) ([]store.StoredProvider, error) {
	result := make([]store.StoredProvider, 0, len(s.providers))
	for _, p := range s.providers {
		result = append(result, *p)
	}
	return result, nil
}

func (s *fakeStore) GetByID(_ context.Context, id string) (*store.StoredProvider, error) {
	for _, p := range s.providers {
		if p.ID == id {
			return p, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (s *fakeStore) GetByName(_ context.Context, name string) (*store.StoredProvider, error) {
	p, ok := s.providers[name]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return p, nil
}

// buildCreateCE constructs a creation request CloudEvent.
func buildCreateCE(resourceID, serviceType string) []byte {
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource("//dcm/control-plane")
	event.SetType("dcm.request.create")
	event.SetTime(time.Now())
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]any{
		"resourceId":  resourceID,
		"serviceType": serviceType,
		"spec":        map[string]any{"size": "small"},
	})
	data, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	return data
}

// buildDeleteCE constructs a deletion request CloudEvent.
func buildDeleteCE(resourceID, serviceType string) []byte {
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource("//dcm/control-plane")
	event.SetType("dcm.request.delete")
	event.SetTime(time.Now())
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]any{
		"resourceId":  resourceID,
		"serviceType": serviceType,
	})
	data, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	return data
}

// buildCancelCE constructs a cancel request CloudEvent.
func buildCancelCE(resourceID, serviceType string) []byte {
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource("//dcm/control-plane")
	event.SetType("dcm.request.cancel")
	event.SetTime(time.Now())
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]any{
		"resourceId":  resourceID,
		"serviceType": serviceType,
	})
	data, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	return data
}

const ceWaitTimeout = 5 * time.Second

// expectResponseCE reads one CE from the response subscription and returns it.
func expectResponseCE(sub *nats.Subscription) cloudevents.Event {
	msg, err := sub.NextMsg(ceWaitTimeout)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "expected a response CE on dcm.agents.responses")
	var event cloudevents.Event
	ExpectWithOffset(1, json.Unmarshal(msg.Data, &event)).To(Succeed())
	return event
}

// expectNoResponseCE asserts that no CE is received within the timeout.
func expectNoResponseCE(sub *nats.Subscription, timeout time.Duration) {
	msg, err := sub.NextMsg(timeout)
	ExpectWithOffset(1, err).To(MatchError(nats.ErrTimeout), "expected no response CE but got one")
	ExpectWithOffset(1, msg).To(BeNil())
}

var _ = Describe("Resource Operation Routing", Label("integration"), func() {
	var (
		ctx           context.Context
		cancel        context.CancelFunc
		testConn      *nats.Conn
		testJS        jetstream.JetStream
		responseSub   *nats.Subscription
		registry      *provider.Registry
		healthTracker *provider.InMemoryHealthTracker
		fakeForwarder *fakeSPForwarder
		fakeRetry     *fakeRetryConsumer
		st            *fakeStore
		publisher     *natsPublisher
		denyList      *routing.DenyList
		router        *routing.Router
		topicName     string
		routingCfg    config.RoutingConfig
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("agent-test-%s", uuid.New().String()[:8])
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())

		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: "responses", Subjects: []string{"dcm.agents.responses"},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topicName + "-retry", Subjects: []string{topicName + ".retry"},
		})
		Expect(err).NotTo(HaveOccurred())

		responseSub, err = testConn.SubscribeSync("dcm.agents.responses")
		Expect(err).NotTo(HaveOccurred())
		Expect(testConn.Flush()).To(Succeed())

		registry = provider.NewRegistry()
		healthTracker = provider.NewInMemoryHealthTracker()
		fakeForwarder = &fakeSPForwarder{}
		fakeRetry = &fakeRetryConsumer{}
		st = newFakeStore()
		publisher = &natsPublisher{js: testJS}
		denyList = routing.NewDenyList(100)

		routingCfg = config.RoutingConfig{
			RetryMaxAttempts: 3,
			RetryBackoff:     2 * time.Second,
			RetryMaxBackoff:  30 * time.Second,
			DenyListMaxSize:  100000,
		}
	})

	AfterEach(func() {
		cancel()
		if responseSub != nil {
			_ = responseSub.Unsubscribe()
		}
		_ = testJS.DeleteStream(context.Background(), "responses")
		_ = testJS.DeleteStream(context.Background(), topicName+"-retry")
		testConn.Close()
	})

	setupDefaultRouter := func() {
		router = routing.NewRouter(registry, healthTracker, st, fakeForwarder, publisher, fakeRetry, denyList, routingCfg, slog.Default(), "agent-prod-1", topicName)
	}

	registerProvider := func(name, serviceType, endpoint, providerType string, status v1alpha1.ProviderStatus) {
		providerID := uuid.New().String()
		Expect(registry.Claim(name, serviceType)).To(Succeed())
		Expect(st.Save(ctx, store.StoredProvider{
			ID:            providerID,
			Name:          name,
			Endpoint:      endpoint,
			ServiceType:   serviceType,
			SchemaVersion: "v1alpha1",
			Type:          providerType,
			CreateTime:    time.Now(),
			UpdateTime:    time.Now(),
		})).To(Succeed())
		healthTracker.SetState(providerID, status, time.Now())
	}

	It("routes creation to Ready embedded SP (IT-RTE-010)", func() {
		registerProvider("embedded-sp", "container", "", "embedded", v1alpha1.Ready)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-embed-001", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-embed-001"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.TopicName).To(Equal(topicName))
		Expect(data.Status).To(Equal("PROVISIONING"))
	})

	It("routes deletion to Ready embedded SP (IT-RTE-015)", func() {
		registerProvider("embedded-sp", "container", "", "embedded", v1alpha1.Ready)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildDeleteCE("res-embed-del", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.DeleteCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.deletion-acknowledged"))
		var data routing.DeletionAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-embed-del"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.TopicName).To(Equal(topicName))
		Expect(data.Status).To(Equal("DELETING"))
	})

	It("routes creation to Ready external SP (IT-RTE-020)", func() {
		registerProvider("external-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-ext-001", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-ext-001"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.TopicName).To(Equal(topicName))
		Expect(data.Status).To(Equal("PROVISIONING"))
	})

	It("routes deletion to Ready external SP (IT-RTE-030)", func() {
		registerProvider("external-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildDeleteCE("res-123", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.DeleteCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.deletion-acknowledged"))
		var data routing.DeletionAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-123"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.TopicName).To(Equal(topicName))
		Expect(data.Status).To(Equal("DELETING"))
	})

	It("rejects unsupported service type with error CE (IT-RTE-040)", func() {
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-no-sp", "storage"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-no-sp"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.Error).To(Equal("UNSUPPORTED_SERVICE_TYPE"))
	})

	It("rejects request when SP is Unavailable (IT-RTE-050)", func() {
		registerProvider("unavailable-sp", "database", "http://mock:8080", "external", v1alpha1.Unavailable)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-unavail", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-unavail"))
		Expect(data.Error).To(Equal("SP_UNAVAILABLE"))
	})

	It("queues creation when SP is Unhealthy (IT-RTE-060)", func() {
		registerProvider("unhealthy-sp", "database", "http://mock:8080", "external", v1alpha1.Unhealthy)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-unhealthy", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.request-queued"))
		var data routing.RequestQueuedData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-unhealthy"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.ServiceType).To(Equal("database"))
		Expect(data.Status).To(Equal("QUEUED"))
	})

	It("queues deletion when SP is Unhealthy (IT-RTE-065)", func() {
		registerProvider("unhealthy-sp", "database", "http://mock:8080", "external", v1alpha1.Unhealthy)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildDeleteCE("res-del-001", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.DeleteCallCount()).To(Equal(0))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.request-queued"))
		var data routing.RequestQueuedData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-del-001"))
		Expect(data.ServiceType).To(Equal("database"))
		Expect(data.Status).To(Equal("QUEUED"))
	})

	It("exhausts retries with configurable policy (IT-RTE-070)", func() {
		registerProvider("retry-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		fakeForwarder.createErr = &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"}
		routingCfg.RetryMaxAttempts = 5
		routingCfg.RetryBackoff = time.Millisecond
		routingCfg.RetryMaxBackoff = time.Millisecond
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-retry", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(5))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-retry"))
		Expect(data.Error).To(Equal("RETRY_EXHAUSTED"))
	})

	It("applies retry policy with minimal budget (IT-RTE-080)", func() {
		registerProvider("retry-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		fakeForwarder.createErr = &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"}
		routingCfg.RetryMaxAttempts = 1
		routingCfg.RetryBackoff = time.Millisecond
		routingCfg.RetryMaxBackoff = time.Millisecond
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-retry-min", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-retry-min"))
		Expect(data.Error).To(Equal("RETRY_EXHAUSTED"))
	})

	It("fails immediately on non-retryable 4xx (IT-RTE-090)", func() {
		registerProvider("bad-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		fakeForwarder.createErr = &routing.SPResponseError{StatusCode: 400, Message: "Bad Request"}
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-4xx", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-4xx"))
		Expect(data.Error).To(Equal("NON_RETRYABLE_SP_ERROR"))
	})

	It("deny list filters cancelled create (IT-RTE-100)", func() {
		registerProvider("normal-sp", "container", "", "embedded", v1alpha1.Ready)
		denyList.Add("res-456")
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-456", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		expectNoResponseCE(responseSub, 500*time.Millisecond)
	})

	It("deny list consume-on-use allows second request through (IT-RTE-105)", func() {
		registerProvider("normal-sp", "container", "", "embedded", v1alpha1.Ready)
		denyList.Add("res-consume")
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-consume", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		err = router.HandleRequest(ctx, buildCreateCE("res-consume", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-consume"))
		Expect(data.Status).To(Equal("PROVISIONING"))
	})

	It("deny list LRU eviction allows evicted resource through (IT-RTE-110)", func() {
		smallDenyList := routing.NewDenyList(3)
		smallDenyList.Add("res-oldest")
		smallDenyList.Add("res-mid")
		smallDenyList.Add("res-newest")
		smallDenyList.Add("res-evicting") // evicts res-oldest

		registerProvider("normal-sp", "container", "", "embedded", v1alpha1.Ready)
		router = routing.NewRouter(registry, healthTracker, st, fakeForwarder, publisher, fakeRetry, smallDenyList, routingCfg, slog.Default(), "agent-prod-1", topicName)

		err := router.HandleRequest(ctx, buildCreateCE("res-oldest", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-oldest"))
		Expect(data.Status).To(Equal("PROVISIONING"))

		err = router.HandleRequest(ctx, buildCreateCE("res-newest", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1)) // still denied, no new call
	})

	It("cancel for request in retry topic removes matching message (IT-RTE-120)", func() {
		registerProvider("unhealthy-sp", "database", "http://mock:8080", "external", v1alpha1.Unhealthy)

		acked := false
		fakeRetry.messages = []routing.RetryMessage{
			{Data: buildCreateCE("res-789", "database"), ResourceID: "res-789", ServiceType: "database", AckFunc: func() error { acked = true; return nil }},
			{Data: buildCreateCE("res-other", "database"), ResourceID: "res-other", ServiceType: "database", AckFunc: func() error { return nil }},
		}
		setupDefaultRouter()

		err := router.HandleCancel(ctx, buildCancelCE("res-789", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(acked).To(BeTrue())
		Expect(fakeRetry.republished).To(HaveLen(1))
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))
		Expect(denyList.Contains("res-789")).To(BeTrue())

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.cancel-acknowledged"))
		var data routing.CancelAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-789"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.ServiceType).To(Equal("database"))
	})

	It("delete bypasses deny list — denied resource still gets deleted (TC-7)", func() {
		registerProvider("normal-sp", "container", "", "embedded", v1alpha1.Ready)
		denyList.Add("res-deny-del")
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildDeleteCE("res-deny-del", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.DeleteCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.deletion-acknowledged"))
		var data routing.DeletionAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-deny-del"))
		Expect(data.Status).To(Equal("DELETING"))

		Expect(denyList.Contains("res-deny-del")).To(BeTrue(), "deny list entry should NOT be consumed by delete")
	})

	It("cancel before any request adds to deny list and publishes cancel-ack CE (TC-7)", Label("integration"), func() {
		setupDefaultRouter()

		err := router.HandleCancel(ctx, buildCancelCE("res-never-seen", "container"))
		Expect(err).NotTo(HaveOccurred())

		Expect(denyList.Contains("res-never-seen")).To(BeTrue())

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.cancel-acknowledged"))
		var data routing.CancelAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-never-seen"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
	})

	It("cancel rejected for in-flight provisioning (IT-RTE-130)", func() {
		registerProvider("normal-sp", "container", "", "embedded", v1alpha1.Ready)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-101", "container"))
		Expect(err).NotTo(HaveOccurred())

		// Drain the creation-ack CE from the first request
		_ = expectResponseCE(responseSub)

		err = router.HandleCancel(ctx, buildCancelCE("res-101", "container"))
		Expect(err).NotTo(HaveOccurred())

		Expect(denyList.Contains("res-101")).To(BeFalse())

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.cancel-rejected"))
		var data routing.CancelRejectedData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-101"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.Reason).To(Equal("resource already dispatched"))
	})

	It("allows delete after successful create for same resourceId (IT-RTE-140)", func() {
		registerProvider("lifecycle-sp", "container", "", "embedded", v1alpha1.Ready)
		setupDefaultRouter()

		Expect(router.HandleRequest(ctx, buildCreateCE("res-lifecycle", "container"))).To(Succeed())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))
		_ = expectResponseCE(responseSub)

		Expect(router.HandleRequest(ctx, buildDeleteCE("res-lifecycle", "container"))).To(Succeed())
		Expect(fakeForwarder.DeleteCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.deletion-acknowledged"))
		var data routing.DeletionAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-lifecycle"))
		Expect(data.Status).To(Equal("DELETING"))
	})

	It("resolves provider health by provider ID, not name (IT-RTE-135)", func() {
		fixedID := "id-not-a-name-12345"
		Expect(registry.Claim("my-provider", "container")).To(Succeed())
		Expect(st.Save(ctx, store.StoredProvider{
			ID:          fixedID,
			Name:        "my-provider",
			Endpoint:    "",
			ServiceType: "container",
			Type:        "embedded",
			CreateTime:  time.Now(),
			UpdateTime:  time.Now(),
		})).To(Succeed())
		healthTracker.SetState(fixedID, v1alpha1.Ready, time.Now())
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-id-check", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-id-check"))
	})

	It("returns Unavailable when stored provider has empty ID (IT-RTE-150)", func() {
		Expect(registry.Claim("corrupt-sp", "database")).To(Succeed())
		Expect(st.Save(ctx, store.StoredProvider{
			ID:            "",
			Name:          "corrupt-sp",
			Endpoint:      "http://mock:8080",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
			Type:          "external",
			CreateTime:    time.Now(),
			UpdateTime:    time.Now(),
		})).To(Succeed())
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-corrupt", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-corrupt"))
		Expect(data.Error).To(Equal("SP_UNAVAILABLE"))
	})

	It("failure cleanup allows retry on redelivery (IT-RTE-145)", func() {
		registerProvider("retry-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		routingCfg.RetryMaxAttempts = 1
		routingCfg.RetryBackoff = time.Millisecond
		routingCfg.RetryMaxBackoff = time.Millisecond
		fakeForwarder.createErr = &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"}
		setupDefaultRouter()

		err := router.HandleRequest(ctx, buildCreateCE("res-retry-redeliver", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		// Drain the retry-exhausted error CE
		ce := expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))

		// Simulate redelivery: SP now healthy, same resource
		fakeForwarder.mu.Lock()
		fakeForwarder.createErr = nil
		fakeForwarder.mu.Unlock()

		err = router.HandleRequest(ctx, buildCreateCE("res-retry-redeliver", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(2))

		ce = expectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-retry-redeliver"))
		Expect(data.Status).To(Equal("PROVISIONING"))
	})
})
