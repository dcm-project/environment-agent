package routing_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/messaging"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"
	"github.com/dcm-project/environment-agent/internal/routing"
	"github.com/dcm-project/environment-agent/internal/routing/routingtest"
)

var _ = Describe("Resource Operation Routing", Label("integration"), func() {
	var (
		ctx           context.Context
		cancel        context.CancelFunc
		testConn      *nats.Conn
		testJS        jetstream.JetStream
		responseSub   *nats.Subscription
		registry      *provider.Registry
		healthTracker *provider.InMemoryHealthTracker
		fakeForwarder *routingtest.FakeSPForwarder
		fakeRetry     *routingtest.FakeRetryConsumer
		st            *routingtest.FakeStore
		publisher     *routingtest.NATSPublisher
		denyList      *routing.ResourceSet
		router        *routing.Router
		topicName     string
		topics        messaging.TopicNames
		routingCfg    config.RoutingConfig
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("agent-test-%s", uuid.New().String()[:8])
		// Router.TopicName/RetryTopic below use topics.Main/topics.Retry, the
		// prefixed subjects main.go wires in production, not the bare
		// topicName — so a regression passing the unprefixed base fails here too.
		topics = messaging.DeriveTopicNames(topicName, "")
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
		fakeForwarder = &routingtest.FakeSPForwarder{}
		fakeRetry = &routingtest.FakeRetryConsumer{}
		st = routingtest.NewFakeStore()
		publisher = &routingtest.NATSPublisher{JS: testJS}
		denyList = routing.NewResourceSet(100)

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
		router = routing.NewRouter(routing.RouterDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     fakeForwarder,
			Publisher:     publisher,
			RetryConsumer: fakeRetry,
			DenyList:      denyList,
			Config:        routingCfg,
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topics.Main,
			RetryTopic:    topics.Retry,
		})
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

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-embed-001", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-embed-001"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.TopicName).To(Equal(topics.Main))
		Expect(data.Status).To(Equal("PROVISIONING"))
	})

	It("routes deletion to Ready embedded SP (IT-RTE-015)", func() {
		registerProvider("embedded-sp", "container", "", "embedded", v1alpha1.Ready)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildDeleteCE("res-embed-del", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.DeleteCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.deletion-acknowledged"))
		var data routing.DeletionAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-embed-del"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.TopicName).To(Equal(topics.Main))
		Expect(data.Status).To(Equal("DELETING"))
	})

	It("routes creation to Ready external SP (IT-RTE-020)", func() {
		registerProvider("external-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-ext-001", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-ext-001"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.TopicName).To(Equal(topics.Main))
		Expect(data.Status).To(Equal("PROVISIONING"))
	})

	It("routes deletion to Ready external SP (IT-RTE-030)", func() {
		registerProvider("external-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildDeleteCE("res-123", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.DeleteCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.deletion-acknowledged"))
		var data routing.DeletionAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-123"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.TopicName).To(Equal(topics.Main))
		Expect(data.Status).To(Equal("DELETING"))
	})

	It("rejects unsupported service type with error CE (IT-RTE-040)", func() {
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-no-sp", "storage"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		ce := routingtest.ExpectResponseCE(responseSub)
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

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-unavail", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-unavail"))
		Expect(data.Error).To(Equal("SP_UNAVAILABLE"))
	})

	It("treats an empty provider ID as Unavailable and logs the anomaly (IT-RTE-150)", func() {
		Expect(registry.Claim("corrupt-sp", "database")).To(Succeed())
		Expect(st.Save(ctx, store.StoredProvider{
			ID:            "", // simulated store/registry data corruption (REQ-RTE-026)
			Name:          "corrupt-sp",
			Endpoint:      "http://mock:8080",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
			Type:          "external",
			CreateTime:    time.Now(),
			UpdateTime:    time.Now(),
		})).To(Succeed())

		ch := &captureLogHandler{}
		router = routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st,
			Forwarder: fakeForwarder, Publisher: publisher, RetryConsumer: fakeRetry,
			DenyList: denyList, Config: routingCfg, Logger: slog.New(ch),
			AgentName: "agent-prod-1", TopicName: topics.Main, RetryTopic: topics.Retry,
		})

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-corrupt", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0), "SP must not be called for a corrupted provider record")

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-corrupt"))
		Expect(data.Error).To(Equal("SP_UNAVAILABLE"))

		found := false
		for _, rec := range ch.all() {
			if rec.Message == "provider has empty ID (data corruption)" && rec.Level == slog.LevelError {
				found = true
			}
		}
		Expect(found).To(BeTrue(), "empty-ID anomaly must be logged at ERROR")
	})

	It("propagates a transient store error for redelivery instead of swallowing it (resolve.go coverage hardening)", func() {
		Expect(registry.Claim("flaky-sp", "database")).To(Succeed())
		st.GetByNameErr = fmt.Errorf("simulated transient store failure")
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-store-err", "database"))
		Expect(err).To(HaveOccurred(), "a transient store error must propagate so the caller can retry/redeliver, not be swallowed")
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))
		routingtest.ExpectNoResponseCE(responseSub, 200*time.Millisecond)
	})

	It("treats a registry/store inconsistency (registered but no store record) like an unregistered service type (resolve.go coverage hardening)", func() {
		// FakeStore.GetByName returns an error for an absent map key, so a
		// direct nil insert (bypassing Save) is required to reach the actual
		// sp==nil branch — matching production FileStore's GetByName, which
		// returns (nil, nil) rather than an error when a record isn't found.
		Expect(registry.Claim("ghost-sp", "database")).To(Succeed())
		st.Providers["ghost-sp"] = nil
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-ghost", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-ghost"))
		Expect(data.Error).To(Equal("UNSUPPORTED_SERVICE_TYPE"))
	})

	It("treats a valid provider with no recorded health state as Unavailable (resolve.go coverage hardening)", func() {
		// A real, non-empty ID that healthTracker.SetState was never called
		// for (distinct from an explicit Unhealthy/Unavailable state).
		Expect(registry.Claim("unknown-health-sp", "database")).To(Succeed())
		Expect(st.Save(ctx, store.StoredProvider{
			ID: uuid.New().String(), Name: "unknown-health-sp", Endpoint: "http://mock:8080",
			ServiceType: "database", SchemaVersion: "v1alpha1", Type: "external",
			CreateTime: time.Now(), UpdateTime: time.Now(),
		})).To(Succeed())
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-no-health", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-no-health"))
		Expect(data.Error).To(Equal("SP_UNAVAILABLE"))
	})

	It("queues creation when SP is Unhealthy (IT-RTE-060)", func() {
		registerProvider("unhealthy-sp", "database", "http://mock:8080", "external", v1alpha1.Unhealthy)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-unhealthy", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		ce := routingtest.ExpectResponseCE(responseSub)
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

		err := router.HandleRequest(ctx, routingtest.BuildDeleteCE("res-del-001", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.DeleteCallCount()).To(Equal(0))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.request-queued"))
		var data routing.RequestQueuedData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-del-001"))
		Expect(data.ServiceType).To(Equal("database"))
		Expect(data.Status).To(Equal("QUEUED"))
	})

	It("exhausts retries with configurable policy (IT-RTE-070)", func() {
		registerProvider("retry-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		fakeForwarder.CreateErr = &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"}
		routingCfg.RetryMaxAttempts = 5
		routingCfg.RetryBackoff = time.Millisecond
		routingCfg.RetryMaxBackoff = time.Millisecond
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-retry", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(5))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-retry"))
		Expect(data.Error).To(Equal("RETRY_EXHAUSTED"))
	})

	It("does not leak the SP response body into SP-error logs (leak-check, mirrors forwarder_test.go AC-RCM-150)", func() {
		registerProvider("leak-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		fakeForwarder.CreateErr = &routing.SPResponseError{StatusCode: 503, Message: "sensitive backend detail"}
		routingCfg.RetryMaxAttempts = 2
		routingCfg.RetryBackoff = time.Millisecond
		routingCfg.RetryMaxBackoff = time.Millisecond

		ch := &captureLogHandler{}
		router = routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st, Forwarder: fakeForwarder,
			Publisher: publisher, RetryConsumer: fakeRetry, DenyList: denyList, Config: routingCfg,
			Logger: slog.New(ch), AgentName: "agent-prod-1", TopicName: topics.Main, RetryTopic: topics.Retry,
		})

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-leak", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(2))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))

		var spErrRecords []slog.Record
		for _, rec := range ch.records {
			if rec.Message == "SP error" || rec.Message == "SP call failed, retrying" {
				spErrRecords = append(spErrRecords, rec)
			}
		}
		Expect(spErrRecords).NotTo(BeEmpty())
		for _, rec := range spErrRecords {
			v, ok := attrValue(rec, "http_status")
			Expect(ok).To(BeTrue(), "%s log must carry http_status instead of the raw error", rec.Message)
			Expect(v.Kind()).To(Equal(slog.KindInt64), "http_status must be logged as an int, not stringified")
			Expect(v.Int64()).To(BeEquivalentTo(503))
			_, hasErrAttr := attrValue(rec, "error")
			Expect(hasErrAttr).To(BeFalse(), "%s log must not carry a raw error attr for *SPResponseError", rec.Message)

			var body string
			rec.Attrs(func(a slog.Attr) bool {
				body += a.Value.String()
				return true
			})
			Expect(body).NotTo(ContainSubstring("sensitive backend detail"))
		}
	})

	It("applies retry policy with minimal budget (IT-RTE-080)", func() {
		registerProvider("retry-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		fakeForwarder.CreateErr = &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"}
		routingCfg.RetryMaxAttempts = 1
		routingCfg.RetryBackoff = time.Millisecond
		routingCfg.RetryMaxBackoff = time.Millisecond
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-retry-min", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-retry-min"))
		Expect(data.Error).To(Equal("RETRY_EXHAUSTED"))
	})

	It("fails immediately on non-retryable 4xx (IT-RTE-090)", func() {
		registerProvider("bad-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		fakeForwarder.CreateErr = &routing.SPResponseError{StatusCode: 400, Message: "Bad Request"}
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-4xx", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var data routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-4xx"))
		Expect(data.Error).To(Equal("NON_RETRYABLE_SP_ERROR"))
	})

	It("deny list filters cancelled create and logs the drop (IT-RTE-100, IT-RTE-142)", func() {
		registerProvider("normal-sp", "container", "", "embedded", v1alpha1.Ready)
		denyList.Add("res-456")

		ch := &captureLogHandler{}
		router = routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st,
			Forwarder: fakeForwarder, Publisher: publisher, RetryConsumer: fakeRetry,
			DenyList: denyList, Config: routingCfg, Logger: slog.New(ch),
			AgentName: "agent-prod-1", TopicName: topics.Main, RetryTopic: topics.Retry,
		})

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-456", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		routingtest.ExpectNoResponseCE(responseSub, 500*time.Millisecond)

		found := false
		for _, rec := range ch.all() {
			if rec.Message != "create request dropped, resource in deny list" || rec.Level != slog.LevelInfo {
				continue
			}
			rec.Attrs(func(a slog.Attr) bool {
				if a.Key == "resource_id" && a.Value.String() == "res-456" {
					found = true
				}
				return true
			})
		}
		Expect(found).To(BeTrue(), "deny-list drop must be logged at INFO with resource_id")
	})

	It("deny list consume-on-use allows second request through (IT-RTE-105)", func() {
		registerProvider("normal-sp", "container", "", "embedded", v1alpha1.Ready)
		denyList.Add("res-consume")
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-consume", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))

		err = router.HandleRequest(ctx, routingtest.BuildCreateCE("res-consume", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-consume"))
		Expect(data.Status).To(Equal("PROVISIONING"))
	})

	It("deny list LRU eviction allows evicted resource through (IT-RTE-110)", func() {
		smallDenyList := routing.NewResourceSet(3)
		smallDenyList.Add("res-oldest")
		smallDenyList.Add("res-mid")
		smallDenyList.Add("res-newest")
		smallDenyList.Add("res-evicting") // evicts res-oldest

		registerProvider("normal-sp", "container", "", "embedded", v1alpha1.Ready)
		router = routing.NewRouter(routing.RouterDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     fakeForwarder,
			Publisher:     publisher,
			RetryConsumer: fakeRetry,
			DenyList:      smallDenyList,
			Config:        routingCfg,
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topics.Main,
			RetryTopic:    topics.Retry,
		})

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-oldest", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-oldest"))
		Expect(data.Status).To(Equal("PROVISIONING"))

		err = router.HandleRequest(ctx, routingtest.BuildCreateCE("res-newest", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1)) // still denied, no new call
	})

	It("cancel for request in retry topic removes matching message and logs the purge summary (IT-RTE-120, IT-RTE-142)", func() {
		registerProvider("unhealthy-sp", "database", "http://mock:8080", "external", v1alpha1.Unhealthy)

		acked, nakked := false, false
		fakeRetry.Messages = []routing.RetryMessage{
			{Data: routingtest.BuildCreateCE("res-789", "database"), ResourceID: "res-789", ServiceType: "database", AckFunc: func() error { acked = true; return nil }},
			{
				Data: routingtest.BuildCreateCE("res-other", "database"), ResourceID: "res-other", ServiceType: "database",
				AckFunc: func() error { return fmt.Errorf("non-matching message must be Nak'd, not acked") },
				NakFunc: func() error { nakked = true; return nil },
			},
		}
		ch := &captureLogHandler{}
		router = routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st,
			Forwarder: fakeForwarder, Publisher: publisher, RetryConsumer: fakeRetry,
			DenyList: denyList, Config: routingCfg, Logger: slog.New(ch),
			AgentName: "agent-prod-1", TopicName: topics.Main, RetryTopic: topics.Retry,
		})

		err := router.HandleCancel(ctx, routingtest.BuildCancelCE("res-789", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(acked).To(BeTrue())
		Expect(nakked).To(BeTrue(),
			"non-matching retry message must be Nak'd in place, not acked+republished")
		Expect(fakeForwarder.CreateCallCount()).To(Equal(0))
		Expect(denyList.Contains("res-789")).To(BeTrue())

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.cancel-acknowledged"))
		var data routing.CancelAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-789"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.ServiceType).To(Equal("database"))

		found := false
		for _, rec := range ch.all() {
			if rec.Message != "retry topic purged for cancel" || rec.Level != slog.LevelInfo {
				continue
			}
			resourceID, _ := attrValue(rec, "resource_id")
			matched, matchedOK := attrValue(rec, "matched")
			requeued, requeuedOK := attrValue(rec, "requeued")
			// Kind-check before Int64(): a missing/wrong-typed attribute must
			// fail this assertion cleanly, not panic the whole test run.
			if resourceID.String() == "res-789" &&
				matchedOK && matched.Kind() == slog.KindInt64 && matched.Int64() == 1 &&
				requeuedOK && requeued.Kind() == slog.KindInt64 && requeued.Int64() == 1 {
				found = true
			}
		}
		Expect(found).To(BeTrue(), "purge summary must be logged at INFO with resource_id, matched, requeued")
	})

	It("delete bypasses deny list — denied resource still gets deleted (TC-7)", func() {
		registerProvider("normal-sp", "container", "", "embedded", v1alpha1.Ready)
		denyList.Add("res-deny-del")
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildDeleteCE("res-deny-del", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.DeleteCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.deletion-acknowledged"))
		var data routing.DeletionAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-deny-del"))
		Expect(data.Status).To(Equal("DELETING"))

		Expect(denyList.Contains("res-deny-del")).To(BeTrue(), "deny list entry should NOT be consumed by delete")
	})

	It("cancel before any request adds to deny list and publishes cancel-ack CE (TC-7)", Label("integration"), func() {
		setupDefaultRouter()

		err := router.HandleCancel(ctx, routingtest.BuildCancelCE("res-never-seen", "container"))
		Expect(err).NotTo(HaveOccurred())

		Expect(denyList.Contains("res-never-seen")).To(BeTrue())

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.cancel-acknowledged"))
		var data routing.CancelAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-never-seen"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
	})

	It("cancel rejected for in-flight provisioning (IT-RTE-130)", func() {
		registerProvider("normal-sp", "container", "", "embedded", v1alpha1.Ready)
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-101", "container"))
		Expect(err).NotTo(HaveOccurred())

		// Drain the creation-ack CE from the first request
		_ = routingtest.ExpectResponseCE(responseSub)

		err = router.HandleCancel(ctx, routingtest.BuildCancelCE("res-101", "container"))
		Expect(err).NotTo(HaveOccurred())

		Expect(denyList.Contains("res-101")).To(BeFalse())

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.cancel-rejected"))
		var data routing.CancelRejectedData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-101"))
		Expect(data.AgentName).To(Equal("agent-prod-1"))
		Expect(data.Reason).To(Equal("resource already claimed"))
	})

	It("allows delete after successful create for same resourceId (IT-RTE-175)", func() {
		registerProvider("lifecycle-sp", "container", "", "embedded", v1alpha1.Ready)
		setupDefaultRouter()

		Expect(router.HandleRequest(ctx, routingtest.BuildCreateCE("res-lifecycle", "container"))).To(Succeed())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))
		_ = routingtest.ExpectResponseCE(responseSub)

		Expect(router.HandleRequest(ctx, routingtest.BuildDeleteCE("res-lifecycle", "container"))).To(Succeed())
		Expect(fakeForwarder.DeleteCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.deletion-acknowledged"))
		var data routing.DeletionAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-lifecycle"))
		Expect(data.Status).To(Equal("DELETING"))
	})

	It("blocks a concurrent duplicate forward for the same resourceId in flight, without blocking the eventual legitimate one (IT-RTE-180)", func() {
		registerProvider("gate-sp", "container", "", "embedded", v1alpha1.Ready)
		gated := &routingtest.GatedSPForwarder{}
		router = routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st,
			Forwarder: gated, Publisher: publisher, DenyList: denyList,
			Config: routingCfg, Logger: slog.Default(), AgentName: "agent-prod-1",
			TopicName: topics.Main, RetryTopic: topics.Retry,
		})

		firstErr := make(chan error, 1)
		go func() {
			firstErr <- router.HandleRequest(ctx, routingtest.BuildCreateCE("res-race", "container"))
		}()

		Eventually(gated.CallCount, time.Second).Should(Equal(1))

		// Second forward for the SAME resourceId while the first is still
		// in-flight — must be rejected, not double-dispatched to the SP.
		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-race", "container"))
		Expect(err).To(MatchError(routing.ErrForwardInFlight))
		Expect(gated.CallCount()).To(Equal(1), "SP must not receive a concurrent second call for the same resourceId")

		gated.Release()
		Expect(<-firstErr).NotTo(HaveOccurred())

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))

		// The lock must be released once the in-flight attempt finished, so a
		// later (non-concurrent) redelivery for the same resourceId is not
		// permanently blocked.
		err = router.HandleRequest(ctx, routingtest.BuildDeleteCE("res-race", "container"))
		Expect(err).NotTo(HaveOccurred())
		_ = routingtest.ExpectResponseCE(responseSub)
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

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-id-check", "container"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-id-check"))
	})

	It("failure cleanup allows retry on redelivery (IT-RTE-145)", func() {
		registerProvider("retry-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		routingCfg.RetryMaxAttempts = 1
		routingCfg.RetryBackoff = time.Millisecond
		routingCfg.RetryMaxBackoff = time.Millisecond
		fakeForwarder.CreateErr = &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"}
		setupDefaultRouter()

		err := router.HandleRequest(ctx, routingtest.BuildCreateCE("res-retry-redeliver", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(1))

		// Drain the retry-exhausted error CE
		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))

		// Simulate redelivery: SP now healthy, same resource
		fakeForwarder.SetCreateErr(nil)

		err = router.HandleRequest(ctx, routingtest.BuildCreateCE("res-retry-redeliver", "database"))
		Expect(err).NotTo(HaveOccurred())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(2))

		ce = routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
		var data routing.CreationAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-retry-redeliver"))
		Expect(data.Status).To(Equal("PROVISIONING"))
	})

	It("cancel during startup window with no retry consumer publishes cancel-ack (IT-RTE-165)", func() {
		registerProvider("startup-sp", "container", "", "embedded", v1alpha1.Ready)
		router = routing.NewRouter(routing.RouterDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     fakeForwarder,
			Publisher:     publisher,
			DenyList:      denyList,
			Config:        routingCfg,
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topics.Main,
			RetryTopic:    topics.Retry,
		})

		err := router.HandleCancel(ctx, routingtest.BuildCancelCE("res-startup-cancel", "container"))
		Expect(err).NotTo(HaveOccurred())

		Expect(denyList.Contains("res-startup-cancel")).To(BeTrue())

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.cancel-acknowledged"))
		var data routing.CancelAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-startup-cancel"))
		Expect(data.ServiceType).To(Equal("container"))
	})

	It("context cancellation suppresses error CE after SP failure (IT-RTE-160)", func() {
		registerProvider("cancel-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		fakeForwarder.CreateErr = &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"}
		routingCfg.RetryMaxAttempts = 1
		setupDefaultRouter()

		cancelCtx, cancelFn := context.WithCancel(ctx)
		cancelFn()

		err := router.HandleRequest(cancelCtx, routingtest.BuildCreateCE("res-ctx-cancel", "database"))
		Expect(err).To(MatchError(context.Canceled))
		routingtest.ExpectNoResponseCE(responseSub, 100*time.Millisecond)
	})

	It("context deadline during retry backoff aborts without error CE (IT-RTE-170)", func() {
		registerProvider("deadline-sp", "database", "http://mock:8080", "external", v1alpha1.Ready)
		fakeForwarder.CreateErr = &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"}
		routingCfg.RetryMaxAttempts = 3
		routingCfg.RetryBackoff = 10 * time.Second
		routingCfg.RetryMaxBackoff = 10 * time.Second
		setupDefaultRouter()

		shortCtx, shortCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer shortCancel()

		err := router.HandleRequest(shortCtx, routingtest.BuildCreateCE("res-ctx-deadline", "database"))
		Expect(err).To(MatchError(context.DeadlineExceeded))
		routingtest.ExpectNoResponseCE(responseSub, 100*time.Millisecond)
	})

	It("uses only canonical snake_case correlation field names across a full retry-then-succeed lifecycle (IT-XC-LOG-030)", func() {
		providerID := uuid.New().String()
		Expect(registry.Claim("canon-sp", "database")).To(Succeed())
		Expect(st.Save(ctx, store.StoredProvider{
			ID: providerID, Name: "canon-sp", Endpoint: "http://mock:8080", ServiceType: "database",
			SchemaVersion: "v1alpha1", Type: "external", CreateTime: time.Now(), UpdateTime: time.Now(),
		})).To(Succeed())
		healthTracker.SetState(providerID, v1alpha1.Ready, time.Now())

		// One retryable failure then success, so both the failure-path
		// (SP error / SP call failed, retrying) and success-path (SP
		// dispatch completed / published CE) log sites are exercised.
		fakeForwarder.CreateErr = &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"}
		fakeForwarder.FailFirst = 1
		routingCfg.RetryMaxAttempts = 3
		routingCfg.RetryBackoff = time.Millisecond
		routingCfg.RetryMaxBackoff = time.Millisecond

		ch := &captureLogHandler{}
		router = routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st, Forwarder: fakeForwarder,
			Publisher: publisher, RetryConsumer: fakeRetry, DenyList: denyList, Config: routingCfg,
			Logger: slog.New(ch), AgentName: "agent-prod-1", TopicName: topics.Main, RetryTopic: topics.Retry,
		})

		const resourceID = "res-canon-log"
		createCE := routingtest.BuildCreateCE(resourceID, "database")
		var envelope struct{ ID string }
		Expect(json.Unmarshal(createCE, &envelope)).To(Succeed())
		ceID := envelope.ID

		Expect(router.HandleRequest(ctx, createCE)).To(Succeed())
		Expect(fakeForwarder.CreateCallCount()).To(Equal(2))

		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))

		records := ch.all()
		Expect(records).NotTo(BeEmpty())

		disallowed := []string{"resourceId", "resourceID", "ceId", "ceID", "serviceType", "providerId", "providerID"}
		found := map[string]bool{}
		for _, rec := range records {
			rec.Attrs(func(a slog.Attr) bool {
				for _, bad := range disallowed {
					Expect(a.Key).NotTo(Equal(bad),
						"log %q uses non-canonical key %q instead of a snake_case correlation field", rec.Message, bad)
				}
				switch a.Key {
				case "resource_id":
					if a.Value.String() == resourceID {
						found["resource_id"] = true
					}
				case "ce_id":
					if a.Value.String() == ceID {
						found["ce_id"] = true
					}
				case "service_type":
					if a.Value.String() == "database" {
						found["service_type"] = true
					}
				case "provider_id":
					if a.Value.String() == providerID {
						found["provider_id"] = true
					}
				case "ce_type":
					found["ce_type"] = true
				}
				return true
			})
		}
		for _, key := range []string{"resource_id", "ce_id", "ce_type", "service_type", "provider_id"} {
			Expect(found[key]).To(BeTrue(), "expected at least one log record with canonical field %q populated with the expected value", key)
		}
	})
})
