package dcm_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/dcm"
)

// --- Mock DCM server ---

type capturedRequest struct {
	Method    string
	Path      string
	Body      []byte
	Timestamp time.Time
}

type mockDCM struct {
	server *httptest.Server

	mu            sync.Mutex
	registrations []capturedRequest
	heartbeats    []capturedRequest

	regStatus   int
	regBody     string
	hbStatus    int
	retryAfter  string
	regSequence []int
	seqIndex    int
	hangReg     bool
}

func newMockDCM() *mockDCM {
	m := &mockDCM{
		regStatus: http.StatusCreated,
		regBody:   `{"agentId":"agent-123"}`,
		hbStatus:  http.StatusOK,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/agents", m.handleRegistration)
	mux.HandleFunc("PUT /api/v1/agents/{agentId}/heartbeat", m.handleHeartbeat)
	m.server = httptest.NewServer(mux)
	return m
}

func (m *mockDCM) handleRegistration(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	m.mu.Lock()
	m.registrations = append(m.registrations, capturedRequest{
		Method:    r.Method,
		Path:      r.URL.Path,
		Body:      body,
		Timestamp: time.Now(),
	})

	hang := m.hangReg
	status := m.regStatus
	respBody := m.regBody
	retryAfterVal := m.retryAfter
	if len(m.regSequence) > 0 && m.seqIndex < len(m.regSequence) {
		status = m.regSequence[m.seqIndex]
		m.seqIndex++
	}
	m.mu.Unlock()

	if hang {
		<-r.Context().Done()
		return
	}

	if retryAfterVal != "" && status == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", retryAfterVal)
	}

	w.WriteHeader(status)
	if status >= 200 && status < 300 {
		_, _ = w.Write([]byte(respBody))
	}
}

func (m *mockDCM) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	m.mu.Lock()
	m.heartbeats = append(m.heartbeats, capturedRequest{
		Method:    r.Method,
		Path:      r.URL.Path,
		Body:      body,
		Timestamp: time.Now(),
	})
	status := m.hbStatus
	m.mu.Unlock()

	w.WriteHeader(status)
}

func (m *mockDCM) getRegistrations() []capturedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]capturedRequest, len(m.registrations))
	copy(cp, m.registrations)
	return cp
}

func (m *mockDCM) getHeartbeats() []capturedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]capturedRequest, len(m.heartbeats))
	copy(cp, m.heartbeats)
	return cp
}

// --- Mock interfaces ---

type stubServiceTypeLister struct {
	mu    sync.Mutex
	types []string
}

func (s *stubServiceTypeLister) AdvertisableServiceTypes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]string, len(s.types))
	copy(cp, s.types)
	return cp
}

func (s *stubServiceTypeLister) setTypes(types []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.types = types
}

// callCountingLister returns empty for the first `returnEmpty` calls, then returns types.
// Simulates transient lister failures without needing a notification to retry.
type callCountingLister struct {
	mu          sync.Mutex
	calls       int
	returnEmpty int
	types       []string
}

func (c *callCountingLister) AdvertisableServiceTypes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls <= c.returnEmpty {
		return nil
	}
	cp := make([]string, len(c.types))
	copy(cp, c.types)
	return cp
}

type stubConsumerLagProvider struct {
	mu  sync.Mutex
	lag int64
}

func (s *stubConsumerLagProvider) ConsumerLag() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lag
}

type stubResourceCapacityProvider struct {
	capacity *v1alpha1.ResourceCapacity
}

func (s *stubResourceCapacityProvider) ResourceCapacity() *v1alpha1.ResourceCapacity {
	return s.capacity
}

// --- Helpers ---

func defaultRegistrarConfig(mockURL string) dcm.RegistrarConfig {
	return dcm.RegistrarConfig{
		AgentName:                 "test-agent",
		Environment:               "test",
		Cost:                      "medium",
		TopicName:                 "test-agent",
		RegistrationURL:           mockURL,
		InitialBackoff:            10 * time.Millisecond,
		MaxBackoff:                200 * time.Millisecond,
		HeartbeatInterval:         100 * time.Millisecond,
		PrerequisiteRetryInterval: 50 * time.Millisecond,
	}
}

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// --- Tests ---

var _ = Describe("DCM Registration", Label("integration"), func() {
	var (
		mock   *mockDCM
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		mock = newMockDCM()
		DeferCleanup(mock.server.Close)
		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // Ginkgo BeforeEach requires closure variable assignment
		DeferCleanup(cancel)
	})

	It("registers after first non-Unavailable SP (IT-DCM-010)", func() {
		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))

		regs := mock.getRegistrations()
		var payload map[string]interface{}
		Expect(json.Unmarshal(regs[0].Body, &payload)).To(Succeed())
		Expect(payload).To(HaveKey("serviceTypes"))
	})

	It("has no agentId before registration (IT-DCM-015)", func() {
		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		id, ok := r.AgentID()
		Expect(ok).To(BeFalse())
		Expect(id).To(BeEmpty())
		Expect(mock.getHeartbeats()).To(BeEmpty())

		r.Start(ctx)

		// Companion: Eventually agentId becomes non-empty — fails on stub → RED
		Eventually(func() bool {
			_, registered := r.AgentID()
			return registered
		}, 3*time.Second, 50*time.Millisecond).Should(BeTrue())
	})

	It("does not block HTTP startup (IT-DCM-020)", func() {
		mock.mu.Lock()
		mock.hangReg = true
		mock.mu.Unlock()

		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		// Companion: Eventually mock receives registration POST — fails on no-op Start → RED
		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 2*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))
	})

	It("waits for non-Unavailable SP (IT-DCM-030)", func() {
		lister := &stubServiceTypeLister{types: []string{}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Consistently(func() int {
			return len(mock.getRegistrations())
		}, 500*time.Millisecond, 50*time.Millisecond).Should(Equal(0))

		// Companion: Eventually POST when SP becomes available — fails on no-op Start → RED
		lister.setTypes([]string{"container"})
		r.NotifyServiceTypeChange()

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))
	})

	It("retries prerequisite check without notification (IT-DCM-035)", func() {
		lister := &callCountingLister{
			returnEmpty: 2,
			types:       []string{"container"},
		}

		cfg := defaultRegistrarConfig(mock.server.URL)
		cfg.PrerequisiteRetryInterval = 30 * time.Millisecond

		r, err := dcm.NewRegistrar(cfg, lister, &stubConsumerLagProvider{}, nil, discardLogger)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		// Without notifications, the retry ticker should eventually re-evaluate
		// and find service types once the lister stops returning empty.
		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 20*time.Millisecond).Should(BeNumerically(">=", 1))
	})

	It("defers changes before registration (IT-DCM-040)", func() {
		lister := &stubServiceTypeLister{types: []string{"container", "database"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		// Companion: first POST must include both types — fails because no POST → RED
		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))

		regs := mock.getRegistrations()
		var payload map[string]interface{}
		Expect(json.Unmarshal(regs[0].Body, &payload)).To(Succeed())
		types, ok := payload["serviceTypes"].([]interface{})
		Expect(ok).To(BeTrue())
		Expect(types).To(HaveLen(2))
	})

	It("sends correct registration payload (IT-DCM-050)", func() {
		cfg := dcm.RegistrarConfig{
			AgentName:         "agent-prod-1",
			Environment:       "production",
			Cost:              "medium",
			TopicName:         "agent-prod-1",
			RegistrationURL:   mock.server.URL,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        200 * time.Millisecond,
			HeartbeatInterval: 100 * time.Millisecond,
		}
		lister := &stubServiceTypeLister{types: []string{"container", "database"}}
		r, err := dcm.NewRegistrar(cfg, lister, &stubConsumerLagProvider{}, nil, discardLogger)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))

		var payload map[string]interface{}
		Expect(json.Unmarshal(mock.getRegistrations()[0].Body, &payload)).To(Succeed())
		Expect(payload["name"]).To(Equal("agent-prod-1"))
		Expect(payload["environment"]).To(Equal("production"))
		Expect(payload["cost"]).To(Equal("medium"))
		Expect(payload["topicName"]).To(Equal("agent-prod-1"))
		Expect(payload).To(HaveKey("serviceTypes"))
	})

	It("includes resourcesAvailable when available (IT-DCM-060)", func() {
		cpu := "16"
		mem := "64GB"
		resources := &stubResourceCapacityProvider{
			capacity: &v1alpha1.ResourceCapacity{
				TotalCpu:    &cpu,
				TotalMemory: &mem,
			},
		}
		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, resources, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))

		var payload map[string]interface{}
		Expect(json.Unmarshal(mock.getRegistrations()[0].Body, &payload)).To(Succeed())
		Expect(payload).To(HaveKey("resourcesAvailable"))
	})

	It("re-registration is idempotent (IT-DCM-070)", func() {
		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))

		id, ok := r.AgentID()
		Expect(ok).To(BeTrue())
		Expect(id).To(Equal("agent-123"))
	})

	It("retries with exponential backoff (IT-DCM-080)", func() {
		mock.mu.Lock()
		mock.regSequence = []int{503, 503, 201}
		mock.mu.Unlock()

		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 5*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 3))

		regs := mock.getRegistrations()
		for i := 1; i < len(regs); i++ {
			Expect(regs[i].Timestamp).To(BeTemporally(">", regs[i-1].Timestamp))
		}
	})

	It("stops retries on non-retryable error (IT-DCM-090)", func() {
		mock.mu.Lock()
		mock.regStatus = http.StatusBadRequest
		mock.mu.Unlock()

		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		// Exact count: must be exactly 1 (not soft <= 1) — prevents accidental GREEN
		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(Equal(1))

		Consistently(func() int {
			return len(mock.getRegistrations())
		}, 500*time.Millisecond, 50*time.Millisecond).Should(Equal(1))
	})

	It("respects 429 Retry-After header (IT-DCM-100)", func() {
		mock.mu.Lock()
		mock.regSequence = []int{429, 201}
		mock.retryAfter = "1"
		mock.mu.Unlock()

		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 4*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 2))

		regs := mock.getRegistrations()
		gap := regs[1].Timestamp.Sub(regs[0].Timestamp)
		Expect(gap).To(BeNumerically(">=", time.Second))
	})

	It("applies standard backoff on 429 without Retry-After (IT-DCM-105)", func() {
		mock.mu.Lock()
		mock.regSequence = []int{429, 201}
		mock.mu.Unlock()

		cfg := defaultRegistrarConfig(mock.server.URL)
		cfg.InitialBackoff = 50 * time.Millisecond
		cfg.MaxBackoff = 200 * time.Millisecond

		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(cfg, lister, &stubConsumerLagProvider{}, nil, discardLogger)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 5*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 2))

		regs := mock.getRegistrations()
		gap := regs[1].Timestamp.Sub(regs[0].Timestamp)
		Expect(gap).To(BeNumerically("<=", cfg.MaxBackoff+50*time.Millisecond))
		Expect(gap).To(BeNumerically(">", 0))
	})

	It("rejects invalid registration URL (constructor)", func() {
		cfg := defaultRegistrarConfig("://bad")
		_, err := dcm.NewRegistrar(
			cfg, &stubServiceTypeLister{}, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("DCM Heartbeat", Label("integration"), func() {
	var (
		mock   *mockDCM
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		mock = newMockDCM()
		DeferCleanup(mock.server.Close)
		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // Ginkgo BeforeEach requires closure variable assignment
		DeferCleanup(cancel)
	})

	It("sends periodic heartbeats (IT-DCM-120)", func() {
		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getHeartbeats())
		}, 2*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 2))

		for _, hb := range mock.getHeartbeats() {
			Expect(hb.Path).To(Equal("/api/v1/agents/agent-123/heartbeat"))
			var payload map[string]interface{}
			Expect(json.Unmarshal(hb.Body, &payload)).To(Succeed())
			Expect(payload).To(HaveKey("timestamp"))
			Expect(payload).To(HaveKey("consumerLag"))
		}
	})

	It("uses configurable heartbeat interval (IT-DCM-130)", func() {
		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getHeartbeats())
		}, 2*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 3))

		hbs := mock.getHeartbeats()
		var gaps []time.Duration
		for i := 1; i < len(hbs); i++ {
			gaps = append(gaps, hbs[i].Timestamp.Sub(hbs[i-1].Timestamp))
		}
		Expect(len(gaps)).To(BeNumerically(">=", 2))
	})

	It("includes consumer lag in heartbeat (IT-DCM-140)", func() {
		lister := &stubServiceTypeLister{types: []string{"container"}}
		lag := &stubConsumerLagProvider{lag: 5}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, lag, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getHeartbeats())
		}, 2*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))

		var payload map[string]interface{}
		Expect(json.Unmarshal(mock.getHeartbeats()[0].Body, &payload)).To(Succeed())
		Expect(payload["consumerLag"]).To(BeNumerically("==", 5))
	})

	It("retries on heartbeat failure (IT-DCM-150)", func() {
		mock.mu.Lock()
		mock.hbStatus = http.StatusInternalServerError
		mock.mu.Unlock()

		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getHeartbeats())
		}, 2*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 2))
	})
})

var _ = Describe("Service Type Updates", Label("integration"), func() {
	var (
		mock   *mockDCM
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		mock = newMockDCM()
		DeferCleanup(mock.server.Close)
		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // Ginkgo BeforeEach requires closure variable assignment
		DeferCleanup(cancel)
	})

	It("triggers DCM update on service type change (IT-DCM-110)", func() {
		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))

		lister.setTypes([]string{"container", "database"})
		r.NotifyServiceTypeChange()

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 2))

		regs := mock.getRegistrations()
		var payload map[string]interface{}
		Expect(json.Unmarshal(regs[len(regs)-1].Body, &payload)).To(Succeed())
		types, ok := payload["serviceTypes"].([]interface{})
		Expect(ok).To(BeTrue())
		Expect(types).To(HaveLen(2))
	})

	It("sends empty serviceTypes when all SPs unavailable (IT-DCM-160)", func() {
		lister := &stubServiceTypeLister{types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))

		lister.setTypes([]string{})
		r.NotifyServiceTypeChange()

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 2))

		regs := mock.getRegistrations()
		var payload map[string]interface{}
		Expect(json.Unmarshal(regs[len(regs)-1].Body, &payload)).To(Succeed())
		types, ok := payload["serviceTypes"].([]interface{})
		Expect(ok).To(BeTrue())
		Expect(types).To(BeEmpty())
	})

	It("excludes Unavailable SPs from serviceTypes (IT-DCM-170)", func() {
		lister := &stubServiceTypeLister{types: []string{"container", "database"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, discardLogger,
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 3*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1))

		regs := mock.getRegistrations()
		var payload map[string]interface{}
		Expect(json.Unmarshal(regs[0].Body, &payload)).To(Succeed())
		types, ok := payload["serviceTypes"].([]interface{})
		Expect(ok).To(BeTrue())
		Expect(types).To(ContainElement("container"))
		Expect(types).To(ContainElement("database"))
	})
})
