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
		regBody:   `{"agent_id":"agent-123"}`,
		hbStatus:  http.StatusOK,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1alpha1/agents", m.handleRegistration)
	mux.HandleFunc("PUT /api/v1alpha1/agents/{agentId}/heartbeat", m.handleHeartbeat)
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

// panicNTimesLister panics on its first n calls, then behaves like a normal
// lister, simulating a transiently-broken dependency.
type panicNTimesLister struct {
	mu    sync.Mutex
	calls int
	n     int
	types []string
}

func (p *panicNTimesLister) AdvertisableServiceTypes() []string {
	p.mu.Lock()
	p.calls++
	calls := p.calls
	p.mu.Unlock()
	if calls <= p.n {
		panic("simulated transient lister panic")
	}
	cp := make([]string, len(p.types))
	copy(cp, p.types)
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

// captureHandler records slog records for assertion.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *captureHandler) WithGroup(string) slog.Handler            { return h }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) all() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

func findRecord(records []slog.Record, msg string) (slog.Record, bool) {
	for _, r := range records {
		if r.Message == msg {
			return r, true
		}
	}
	return slog.Record{}, false
}

func findRecords(records []slog.Record, msg string) []slog.Record {
	var out []slog.Record
	for _, r := range records {
		if r.Message == msg {
			out = append(out, r)
		}
	}
	return out
}

func recordAttr(rec slog.Record, key string) (slog.Value, bool) {
	var v slog.Value
	var found bool
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v, found = a.Value, true
			return false
		}
		return true
	})
	return v, found
}

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
		Expect(payload).To(HaveKey("service_types"))
	})

	It("has no agent_id before registration (IT-DCM-015)", func() {
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

		// Companion: Eventually agent_id becomes non-empty — fails on stub → RED
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
		types, ok := payload["service_types"].([]interface{})
		Expect(ok).To(BeTrue())
		Expect(types).To(HaveLen(2))
	})

	It("sends correct registration payload (IT-DCM-050)", func() {
		cfg := dcm.RegistrarConfig{
			AgentName:   "agent-prod-1",
			Environment: "production",
			Cost:        "medium",
			// Realistic value: production always passes the CP-prefixed
			// subject (messaging.TopicNames.Main), never the bare base name
			// — see cmd/environment-agent/main.go wiring.
			TopicName:         "dcm.agent.agent-prod-1",
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
		Expect(payload["topic_name"]).To(Equal("dcm.agent.agent-prod-1"))
		Expect(payload).To(HaveKey("service_types"))
	})

	It("includes resources_available when available (IT-DCM-060)", func() {
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
		Expect(payload).To(HaveKey("resources_available"))
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

	It("recovers from a panic in the registrar goroutine without crashing the process, "+
		"and keeps the goroutine alive to make forward progress afterward (IT-DCM-180)", func() {
		cfg := defaultRegistrarConfig(mock.server.URL)
		lister := &panicNTimesLister{n: 3, types: []string{"container"}}
		r, err := dcm.NewRegistrar(cfg, lister, &stubConsumerLagProvider{}, nil, discardLogger)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(mock.getRegistrations())
		}, 8*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1),
			"registration must eventually succeed once the lister stops panicking — proving the "+
				"registrar goroutine survived 3 recovered panics and kept retrying rather than "+
				"exiting after the first one")

		Consistently(r.Done(), 200*time.Millisecond).ShouldNot(BeClosed(),
			"Done() must NOT close merely because a panic was recovered — the goroutine must "+
				"remain alive (e.g. in its heartbeat loop) afterward")

		cancel()
		Eventually(r.Done(), 5*time.Second).Should(BeClosed(),
			"Done() must still close promptly on a real shutdown (context cancellation)")
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
			Expect(hb.Path).To(Equal("/api/v1alpha1/agents/agent-123/heartbeat"))
			var payload map[string]interface{}
			Expect(json.Unmarshal(hb.Body, &payload)).To(Succeed())
			Expect(payload).To(HaveKey("timestamp"))
			Expect(payload).To(HaveKey("consumer_lag"))
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

	It("sends strictly increasing heartbeat payload timestamps (IT-DCM-135)", func() {
		// Unlike IT-DCM-130, this asserts on the `timestamp` field inside the
		// heartbeat body itself, which the control plane requires to be
		// strictly increasing.
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
		payloadTimestamps := make([]time.Time, 0, len(hbs))
		for _, hb := range hbs {
			var payload struct {
				Timestamp time.Time `json:"timestamp"`
			}
			Expect(json.Unmarshal(hb.Body, &payload)).To(Succeed())
			payloadTimestamps = append(payloadTimestamps, payload.Timestamp)
		}
		for i := 1; i < len(payloadTimestamps); i++ {
			Expect(payloadTimestamps[i]).To(BeTemporally(">", payloadTimestamps[i-1]),
				"heartbeat payload timestamps must be strictly increasing (CP rejects timestamp <= last recorded)")
		}
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
		Expect(payload["consumer_lag"]).To(BeNumerically("==", 5))
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
		types, ok := payload["service_types"].([]interface{})
		Expect(ok).To(BeTrue())
		Expect(types).To(HaveLen(2))
	})

	It("sends empty service_types when all SPs unavailable (IT-DCM-160)", func() {
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
		types, ok := payload["service_types"].([]interface{})
		Expect(ok).To(BeTrue())
		Expect(types).To(BeEmpty())
	})

	It("excludes Unavailable SPs from service_types (IT-DCM-170)", func() {
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
		types, ok := payload["service_types"].([]interface{})
		Expect(ok).To(BeTrue())
		Expect(types).To(ContainElement("container"))
		Expect(types).To(ContainElement("database"))
	})
})

var _ = Describe("DCM Registrar Lifecycle Logging", Label("integration"), func() {
	var (
		mock   *mockDCM
		ctx    context.Context
		cancel context.CancelFunc
		ch     *captureHandler
	)

	BeforeEach(func() {
		mock = newMockDCM()
		DeferCleanup(mock.server.Close)
		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // Ginkgo BeforeEach requires closure variable assignment
		DeferCleanup(cancel)
		ch = &captureHandler{}
	})

	It("logs startup, heartbeat success, and re-registration success (IT-DCM-190)", func() {
		lister := &stubServiceTypeLister{types: []string{"container"}}
		lag := &stubConsumerLagProvider{lag: 7}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, lag, nil, slog.New(ch),
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() bool {
			_, ok := findRecord(ch.all(), "DCM registrar starting")
			return ok
		}, 2*time.Second, 20*time.Millisecond).Should(BeTrue())

		Eventually(func() bool {
			_, ok := findRecord(ch.all(), "heartbeat succeeded")
			return ok
		}, 2*time.Second, 20*time.Millisecond).Should(BeTrue())
		hbRec, _ := findRecord(ch.all(), "heartbeat succeeded")
		Expect(hbRec.Level).To(Equal(slog.LevelDebug))
		v, ok := recordAttr(hbRec, "agent_id")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("agent-123"))
		v, ok = recordAttr(hbRec, "consumer_lag")
		Expect(ok).To(BeTrue())
		Expect(v.Int64()).To(Equal(int64(7)))

		lister.setTypes([]string{"container", "database"})
		r.NotifyServiceTypeChange()

		Eventually(func() bool {
			_, ok := findRecord(ch.all(), "re-registered with DCM")
			return ok
		}, 2*time.Second, 20*time.Millisecond).Should(BeTrue())
		reRegRec, _ := findRecord(ch.all(), "re-registered with DCM")
		Expect(reRegRec.Level).To(Equal(slog.LevelInfo))
		v, ok = recordAttr(reRegRec, "agent_id")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("agent-123"))
	})

	It("distinguishes a post-panic restart from a fresh start in the startup log (IT-DCM-195)", func() {
		// panicNTimesLister panics once from inside run(), forcing the
		// supervisor to recover and restart run() — the resulting second
		// "DCM registrar starting" log line must cross-reference the
		// restart via restart_attempt so operators can tell it apart from
		// the initial startup log.
		lister := &panicNTimesLister{n: 1, types: []string{"container"}}
		r, err := dcm.NewRegistrar(
			defaultRegistrarConfig(mock.server.URL),
			lister, &stubConsumerLagProvider{}, nil, slog.New(ch),
		)
		Expect(err).NotTo(HaveOccurred())

		r.Start(ctx)

		Eventually(func() int {
			return len(findRecords(ch.all(), "DCM registrar starting"))
		}, 3*time.Second, 20*time.Millisecond).Should(Equal(2),
			"expected exactly one fresh-start log and one restart log")

		startRecs := findRecords(ch.all(), "DCM registrar starting")
		Expect(startRecs).To(HaveLen(2))

		_, hasAttr := recordAttr(startRecs[0], "restart_attempt")
		Expect(hasAttr).To(BeFalse(), "the initial startup log must not carry restart_attempt")

		v, hasAttr := recordAttr(startRecs[1], "restart_attempt")
		Expect(hasAttr).To(BeTrue(), "the post-panic restart log must carry restart_attempt")
		Expect(v.Int64()).To(BeNumerically(">=", 1))

		Eventually(func() bool {
			_, ok := findRecord(ch.all(), "panic in DCM registrar goroutine, restarting")
			return ok
		}, 2*time.Second, 20*time.Millisecond).Should(BeTrue())
		panicRec, _ := findRecord(ch.all(), "panic in DCM registrar goroutine, restarting")
		v, hasAttr = recordAttr(panicRec, "restart_attempt")
		Expect(hasAttr).To(BeTrue(), "the panic log should also carry restart_attempt for cross-reference")
		Expect(v.Int64()).To(Equal(int64(1)))
	})
})
