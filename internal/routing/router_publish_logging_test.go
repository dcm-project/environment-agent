package routing_test

import (
	"context"
	"errors"
	"log/slog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/routing"
	"github.com/dcm-project/environment-agent/internal/routing/routingtest"
)

// fakePublisher is a minimal Publisher stub letting tests force a publish
// failure deterministically, without a real NATS connection.
type fakePublisher struct {
	publishErr error
}

func (p *fakePublisher) Publish(context.Context, string, []byte) error { return nil }

func (p *fakePublisher) PublishWithMsgID(context.Context, string, string, []byte) error {
	return p.publishErr
}

// Router.publishCE (called from every outcome-CE call site, e.g. HandleCancel's
// CancelAcked) is exercised here via HandleCancel — the cheapest public path
// that reaches it without requiring a registered SP or real NATS/JetStream.
var _ = Describe("CloudEvent publish outcome logging", Label("unit"), func() {
	var (
		ch  *captureLogHandler
		pub *fakePublisher
	)

	newRouterWithPublisher := func() *routing.Router {
		return routing.NewRouter(routing.RouterDeps{
			Registry:      provider.NewRegistry(),
			HealthTracker: provider.NewInMemoryHealthTracker(),
			Store:         routingtest.NewFakeStore(),
			Publisher:     pub,
			DenyList:      routing.NewResourceSet(100),
			Logger:        slog.New(ch),
			AgentName:     "agent-prod-1",
		})
	}

	BeforeEach(func() {
		ch = &captureLogHandler{}
	})

	It("logs INFO with ce_type and resource_id when the publish succeeds (IT-RTE-140)", func() {
		pub = &fakePublisher{}
		router := newRouterWithPublisher()

		err := router.HandleCancel(context.Background(), routingtest.BuildCancelCE("res-pub-ok", "database"))
		Expect(err).NotTo(HaveOccurred())

		rec := ch.last()
		Expect(rec.Message).To(Equal("published CE"))
		Expect(rec.Level).To(Equal(slog.LevelInfo))
		v, ok := attrValue(rec, "ce_type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal(cloudevent.TypeCancelAcked))
		v, ok = attrValue(rec, "resource_id")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("res-pub-ok"))
	})

	It("logs WARN with ce_type, resource_id, and error when the publish fails (IT-RTE-140)", func() {
		pub = &fakePublisher{publishErr: errors.New("nats: no responders available for request")}
		router := newRouterWithPublisher()

		err := router.HandleCancel(context.Background(), routingtest.BuildCancelCE("res-pub-fail", "database"))
		Expect(err).NotTo(HaveOccurred(), "a publish failure must not itself be treated as a HandleCancel error")

		rec := ch.last()
		Expect(rec.Message).To(Equal("failed to publish CE"))
		Expect(rec.Level).To(Equal(slog.LevelWarn))
		v, ok := attrValue(rec, "ce_type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal(cloudevent.TypeCancelAcked))
		v, ok = attrValue(rec, "resource_id")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("res-pub-fail"))
		_, ok = attrValue(rec, "error")
		Expect(ok).To(BeTrue())
	})
})
