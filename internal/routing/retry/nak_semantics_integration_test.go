package retry_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/messaging"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/routing"
	"github.com/dcm-project/environment-agent/internal/routing/retry"
	"github.com/dcm-project/environment-agent/internal/routing/routingtest"
)

// Verifies the real-JetStream property a fake-consumer test cannot: that
// FetchRetryMessages' NakFunc (Nak-in-place, per AC-RTE-080) actually causes
// real redelivery with an incrementing NumDelivered, rather than losing the
// message or resetting it to a fresh delivery. This is the load-bearing
// assumption behind DD-410's decision not to cap the retry-subject
// consumer's MaxDeliver.
var _ = Describe("Retry-Topic Nak-In-Place Semantics", Label("integration"), func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		testConn  *nats.Conn
		testJS    jetstream.JetStream
		topicName string
		topics    messaging.TopicNames
	)

	BeforeEach(func() {
		topicName = fmt.Sprintf("nak-semantics-test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("agent-prod-1", topicName)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		var err error
		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())

		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topics.RetryStream(), Subjects: []string{topics.Retry},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topics.RetryStream(), jetstream.ConsumerConfig{
			Durable: topics.RetryConsumer(), AckPolicy: jetstream.AckExplicitPolicy,
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
		_ = testJS.DeleteStream(context.Background(), topics.RetryStream())
		testConn.Close()
	})

	It("increments NumDelivered on redelivery after Nak-in-place, instead of losing or resetting the message (IT-RCM-095)", func() {
		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry:      provider.NewRegistry(),
			HealthTracker: provider.NewInMemoryHealthTracker(),
			Store:         routingtest.NewFakeStore(),
			Publisher:     &routingtest.NATSPublisher{JS: testJS},
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      routing.NewResourceSet(100),
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			Topics:        topics,
		})

		ceBytes := routingtest.BuildCreateCE("res-nak-in-place", "database")
		_, err := testJS.Publish(ctx, topics.Retry, ceBytes)
		Expect(err).NotTo(HaveOccurred())

		msgs, err := processor.FetchRetryMessages(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(msgs).To(HaveLen(1))
		Expect(msgs[0].ResourceID).To(Equal("res-nak-in-place"))

		// The production NakFunc closure (msg.NakWithDelay), not a fake —
		// proves the real Nak call, not just the decision to invoke one.
		Expect(msgs[0].NakFunc()).To(Succeed())

		// storeErrorRetryDelay (10s, unexported in retry.Processor) governs
		// redelivery timing; poll with a generous margin past it.
		cons, err := testJS.Consumer(ctx, topics.RetryStream(), topics.RetryConsumer())
		Expect(err).NotTo(HaveOccurred())

		var redelivered jetstream.Msg
		Eventually(func() error {
			batch, fetchErr := cons.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
			if fetchErr != nil {
				return fetchErr
			}
			for m := range batch.Messages() {
				redelivered = m
			}
			if redelivered == nil {
				return errors.New("message not yet redelivered")
			}
			return nil
		}, 20*time.Second, 500*time.Millisecond).Should(Succeed())

		meta, err := redelivered.Metadata()
		Expect(err).NotTo(HaveOccurred())
		Expect(meta.NumDelivered).To(Equal(uint64(2)),
			"Nak-in-place must increment delivery count, not reset it to 1 (DD-410's load-bearing assumption)")
		Expect(redelivered.Data()).To(Equal(ceBytes), "redelivery must be the same message, not a republished duplicate")

		Expect(redelivered.Ack()).To(Succeed())
	})
})
