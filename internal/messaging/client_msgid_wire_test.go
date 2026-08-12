package messaging_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/messaging"
)

// This file exercises Client.PublishWithMsgID against the suite's real
// embedded NATS/JetStream server, asserting the Nats-Msg-Id header and dedup
// behavior (REQ-XC-CE-050, REQ-MSG-135/150) at the wire level.
var _ = Describe("PublishWithMsgID wire-level behavior", Label("integration"), func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		testConn   *nats.Conn
		testJS     jetstream.JetStream
		subject    string
		streamName string
		client     *messaging.Client
	)

	BeforeEach(func() {
		var err error
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern
		suffix := uuid.New().String()[:8]
		subject = fmt.Sprintf("dcm.agents.responses.msgid-test-%s", suffix)
		streamName = fmt.Sprintf("msgid-test-%s", suffix)

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())

		// A dedicated per-test stream+subject (rather than the suite-shared
		// "dcm-agent-responses" stream) keeps the dedup-window assertion
		// below deterministic and isolated from other tests' publishes.
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:       streamName,
			Subjects:   []string{subject},
			Duplicates: 30 * time.Second,
		})
		Expect(err).NotTo(HaveOccurred())

		client = messaging.NewClient(messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: fmt.Sprintf("wiretest-%s", suffix),
			AgentName: "wire-test-agent",
		}, slog.Default())
		setNoopHandlers(client)
		Expect(client.Start(ctx)).To(Succeed())
	})

	AfterEach(func() {
		client.Stop()
		_ = testJS.DeleteStream(context.Background(), streamName)
		cancel()
		testConn.Close()
	})

	It("sets the wire-level Nats-Msg-Id header to the caller's msgID (IT-MSG-160)", func() {
		msgID := "wire-msg-id-" + uuid.New().String()
		// Start is non-blocking (AC-MSG-050): retry until doSetup has
		// populated the client's jetstream handle. Failed attempts return
		// before reaching the server, so no duplicate publish occurs.
		Eventually(func() error {
			return client.PublishWithMsgID(ctx, subject, msgID, []byte(`{"hello":"world"}`))
		}, 5*time.Second).Should(Succeed())

		cons, err := testJS.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			AckPolicy: jetstream.AckNonePolicy,
		})
		Expect(err).NotTo(HaveOccurred())
		batch, err := cons.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		Expect(err).NotTo(HaveOccurred())

		var received jetstream.Msg
		for m := range batch.Messages() {
			received = m
		}
		Expect(received).NotTo(BeNil(), "expected to receive the published message from the real stream")
		Expect(received.Headers().Get(jetstream.MsgIDHeader)).To(Equal(msgID),
			"the raw NATS message on the wire must carry a Nats-Msg-Id header equal to the caller's msgID")
	})

	It("lets JetStream's server-side dedup drop a second publish with the same Nats-Msg-Id (IT-MSG-161)", func() {
		msgID := "dedup-msg-id-" + uuid.New().String()

		Eventually(func() error {
			return client.PublishWithMsgID(ctx, subject, msgID, []byte(`{"attempt":1}`))
		}, 5*time.Second).Should(Succeed())
		Expect(client.PublishWithMsgID(ctx, subject, msgID, []byte(`{"attempt":2}`))).To(Succeed())

		stream, err := testJS.Stream(ctx, streamName)
		Expect(err).NotTo(HaveOccurred())
		info, err := stream.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.State.Msgs).To(Equal(uint64(1)),
			"JetStream's server-side dedup (keyed on the wire-level Nats-Msg-Id header) must drop "+
				"the second publish sharing the same msg ID within the dedup window")
	})
})
