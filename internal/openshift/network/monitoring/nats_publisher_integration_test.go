package monitoring_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/network/monitoring"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NATSPublisher", func() {
	It("publishes a CloudEvent to dcm.network on a real NATS server", func() {
		ns := startEmbeddedNATSServer()
		DeferCleanup(ns.Shutdown)

		url := ns.ClientURL()
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		publisher, err := monitoring.NewNATSPublisher(url, testProviderName, logger)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = publisher.Close() })

		nc, err := nats.Connect(url)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(nc.Close)

		received := make(chan []byte, 1)
		_, err = nc.Subscribe("dcm.network", func(msg *nats.Msg) {
			received <- msg.Data
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(nc.Flush()).To(Succeed())

		err = publisher.Publish(context.Background(), monitoring.StatusEvent{
			InstanceID: "net-real-nats",
			Status:     v1alpha1.READY,
			Message:    "service is ready",
		})
		Expect(err).NotTo(HaveOccurred())

		var payload []byte
		Eventually(received, 2*time.Second).Should(Receive(&payload))

		var ce map[string]any
		Expect(json.Unmarshal(payload, &ce)).To(Succeed())
		Expect(ce).To(HaveKeyWithValue("type", "dcm.status.network"))
		Expect(ce).To(HaveKeyWithValue("subject", "dcm.network"))
		Expect(ce).To(HaveKeyWithValue("source", "dcm/providers/"+testProviderName))

		ceData, ok := ce["data"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(ceData).To(HaveKeyWithValue("id", "net-real-nats"))
		Expect(ceData).To(HaveKeyWithValue("status", string(v1alpha1.READY)))
	})
})
