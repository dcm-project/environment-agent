package monitoring_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/monitoring"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NATSPublisher", func() {
	It("publishes a CloudEvent to dcm.storage on a real NATS server", func() {
		opts := &server.Options{
			Host:   "127.0.0.1",
			Port:   -1,
			NoLog:  true,
			NoSigs: true,
		}
		ns, err := server.NewServer(opts)
		Expect(err).NotTo(HaveOccurred())
		go ns.Start()
		DeferCleanup(ns.Shutdown)
		Expect(ns.ReadyForConnections(5 * time.Second)).To(BeTrue())

		url := ns.ClientURL()
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		publisher, err := monitoring.NewNATSPublisher(url, "k8s-storage-sp", logger)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = publisher.Close() })

		nc, err := nats.Connect(url)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(nc.Close)

		received := make(chan []byte, 1)
		_, err = nc.Subscribe("dcm.storage", func(msg *nats.Msg) {
			received <- msg.Data
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(nc.Flush()).To(Succeed())

		err = publisher.Publish(context.Background(), monitoring.StatusEvent{
			InstanceID: "vol-real-nats",
			Status:     v1alpha1.RUNNING,
			Message:    "PVC is bound",
		})
		Expect(err).NotTo(HaveOccurred())

		var payload []byte
		Eventually(received, 2*time.Second).Should(Receive(&payload))

		var ce map[string]any
		Expect(json.Unmarshal(payload, &ce)).To(Succeed())
		Expect(ce).To(HaveKeyWithValue("type", "dcm.status.storage"))
		Expect(ce).To(HaveKeyWithValue("subject", "dcm.storage"))
		Expect(ce).To(HaveKeyWithValue("source", "dcm/providers/k8s-storage-sp"))

		ceData, ok := ce["data"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(ceData).To(HaveKeyWithValue("id", "vol-real-nats"))
		Expect(ceData).To(HaveKeyWithValue("status", "RUNNING"))
	})
})
