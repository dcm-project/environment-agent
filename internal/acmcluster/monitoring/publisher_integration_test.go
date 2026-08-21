package monitoring_test

import (
	"context"
	"log/slog"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NATSPublisher — Integration Tests", func() {
	It("TC-MON-IT-012: NATS unavailability does not block SP startup", func() {
		// RFC 5737 TEST-NET address — guaranteed unroutable, avoids CI port collisions
		publisher, err := monitoring.NewNATSPublisher(
			"nats://192.0.2.1:4222", testProviderName, slog.Default())
		Expect(err).NotTo(HaveOccurred())
		Expect(publisher).NotTo(BeNil())

		// With RetryOnFailedConnect(true), the connection is in RECONNECTING state.
		// Publish buffers the message internally (up to ReconnectBufSize, default 8MB).
		// It will NOT error for a single small message — it silently buffers.
		ctx := context.Background()
		err = publisher.Publish(ctx, monitoring.StatusEvent{
			InstanceID: "test-instance",
			Status:     v1alpha1.ClusterStatusREADY,
			Message:    "",
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(publisher.Close()).To(Succeed())
	})
})
