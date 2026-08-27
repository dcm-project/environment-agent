package kubernetes_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	k8sstore "github.com/dcm-project/environment-agent/internal/openshift/storage/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var _ = Describe("K8s Store", func() {
	Describe("Health Check", func() {
		It("succeeds when the K8s API server is reachable", func() {
			client := fake.NewClientset()
			s := k8sstore.NewK8sVolumeStore(client, k8sstore.K8sConfig{Namespace: "default"}, slog.New(slog.NewJSONHandler(io.Discard, nil)))

			err := s.CheckHealth(context.Background())
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when discovery fails", func() {
			client := fake.NewClientset()
			s := k8sstore.NewK8sVolumeStore(client, k8sstore.K8sConfig{Namespace: "default"}, slog.New(slog.NewJSONHandler(io.Discard, nil)))

			client.PrependReactor("get", "version", func(_ k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, fmt.Errorf("simulated discovery failure")
			})

			err := s.CheckHealth(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("simulated discovery failure"))
		})
	})
})
