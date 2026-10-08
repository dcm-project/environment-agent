package kubernetes_test

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var _ = Describe("K8s Store", func() {
	Describe("Health Check", func() {
		// CheckHealth succeeds with reachable fake client
		It("succeeds when the K8s API server is reachable (TC-I116)", func() {
			s, _, _ := newTestStore(defaultConfig())

			err := s.CheckHealth(context.Background())
			Expect(err).NotTo(HaveOccurred())
		})

		// CheckHealth returns error when Discovery fails
		It("returns error when Discovery endpoint fails (TC-I117)", func() {
			s, client, _ := newTestStore(defaultConfig())

			fakeClient, ok := client.(*fake.Clientset)
			Expect(ok).To(BeTrue())

			(*fakeClient).PrependReactor("get", "version", func(_ k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, fmt.Errorf("simulated discovery failure")
			})

			err := s.CheckHealth(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("simulated discovery failure"))
		})
	})
})
