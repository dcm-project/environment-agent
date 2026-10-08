package kubernetes_test

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	cnpgstore "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	k8stesting "k8s.io/client-go/testing"
)

var _ = Describe("K8s Store", func() {
	Describe("Conflict & Namespace", func() {
		var s *cnpgstore.CnpgDatabaseStore
		var client *kubernetes.Interface
		var dynClient *dynamic.Interface

		BeforeEach(func() {
			client = new(kubernetes.Interface)
			dynClient = new(dynamic.Interface)
			s, *client, *dynClient = newTestStore(defaultConfig())
		})

		// GenerateName allows same metadata name with different IDs
		It("allows same metadata name with different instance IDs", func() {
			// Pre-create a Cluster with name "web-app"
			err := createFakeCluster(dynClient, testNS, clusterName, testId)
			Expect(err).NotTo(HaveOccurred())

			// Create with the same metadata name but different ID — succeeds
			// because GenerateName produces a unique Cluster name.
			c := minimalDatabase(clusterName)
			_, err = s.Create(context.Background(), c, "different-id")
			Expect(err).NotTo(HaveOccurred())

			// Verify both Clusters exist
			clusterList, listErr := (*dynClient).Resource(clustersGvr).Namespace(testNS).List(context.Background(), metav1.ListOptions{})
			Expect(listErr).NotTo(HaveOccurred())
			Expect(clusterList.Items).To(HaveLen(2))
		})

		// All resources created in the configured namespace
		It("creates all resources in the configured namespace", func() {
			cfg := cnpgstore.CnpgK8sConfig{
				Namespace:           "production",
				ExternalServiceType: "LoadBalancer",
			}
			s, *client, *dynClient = newTestStore(cfg)
			c := minimalDatabase(clusterName, dbWithPassword(testPassword))

			_, err := s.Create(context.Background(), c, testId)
			Expect(err).NotTo(HaveOccurred())

			// Verify Cluster is in "production" namespace
			cluster, err := getCreatedCluster(dynClient, "production")
			Expect(err).NotTo(HaveOccurred())
			Expect(cluster.Namespace).To(Equal("production"))

			// Verify Service is in "production" namespace
			secret, err := getCreatedSecret(client, "production")
			Expect(err).NotTo(HaveOccurred())
			Expect(secret.Namespace).To(Equal("production"))
		})

		// Unexpected K8s API error produces internal store error
		It("produces internal store error on unexpected K8s API error", func() {
			// Inject a K8s API error on Clusters creation
			fakeClient, ok := (*dynClient).(*dynfake.FakeDynamicClient)
			Expect(ok).To(BeTrue())

			fakeClient.PrependReactor("create", "clusters", func(_ k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewInternalError(fmt.Errorf("etcd cluster unavailable"))
			})

			c := minimalDatabase(clusterName)
			_, err := s.Create(context.Background(), c, testId)
			Expect(err).To(HaveOccurred())

			// Error must NOT be a typed store error
			Expect(store.IsNotFound(err)).To(BeFalse(), "error should not be NotFoundError")
			Expect(store.IsConflict(err)).To(BeFalse(), "error should not be ConflictError")

			// Error should contain K8s API error detail (reactor was triggered)
			Expect(err.Error()).To(ContainSubstring("etcd cluster unavailable"))
		})
	})
})
