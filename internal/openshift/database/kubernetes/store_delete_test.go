package kubernetes_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	cnpgstore "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

func verifyClusterDeleted(client *dynamic.Interface, namespace, name string) {
	_, err := (*client).Resource(clustersGvr).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	Expect(err).To(HaveOccurred())
	Expect(apierrors.IsNotFound(err)).To(BeTrue())
}

var _ = Describe("K8s Store", func() {
	Describe("Delete Operations", func() {
		var s *cnpgstore.CnpgDatabaseStore
		var client *kubernetes.Interface
		var dynClient *dynamic.Interface

		BeforeEach(func() {
			client = new(kubernetes.Interface)
			dynClient = new(dynamic.Interface)
			s, *client, *dynClient = newTestStore(defaultConfig())
		})

		// Delete removes Cluster and associated Secret
		It("removes Cluster and associated Secret", func() {
			// Pre-create Cluster and Secret
			err := createFakeCluster(dynClient, "default", "my-db", "abc-123", withUser("application"), withCredentialsSecret("my-db-credentials"))
			Expect(err).NotTo(HaveOccurred())
			err = createFakeSecret(client, "default", "my-db-credentials", "abc-123", withUsername("application"), withPassword("pass"))
			Expect(err).NotTo(HaveOccurred())

			err = s.Delete(context.Background(), "abc-123")
			Expect(err).NotTo(HaveOccurred())

			// Verify Cluster is deleted
			verifyClusterDeleted(dynClient, "default", "my-db")
			// Verify Secret is deleted
			_, secretErr := (*client).CoreV1().Secrets("default").Get(context.Background(), "my-db-credentials", metav1.GetOptions{})
			Expect(secretErr).To(HaveOccurred())
			Expect(apierrors.IsNotFound(secretErr)).To(BeTrue())

			// Verify subsequent Get returns not-found
			_, getErr := s.Get(context.Background(), "abc-123")
			Expect(store.IsNotFound(getErr)).To(BeTrue())
		})

		// Delete succeeds when no Secret exists
		It("succeeds when no Secret exists", func() {
			// Pre-create only Cluster, no Secret
			err := createFakeCluster(dynClient, "default", "my-db", "abc-123")
			Expect(err).NotTo(HaveOccurred())

			err = s.Delete(context.Background(), "abc-123")
			Expect(err).NotTo(HaveOccurred())

			// Verify Cluster is deleted
			verifyClusterDeleted(dynClient, "default", "my-db")
		})

		// Delete returns conflict when multiple Clusters share the same instance ID
		It("returns conflict when multiple Clusters share the same instance ID", func() {
			// Create two Clusters with the same instance ID but different names
			err := createFakeCluster(dynClient, "default", "app-one", "dup-id")
			Expect(err).NotTo(HaveOccurred())
			err = createFakeCluster(dynClient, "default", "app-two", "dup-id")
			Expect(err).NotTo(HaveOccurred())

			err = s.Delete(context.Background(), "dup-id")

			Expect(store.IsConflict(err)).To(BeTrue(), "expected ConflictError, got: %v", err)

			// Verify neither cluster was deleted
			clusterList, listErr := (*dynClient).Resource(clustersGvr).Namespace("default").List(context.Background(), metav1.ListOptions{})
			Expect(listErr).NotTo(HaveOccurred())
			Expect(clusterList.Items).To(HaveLen(2))
		})

		// Delete returns not-found for non-existent database
		It("returns not-found for non-existent database", func() {
			err := s.Delete(context.Background(), "xyz-999")

			Expect(store.IsNotFound(err)).To(BeTrue(), "expected NotFoundError, got: %v", err)
		})
	})
})
