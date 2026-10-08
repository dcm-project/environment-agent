package kubernetes_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	cnpgstore "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var _ = Describe("K8s Store", func() {
	Describe("Get Operations", func() {
		var s *cnpgstore.CnpgDatabaseStore
		var client *kubernetes.Interface
		var dynClient *dynamic.Interface

		BeforeEach(func() {
			client = new(kubernetes.Interface)
			dynClient = new(dynamic.Interface)
			s, *client, *dynClient = newTestStore(defaultConfig())
		})

		// Get returns database with runtime data from Cluster, Pod, Service, and Secret
		It("returns container with runtime data from Cluster, Pod, Service, and Secret", func() {
			// Pre-create Deployment, Pod, and Service
			err := createFakeCluster(dynClient, testNS, clusterName, testId, withUser(testUser), withCredentialsSecret(clusterName+"-credentials"),
				withDatabaseName(testDB), withMajorVersion(18), withArtifacts(client, testId), withReplicas(client, defaultConfig().DefaultStorageClass, testId))
			Expect(err).NotTo(HaveOccurred())
			err = createFakeSecret(client, testNS, clusterName+"-credentials", testId, withUsername(testUser), withPassword(testPassword))
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			// Status from Pod phase
			Expect(result.Status).NotTo(BeNil())
			Expect(*result.Status).To(Equal(v1alpha1.RUNNING))

			// Connection string
			Expect(result.Spec.Network).NotTo(BeNil())
			Expect(*result.Spec.ConnectionString).To(Equal(fmt.Sprintf("postgresql://%s:%s@%s:5432/%s", testUser, testPassword, testInternalAddress, testDB)))
		})

		// Get returns PENDING status when no Pod exists
		It("returns PENDING status when no Pod exists", func() {
			// Pre-create only Cluster, no Pod
			err := createFakeCluster(dynClient, testNS, clusterName, testId, withMajorVersion(18), withArtifacts(client, testId))
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Status).NotTo(BeNil())
			Expect(*result.Status).To(Equal(v1alpha1.PENDING))
		})

		// Get returns not-found for non-existent database
		It("returns not-found for non-existent database", func() {
			_, err := (*s).Get(context.Background(), "xyz-999")

			Expect(store.IsNotFound(err)).To(BeTrue(), "expected NotFoundError, got: %v", err)
		})

		// Get populates connectionString from LoadBalancer status
		It("populates connectionString from LoadBalancer status", func() {
			err := createFakeCluster(dynClient, testNS, clusterName, testId, withExternalVisibility(client, testId), withDatabaseName(testDB),
				withMajorVersion(18), withArtifacts(client, testId), withReplicas(client, defaultConfig().DefaultStorageClass, testId))
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())

			Expect(*result.Spec.ConnectionString).To(Equal(fmt.Sprintf("postgresql://%s:%s@%s:5432/%s", "app", testPassword, testExternalAddress, testDB)))
		})

		// Get populates update_time from Pod condition transition
		It("populates update_time from Pod condition transition", func() {
			transitionTime := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
			err := createFakeCluster(dynClient, testNS, clusterName, testId, withMajorVersion(18), withArtifacts(client, testId),
				withReplicas(client, defaultConfig().DefaultStorageClass, testId, withPodConditions([]corev1.PodCondition{
					{
						Type:               corev1.PodReady,
						Status:             corev1.ConditionTrue,
						LastTransitionTime: metav1.NewTime(transitionTime),
					},
				}),
				))
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.UpdateTime).NotTo(BeNil())
			Expect(result.UpdateTime.UTC()).To(Equal(transitionTime))
		})

		// Get populates update_time from Cluster condition when no Pod
		It("populates update_time from Cluster condition when no Pod", func() {
			transitionTime := time.Date(2026, 2, 18, 9, 0, 0, 0, time.UTC)
			err := createFakeCluster(dynClient, testNS, clusterName, testId,
				withClusterConditions([]metav1.Condition{
					{
						Type:               string(cnpgv1.ClusterReady),
						Status:             metav1.ConditionTrue,
						LastTransitionTime: metav1.NewTime(transitionTime),
					},
				}),
				withMajorVersion(18),
				withArtifacts(client, testId),
			)
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.UpdateTime).NotTo(BeNil())
			Expect(result.UpdateTime.UTC()).To(Equal(transitionTime))
		})

		// Get returns conflict when multiple Clusters share the same instance ID
		It("returns conflict when multiple Clusters share the same instance ID", func() {
			// Create two Clusters with the same instance ID but different names
			err := createFakeCluster(dynClient, testNS, clusterName, testId, withMajorVersion(18))
			Expect(err).NotTo(HaveOccurred())
			err = createFakeCluster(dynClient, testNS, clusterName+"-2", testId, withMajorVersion(18))
			Expect(err).NotTo(HaveOccurred())

			_, err = (*s).Get(context.Background(), testId)

			Expect(store.IsConflict(err)).To(BeTrue(), "expected ConflictError, got: %v", err)
		})
	})
})
