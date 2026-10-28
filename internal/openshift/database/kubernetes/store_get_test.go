package kubernetes_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
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
		It("returns database with runtime data from Cluster, Pod, Service, and Secret", func() {
			// Pre-create Deployment, Pod, and Service
			err := createFakeCluster(dynClient, clusterName, testId, withUser("apps"), withCredentialsSecret(clusterName+"-credentials"),
				withDatabaseName(testDB), withMajorVersion(16), withArtifacts(client, testId), withReplicas(client, defaultConfig().DefaultStorageClass, testId))
			Expect(err).NotTo(HaveOccurred())
			err = createFakeSecret(client, testNS, clusterName+"-credentials", testId, withUsername("apps"), withPassword(testPassword))
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			// Status from Pod phase
			Expect(result.Status).NotTo(BeNil())
			Expect(*result.Status).To(Equal(v1alpha1.RUNNING))

			// Connection string
			Expect(result.Spec.Network).NotTo(BeNil())
			hostPort := net.JoinHostPort(testInternalAddress, "5432")
			Expect(*result.Spec.ConnectionString).To(Equal(fmt.Sprintf("postgresql://%s:%s@%s/%s", "apps", testPassword, hostPort, testDB)))
		})

		// Get recovers the original metadata name by stripping the instance-ID suffix from the cluster name
		It("recovers original metadata name by stripping the instance-ID suffix", func() {
			caseId := "jkl-987"
			// Cluster name follows the {metadata.name}-{id} convention used by buildCluster
			realClusterName := clusterName + "-" + caseId
			err := createFakeCluster(dynClient, realClusterName, caseId,
				withMajorVersion(18), withArtifacts(client, caseId), withReplicas(client, defaultConfig().DefaultStorageClass, caseId))
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), caseId)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Spec.Metadata.Name).To(Equal(clusterName))
			Expect(result.Spec.Metadata.Name).NotTo(Equal(realClusterName))
		})

		// Get populates storage from the cluster storage configuration in API units
		It("populates storage from cluster storage configuration", func() {
			err := createFakeCluster(dynClient, clusterName, testId,
				withStorage("10Gi"), withMajorVersion(18), withArtifacts(client, testId),
				withReplicas(client, defaultConfig().DefaultStorageClass, testId))
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Spec.Resources.Storage).To(Equal("10GB"))
		})

		// Get returns PENDING status when no Pod exists
		It("returns PENDING status when no Pod exists", func() {
			// Pre-create only Cluster, no Pod
			err := createFakeCluster(dynClient, clusterName, testId, withMajorVersion(18), withArtifacts(client, testId))
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

		// Get populates connectionString from NodePort service using a node address and the allocated node port
		It("populates connectionString from NodePort service with node address", func() {
			err := createFakeCluster(dynClient, clusterName, testId, withExternalNodePortVisibility(client, testId), withDatabaseName(testDB),
				withMajorVersion(18), withArtifacts(client, testId), withReplicas(client, defaultConfig().DefaultStorageClass, testId))
			Expect(err).NotTo(HaveOccurred())
			err = createFakeNode(client, "node-1", testNodeAddress)
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())

			hostPort := net.JoinHostPort(testNodeAddress, strconv.FormatInt(testNodePort, 10))
			Expect(*result.Spec.ConnectionString).To(Equal(fmt.Sprintf("postgresql://app:%s@%s/%s", testPassword, hostPort, testDB)))
		})

		// Get returns empty connectionString for NodePort service when no node is available
		It("returns empty connectionString for NodePort service without node", func() {
			err := createFakeCluster(dynClient, clusterName, testId, withExternalNodePortVisibility(client, testId), withDatabaseName(testDB),
				withMajorVersion(18), withArtifacts(client, testId), withReplicas(client, defaultConfig().DefaultStorageClass, testId))
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())

			Expect(*result.Spec.ConnectionString).To(Equal(""))
		})

		// Get populates connectionString from LoadBalancer status
		It("populates connectionString from LoadBalancer status", func() {
			err := createFakeCluster(dynClient, clusterName, testId, withExternalVisibility(client, testId), withDatabaseName(testDB),
				withMajorVersion(18), withArtifacts(client, testId), withReplicas(client, defaultConfig().DefaultStorageClass, testId))
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())

			hostPort := net.JoinHostPort(testExternalAddress, "5432")
			Expect(*result.Spec.ConnectionString).To(Equal(fmt.Sprintf("postgresql://app:%s@%s/%s", testPassword, hostPort, testDB)))
		})

		// Get encodes delimiter characters in password and database name
		It("encodes delimiter characters in password and database name", func() {
			specialPassword := "p@ss:w/ord#1?"
			specialDB := "my/db#name"
			err := createFakeCluster(dynClient, clusterName, testId, withUser(testUser), withCredentialsSecret(clusterName+"-credentials"),
				withDatabaseName(specialDB), withMajorVersion(18), withArtifacts(client, testId), withReplicas(client, defaultConfig().DefaultStorageClass, testId))
			Expect(err).NotTo(HaveOccurred())
			err = createFakeSecret(client, testNS, clusterName+"-credentials", testId, withUsername(testUser), withPassword(specialPassword))
			Expect(err).NotTo(HaveOccurred())

			result, err := (*s).Get(context.Background(), testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			connStr := *result.Spec.ConnectionString
			parsed, parseErr := url.Parse(connStr)
			Expect(parseErr).NotTo(HaveOccurred())
			Expect(parsed.Scheme).To(Equal("postgresql"))
			parsedPassword, _ := parsed.User.Password()
			Expect(parsed.User.Username()).To(Equal(testUser))
			Expect(parsedPassword).To(Equal(specialPassword))
			Expect(parsed.Path).To(Equal("/" + specialDB))
		})

		// Get returns an error when the credentials secret is missing the password field
		It("returns an error when the credentials secret has no password", func() {
			err := createFakeCluster(dynClient, clusterName, testId, withUser(testUser), withCredentialsSecret(clusterName+"-credentials"),
				withMajorVersion(18), withArtifacts(client, testId), withReplicas(client, defaultConfig().DefaultStorageClass, testId))
			Expect(err).NotTo(HaveOccurred())
			// Create a secret with username only — no password field
			err = createFakeSecret(client, testNS, clusterName+"-credentials", testId, withUsername(testUser))
			Expect(err).NotTo(HaveOccurred())

			_, err = (*s).Get(context.Background(), testId)
			Expect(store.IsInvalidArgument(err)).To(BeTrue(), "expected InvalidArgumentError, got: %v", err)
			Expect(err.Error()).To(ContainSubstring("password"))
		})

		// Get populates update_time from Pod condition transition
		It("populates update_time from Pod condition transition", func() {
			transitionTime := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
			err := createFakeCluster(dynClient, clusterName, testId, withMajorVersion(18), withArtifacts(client, testId),
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
			err := createFakeCluster(dynClient, clusterName, testId,
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

		// Get returns conflict when pod and PVC identities do not match (counts equal but labels wrong)
		It("returns conflict when pod and PVC identities do not match", func() {
			err := createFakeCluster(dynClient, clusterName, testId, withMajorVersion(18),
				withMismatchedReplica(client, defaultConfig().DefaultStorageClass, testId))
			Expect(err).NotTo(HaveOccurred())

			_, err = (*s).Get(context.Background(), testId)

			Expect(store.IsConflict(err)).To(BeTrue(), "expected ConflictError, got: %v", err)
		})

		// Get returns conflict when multiple Clusters share the same instance ID
		It("returns conflict when multiple Clusters share the same instance ID", func() {
			// Create two Clusters with the same instance ID but different names
			err := createFakeCluster(dynClient, clusterName, testId, withMajorVersion(18))
			Expect(err).NotTo(HaveOccurred())
			err = createFakeCluster(dynClient, clusterName+"-2", testId, withMajorVersion(18))
			Expect(err).NotTo(HaveOccurred())

			_, err = (*s).Get(context.Background(), testId)

			Expect(store.IsConflict(err)).To(BeTrue(), "expected ConflictError, got: %v", err)
		})
	})
})
