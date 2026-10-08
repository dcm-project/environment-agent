package kubernetes_test

import (
	"context"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	cnpgstore "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	"github.com/dcm-project/environment-agent/internal/ptr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var _ = Describe("K8s Store", func() {
	Describe("Create Operations", func() {
		var s *cnpgstore.CnpgDatabaseStore
		var client *kubernetes.Interface
		var dynClient *dynamic.Interface

		BeforeEach(func() {
			client = new(kubernetes.Interface)
			dynClient = new(dynamic.Interface)
			s, *client, *dynClient = newTestStore(defaultConfig())
		})

		// Create produces a Cluster with replicas=1 by default
		It("produces a Cluster with replicas=1", func() {
			d := minimalDatabase(clusterName)

			result, err := s.Create(context.Background(), d, testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			// Verify Cluster was created with replicas=1
			cluster, err := getCreatedCluster(dynClient, testNS)
			Expect(err).NotTo(HaveOccurred())
			Expect(cluster.Spec.Instances).To(Equal(1))

			// Verify read-only fields are populated
			Expect(result.Id).NotTo(BeNil())
			Expect(result.Status).NotTo(BeNil())
			Expect(result.CreateTime).NotTo(BeNil())
		})

		// Created Cluster and carries DCM labels
		It("carries DCM labels on Cluster", func() {
			d := minimalDatabase(clusterName)

			_, err := s.Create(context.Background(), d, testId)
			Expect(err).NotTo(HaveOccurred())

			cluster, err := getCreatedCluster(dynClient, testNS)
			Expect(err).NotTo(HaveOccurred())

			expectedLabels := map[string]string{
				"dcm.project/managed-by":       "dcm",
				"dcm.project/dcm-instance-id":  testId,
				"dcm.project/dcm-service-type": "database",
			}

			for k, v := range expectedLabels {
				Expect(cluster.Labels).To(HaveKeyWithValue(k, v))
			}
		})

		// Cluster uses the specified version
		It("uses the specified database version", func() {
			d := minimalDatabase(clusterName)
			d.Version = ptr.To(v1alpha1.DatabaseVersion(18))

			_, err := s.Create(context.Background(), d, testId)
			Expect(err).NotTo(HaveOccurred())

			cluster, err := getCreatedCluster(dynClient, testNS)
			Expect(err).NotTo(HaveOccurred())
			Expect(cluster.Spec.ImageName).To(Equal("ghcr.io/cloudnative-pg/postgresql:18-standard-trixie"))
		})

		// CPU resources map to Kubernetes requests and limits
		It("maps CPU resources to requests and limits", func() {
			d := minimalDatabase(clusterName)
			d.Resources.Cpu = v1alpha1.DatabaseCpu{Min: "1", Max: "2000m"}

			_, err := s.Create(context.Background(), d, testId)
			Expect(err).NotTo(HaveOccurred())

			cluster, err := getCreatedCluster(dynClient, testNS)
			Expect(err).NotTo(HaveOccurred())
			Expect(cluster.Spec.Resources.Requests.Cpu().String()).To(Equal("1"))
			Expect(cluster.Spec.Resources.Limits.Cpu().String()).To(Equal("2"))
		})

		// Memory resources convert and map correctly
		It("converts and maps memory resources correctly", func() {
			d := minimalDatabase(clusterName)
			d.Resources.Memory = v1alpha1.DatabaseMemory{Min: "1GB", Max: "2GB"}

			_, err := s.Create(context.Background(), d, testId)
			Expect(err).NotTo(HaveOccurred())

			cluster, err := getCreatedCluster(dynClient, testNS)
			Expect(err).NotTo(HaveOccurred())
			Expect(cluster.Spec.Resources.Requests.Memory().String()).To(Equal("1Gi"))
			Expect(cluster.Spec.Resources.Limits.Memory().String()).To(Equal("2Gi"))
		})

		// Creates an additional managed service when visibility=external
		It("Creates an additional managed service when visibility=external", func() {
			d := databaseWithExternalService(clusterName)

			_, err := s.Create(context.Background(), d, testId)
			Expect(err).NotTo(HaveOccurred())

			cluster, err := getCreatedCluster(dynClient, testNS)
			Expect(err).NotTo(HaveOccurred())
			Expect(cluster.Spec.Managed).NotTo(BeNil())
			Expect(cluster.Spec.Managed.Services).NotTo(BeNil())
			Expect(len(cluster.Spec.Managed.Services.Additional)).To(Equal(1))
			svc := cluster.Spec.Managed.Services.Additional[0]
			Expect(svc.SelectorType).To(Equal(cnpgv1.ServiceSelectorTypeRW))
			// go-client does not provide full handling of generate name, thus, the template can only inherit the base name
			Expect(svc.ServiceTemplate.ObjectMeta.Name).To(Equal(fmt.Sprintf("%s-external-%s", clusterName, cnpgv1.ServiceSelectorTypeRW)))
			Expect(svc.ServiceTemplate.Spec.Type).To(Equal(corev1.ServiceTypeLoadBalancer))
		})

		// Optional fields omitted when not provided
		It("omits optional fields when not provided", func() {
			d := minimalDatabase(clusterName)

			_, err := s.Create(context.Background(), d, testId)
			Expect(err).NotTo(HaveOccurred())

			cluster, err := getCreatedCluster(dynClient, testNS)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cluster.Spec.Managed.Services.Additional)).To(Equal(0))
			Expect(cluster.Spec.Bootstrap.InitDB).NotTo(BeNil())
			Expect(*cluster.Spec.Bootstrap.InitDB).To(Equal(cnpgv1.BootstrapInitDB{}))
			Expect(cluster.Spec.ImageCatalogRef).To(BeNil())
		})

		// Create rejects duplicate dcm-instance-id
		It("rejects duplicate dcm-instance-id", func() {
			// Pre-create a Cluster with the target instance ID
			err := createFakeCluster(dynClient, testNS, "existing-app", "existing-id")
			Expect(err).NotTo(HaveOccurred())

			// Attempt to create with the same ID but different name
			d := minimalDatabase("different-name")
			_, err = s.Create(context.Background(), d, "existing-id")

			var conflictErr *store.ConflictError
			Expect(errors.As(err, &conflictErr)).To(BeTrue(), "expected ConflictError, got: %v", err)
		})

		// Create applies user-specified metadata.labels
		It("applies user-specified metadata labels", func() {
			d := minimalDatabase(clusterName)
			labels := map[string]string{"env": "staging", "team": "platform"}
			d.Metadata.Labels = &labels

			_, err := s.Create(context.Background(), d, testId)
			Expect(err).NotTo(HaveOccurred())

			cluster, err := getCreatedCluster(dynClient, testNS)
			Expect(err).NotTo(HaveOccurred())

			// User labels present
			Expect(cluster.Labels).To(HaveKeyWithValue("env", "staging"))
			Expect(cluster.Labels).To(HaveKeyWithValue("team", "platform"))
			// DCM labels also present
			Expect(cluster.Labels).To(HaveKeyWithValue("dcm.project/managed-by", "dcm"))
			Expect(cluster.Labels).To(HaveKeyWithValue("dcm.project/dcm-instance-id", testId))
		})

		// Create populates image catalog reference when present
		It("populates image catalog reference when present", func() {
			cfg := defaultConfig()
			cfg.ImageCatalogName = "my-catalog"
			s, _, *dynClient = newTestStore(cfg)

			d := minimalDatabase(clusterName)

			_, err := s.Create(context.Background(), d, "test-image-ref")
			Expect(err).NotTo(HaveOccurred())

			cluster, err := getCreatedCluster(dynClient, testNS)
			Expect(err).NotTo(HaveOccurred())

			Expect(cluster.Spec.ImageCatalogRef).NotTo(BeNil())
			Expect(cluster.Spec.ImageCatalogRef.Major).To(Equal(18))
			Expect(*cluster.Spec.ImageCatalogRef.APIGroup).To(Equal("postgresql.cnpg.io"))
			Expect(cluster.Spec.ImageCatalogRef.Kind).To(Equal(cnpgv1.ClusterImageCatalogKind))
			Expect(cluster.Spec.ImageCatalogRef.Name).To(Equal("my-catalog"))
		})
	})
})
