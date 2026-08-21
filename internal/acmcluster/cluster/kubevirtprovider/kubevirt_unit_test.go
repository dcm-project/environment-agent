package kubevirtprovider_test

import (
	"context"
	"errors"
	"fmt"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var _ = Describe("KubeVirt Service", func() {
	var (
		ctx context.Context
		cfg config.ClusterConfig
	)

	BeforeEach(func() {
		ctx = context.Background()
		cfg = defaultConfig()
	})

	// ── Create ──────────────────────────────────────────────────────────

	Describe("Create", func() {
		It("TC-KV-UT-001: creates HostedCluster + NodePool with correct platform, labels, and replicas", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()
			req.Spec.Nodes.Workers.Count = util.Ptr(2)

			result, err := svc.Create(ctx, "test-id", req)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			// MF-2: UpdateTime must equal CreateTime on creation
			Expect(result.CreateTime).NotTo(BeNil())
			Expect(result.UpdateTime).NotTo(BeNil())
			Expect(*result.UpdateTime).To(Equal(*result.CreateTime))

			// Verify HostedCluster was created
			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			hc := hcList.Items[0]
			Expect(hc.Spec.Platform.Type).To(Equal(hyperv1.KubevirtPlatform))
			Expect(hc.Labels).To(HaveKeyWithValue("dcm.project/managed-by", "dcm"))
			Expect(hc.Labels).To(HaveKeyWithValue("dcm.project/dcm-instance-id", "test-id"))
			Expect(hc.Labels).To(HaveKeyWithValue("dcm.project/dcm-service-type", "cluster"))

			// Verify NodePool was created
			var npList hyperv1.NodePoolList
			Expect(k8s.List(ctx, &npList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(npList.Items).To(HaveLen(1))
			np := npList.Items[0]
			Expect(np.Spec.Replicas).NotTo(BeNil())
			Expect(*np.Spec.Replicas).To(Equal(int32(2)))
			Expect(np.Spec.Platform.Type).To(Equal(hyperv1.KubevirtPlatform))
			Expect(np.Labels).To(HaveKeyWithValue("dcm.project/managed-by", "dcm"))
			Expect(np.Labels).To(HaveKeyWithValue("dcm.project/dcm-service-type", "cluster"))
		})

		It("TC-KV-UT-002: control_plane.count and storage are ignored", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()
			req.Spec.Nodes.ControlPlane.Count = util.Ptr(v1alpha1.N5)
			req.Spec.Nodes.ControlPlane.Storage = util.Ptr("500GB")

			result, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			hc := hcList.Items[0]
			Expect(hc.Spec.ControllerAvailabilityPolicy).To(BeZero())
		})

		It("TC-KV-UT-003: control_plane CPU and memory map to resource request override annotations", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()
			req.Spec.Nodes.ControlPlane.Cpu = util.Ptr(4)
			req.Spec.Nodes.ControlPlane.Memory = util.Ptr("16GB")

			result, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			hc := hcList.Items[0]

			// Verify kube-apiserver resource request override annotation
			kasKey := hyperv1.ResourceRequestOverrideAnnotationPrefix + "/kube-apiserver.kube-apiserver"
			Expect(hc.Annotations).To(HaveKeyWithValue(kasKey, "cpu=4,memory=16G"))

			// Verify etcd resource request override annotation
			etcdKey := hyperv1.ResourceRequestOverrideAnnotationPrefix + "/etcd.etcd"
			Expect(hc.Annotations).To(HaveKeyWithValue(etcdKey, "cpu=4,memory=16G"))

			// No other resource-request-override annotations
			for k := range hc.Annotations {
				if k != kasKey && k != etcdKey {
					Expect(k).NotTo(HavePrefix(hyperv1.ResourceRequestOverrideAnnotationPrefix))
				}
			}
		})

		DescribeTable("TC-KV-UT-004: version validation via compatibility matrix and ClusterImageSets",
			func(version string, expectSuccess bool, expectedErrType *v1alpha1.ErrorType) {
				svc, _ := newTestService(cfg)
				req := validCreateCluster()
				req.Spec.Version = version

				result, err := svc.Create(ctx, "test-id", req)

				if expectSuccess {
					Expect(err).NotTo(HaveOccurred())
					Expect(result).NotTo(BeNil())
				} else {
					Expect(err).To(HaveOccurred())
					var domainErr *service.DomainError
					Expect(errors.As(err, &domainErr)).To(BeTrue())
					Expect(domainErr.Type).To(Equal(*expectedErrType))
				}
			},
			Entry("valid version 1.28", "1.28", true, nil),
			Entry("no K8s-to-OCP mapping (9.99)", "9.99", false, util.Ptr(v1alpha1.ErrorTypeUNPROCESSABLEENTITY)),
			Entry("partial K8s version (1.3)", "1.3", false, util.Ptr(v1alpha1.ErrorTypeUNPROCESSABLEENTITY)),
			Entry("OCP format rejected (4.17.0)", "4.17.0", false, util.Ptr(v1alpha1.ErrorTypeUNPROCESSABLEENTITY)),
			Entry("valid version 1.30", "1.30", true, nil),
			Entry("no ClusterImageSet for 1.31", "1.31", false, util.Ptr(v1alpha1.ErrorTypeUNPROCESSABLEENTITY)),
		)

		It("TC-KV-UT-005: release image override bypasses ClusterImageSet lookup", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()
			platform := v1alpha1.Kubevirt
			req.Spec.ProviderHints = &v1alpha1.ProviderHints{
				Acm: &v1alpha1.ACMProviderHints{
					Platform:     &platform,
					ReleaseImage: util.Ptr("quay.io/ocp-release:4.15.2-x86_64"),
				},
			}

			result, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			Expect(hcList.Items[0].Spec.Release.Image).To(Equal("quay.io/ocp-release:4.15.2-x86_64"))
		})

		It("TC-KV-UT-006: base domain from request overrides config default", func() {
			cfgWithDomain := cfg
			cfgWithDomain.BaseDomain = "cluster.local"
			svc, k8s := newTestService(cfgWithDomain)

			// Case 1: Request overrides config
			req := validCreateCluster()
			req.Spec.ProviderHints = &v1alpha1.ProviderHints{
				Acm: &v1alpha1.ACMProviderHints{
					BaseDomain: util.Ptr("example.com"),
				},
			}
			result, err := svc.Create(ctx, "test-id-1", req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			Expect(hcList.Items[0].Spec.DNS.BaseDomain).To(Equal("example.com"))

			// Case 2: Falls back to config default (separate test invocation)
			svc2, k8s2 := newTestService(cfgWithDomain)
			req2 := validCreateCluster()
			req2.Spec.Metadata.Name = "test-cluster-2"
			result2, err2 := svc2.Create(ctx, "test-id-2", req2)
			Expect(err2).NotTo(HaveOccurred())
			Expect(result2).NotTo(BeNil())

			var hcList2 hyperv1.HostedClusterList
			Expect(k8s2.List(ctx, &hcList2, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList2.Items).To(HaveLen(1))
			Expect(hcList2.Items[0].Spec.DNS.BaseDomain).To(Equal("cluster.local"))
		})

		It("TC-KV-UT-007: default platform is KubeVirt when no provider_hints", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()
			req.Spec.ProviderHints = nil

			result, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			Expect(hcList.Items[0].Spec.Platform.Type).To(Equal(hyperv1.KubevirtPlatform))
		})

		It("TC-KV-UT-008: memory/storage format conversion (DCM to K8s)", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()
			req.Spec.Nodes.Workers.Memory = util.Ptr("16GB")
			req.Spec.Nodes.Workers.Storage = util.Ptr("120GB")

			result, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			var npList hyperv1.NodePoolList
			Expect(k8s.List(ctx, &npList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(npList.Items).To(HaveLen(1))
			np := npList.Items[0]
			Expect(np.Spec.Platform.Kubevirt).NotTo(BeNil())
			Expect(np.Spec.Platform.Kubevirt.Compute).NotTo(BeNil())
			Expect(np.Spec.Platform.Kubevirt.Compute.Memory.String()).To(Equal("16G"))
		})

		It("TC-KV-UT-009: workers storage maps to root disk size", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()
			req.Spec.Nodes.Workers.Storage = util.Ptr("120GB")

			result, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			var npList hyperv1.NodePoolList
			Expect(k8s.List(ctx, &npList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(npList.Items).To(HaveLen(1))
			np := npList.Items[0]
			Expect(np.Spec.Platform.Kubevirt).NotTo(BeNil())
			Expect(np.Spec.Platform.Kubevirt.RootVolume).NotTo(BeNil())
			Expect(np.Spec.Platform.Kubevirt.RootVolume.Persistent).NotTo(BeNil())
			Expect(np.Spec.Platform.Kubevirt.RootVolume.Persistent.Size.String()).To(Equal("120G"))
		})

		It("TC-KV-UT-014: duplicate ID returns AlreadyExists error", func() {
			existing := buildHostedCluster("existing", testNamespace, withDCMLabels("abc123"))
			svc, _ := newTestService(cfg, existing)
			req := validCreateCluster()

			_, err := svc.Create(ctx, "abc123", req)

			Expect(err).To(HaveOccurred())
			var domainErr *service.DomainError
			Expect(errors.As(err, &domainErr)).To(BeTrue())
			Expect(domainErr.Type).To(Equal(v1alpha1.ErrorTypeALREADYEXISTS))
		})

		It("TC-KV-UT-015: duplicate metadata.name returns AlreadyExists with name conflict detail", func() {
			existing := buildHostedCluster("my-cluster", testNamespace)
			svc, _ := newTestService(cfg, existing)
			req := validCreateCluster()
			req.Spec.Metadata.Name = "my-cluster"

			_, err := svc.Create(ctx, "new-id", req)

			Expect(err).To(HaveOccurred())
			var domainErr *service.DomainError
			Expect(errors.As(err, &domainErr)).To(BeTrue())
			Expect(domainErr.Type).To(Equal(v1alpha1.ErrorTypeALREADYEXISTS))
			// SF-2: Error message must indicate this is a name conflict
			Expect(domainErr.Message).To(ContainSubstring("name"))
		})

		It("TC-KV-UT-016: K8s API failure returns internal error without leaking details", func() {
			svc, _ := newTestServiceWithInterceptor(cfg, nil, interceptor.Funcs{
				Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
					return fmt.Errorf("kube-apiserver: connection refused")
				},
			})
			req := validCreateCluster()

			_, err := svc.Create(ctx, "test-id", req)
			Expect(err).To(HaveOccurred())
			var domainErr *service.DomainError
			Expect(errors.As(err, &domainErr)).To(BeTrue())
			Expect(domainErr.Type).To(Equal(v1alpha1.ErrorTypeINTERNAL))
			Expect(domainErr.Message).NotTo(ContainSubstring("kube"))
		})

		It("TC-KV-UT-017: NodePool creation failure triggers HostedCluster rollback", func() {
			svc, k8s := newTestServiceWithInterceptor(cfg, nil, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*hyperv1.NodePool); ok {
						return fmt.Errorf("simulated NP failure")
					}
					return c.Create(ctx, obj, opts...)
				},
			})
			req := validCreateCluster()

			_, err := svc.Create(ctx, "test-id", req)

			Expect(err).To(HaveOccurred())
			var domainErr *service.DomainError
			Expect(errors.As(err, &domainErr)).To(BeTrue())
			Expect(domainErr.Type).To(Equal(v1alpha1.ErrorTypeINTERNAL))

			// Verify orphan HC was cleaned up
			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(BeEmpty())
		})

		It("TC-KV-UT-020: missing base_domain returns error", func() {
			cfgNoDomain := cfg
			cfgNoDomain.BaseDomain = ""
			svc, _ := newTestService(cfgNoDomain)
			req := validCreateCluster()
			req.Spec.ProviderHints = nil // No base_domain in request either

			_, err := svc.Create(ctx, "test-id", req)

			Expect(err).To(HaveOccurred())
			var domainErr *service.DomainError
			Expect(errors.As(err, &domainErr)).To(BeTrue())
			Expect(domainErr.Type).To(Equal(v1alpha1.ErrorTypeINVALIDARGUMENT))
		})

		It("TC-KV-UT-028: rollback failure returns combined error", func() {
			svc, _ := newTestServiceWithInterceptor(cfg, nil, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*hyperv1.NodePool); ok {
						return fmt.Errorf("simulated NP failure")
					}
					return c.Create(ctx, obj, opts...)
				},
				Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
					return fmt.Errorf("simulated delete failure")
				},
			})
			req := validCreateCluster()

			_, err := svc.Create(ctx, "test-id", req)
			Expect(err).To(HaveOccurred())
			var domainErr *service.DomainError
			Expect(errors.As(err, &domainErr)).To(BeTrue())
			Expect(domainErr.Type).To(Equal(v1alpha1.ErrorTypeINTERNAL))
			// Error should contain both failures
			Expect(err.Error()).To(ContainSubstring("rollback"))
		})

		It("TC-KV-UT-029: empty platform string defaults to KubeVirt", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()
			emptyPlatform := v1alpha1.ACMProviderHintsPlatform("")
			req.Spec.ProviderHints = &v1alpha1.ProviderHints{
				Acm: &v1alpha1.ACMProviderHints{
					Platform: &emptyPlatform,
				},
			}

			result, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			Expect(hcList.Items[0].Spec.Platform.Type).To(Equal(hyperv1.KubevirtPlatform))
		})

		// ── Resource Identity (TC-XC-ID-UT-xxx) ─────────────────────────

		It("TC-XC-ID-UT-001: resources have both K8s name and DCM instance ID", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()
			req.Spec.Metadata.Name = "my-cluster"

			result, err := svc.Create(ctx, "dcm-id", req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			hc := hcList.Items[0]
			Expect(hc.Name).To(Equal("my-cluster"))
			Expect(hc.Labels).To(HaveKeyWithValue("dcm.project/dcm-instance-id", "dcm-id"))

			var npList hyperv1.NodePoolList
			Expect(k8s.List(ctx, &npList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(npList.Items).To(HaveLen(1))
			Expect(npList.Items[0].Labels).To(HaveKeyWithValue("dcm.project/dcm-instance-id", "dcm-id"))
		})

		It("TC-XC-ID-UT-002: dcm-instance-id label matches id field for lookup", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()

			_, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())

			// Verify the HC can be found by label
			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList,
				client.InNamespace(testNamespace),
				client.MatchingLabels{"dcm.project/dcm-instance-id": "test-id"},
			)).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
		})

		It("TC-KV-UT-030: HostedCluster has exactly 4 service publishing strategies", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()

			_, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())

			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			hc := hcList.Items[0]
			Expect(hc.Spec.Services).To(HaveLen(4))
			Expect(hc.Spec.Services).To(ContainElement(HaveField("Service", hyperv1.OAuthServer)))
			Expect(hc.Spec.Services).To(ContainElement(HaveField("Service", hyperv1.Konnectivity)))
			Expect(hc.Spec.Services).To(ContainElement(HaveField("Service", hyperv1.Ignition)))
			Expect(hc.Spec.Services).To(ContainElement(HaveField("Service", hyperv1.APIServer)))
		})

		It("TC-KV-UT-032: HostedCluster PullSecret references shared Secret", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()

			_, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())

			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			Expect(hcList.Items[0].Spec.PullSecret.Name).To(Equal(cfg.PullSecretName))
		})

		It("TC-KV-UT-033: NodePool has Management.UpgradeType=InPlace", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()

			_, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())

			var npList hyperv1.NodePoolList
			Expect(k8s.List(ctx, &npList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(npList.Items).To(HaveLen(1))
			Expect(npList.Items[0].Spec.Management.UpgradeType).To(Equal(hyperv1.UpgradeTypeInPlace))
		})

		It("TC-KV-UT-034: HostedCluster has Etcd Managed with PersistentVolume storage", func() {
			svc, k8s := newTestService(cfg)
			req := validCreateCluster()

			_, err := svc.Create(ctx, "test-id", req)
			Expect(err).NotTo(HaveOccurred())

			var hcList hyperv1.HostedClusterList
			Expect(k8s.List(ctx, &hcList, client.InNamespace(testNamespace))).To(Succeed())
			Expect(hcList.Items).To(HaveLen(1))
			hc := hcList.Items[0]
			Expect(hc.Spec.Etcd.ManagementType).To(Equal(hyperv1.Managed))
			Expect(hc.Spec.Etcd.Managed).NotTo(BeNil())
			Expect(hc.Spec.Etcd.Managed.Storage.Type).To(Equal(hyperv1.PersistentVolumeEtcdStorage))
		})
	})
})
