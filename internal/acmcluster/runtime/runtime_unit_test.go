package runtime_test

import (
	"context"
	"net/http"

	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/runtime"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("PrepareConfig", func() {
	It("returns error when cfg is nil", func() {
		err := runtime.PrepareConfig(nil)
		Expect(err).To(MatchError("config is required"))
	})

	It("derives pull secret name and default version matrix", func() {
		cfg := &config.Config{
			Registration: config.RegistrationConfig{ProviderName: "acm-cluster-sp"},
			Cluster:      config.ClusterConfig{},
		}

		Expect(runtime.PrepareConfig(cfg)).To(Succeed())
		Expect(cfg.Cluster.PullSecretName).To(Equal("acm-cluster-sp-pull-secret"))
		Expect(cfg.Cluster.VersionMatrix).NotTo(BeEmpty())
	})
})

var _ = Describe("New", func() {
	It("returns error when PullSecretName is empty", func() {
		cfg := &config.Config{
			Registration: config.RegistrationConfig{ProviderName: "acm-cluster-sp"},
			Cluster:      config.ClusterConfig{},
		}

		_, err := runtime.New(context.Background(), cfg, nil, runtime.Options{})
		Expect(err).To(MatchError("cluster pull secret name is empty"))
	})

	It("uses injected Kubernetes client when monitor and registration are disabled", func() {
		cfg := &config.Config{
			Registration: config.RegistrationConfig{ProviderName: "acm-cluster-sp"},
			Cluster: config.ClusterConfig{
				ClusterNamespace: "clusters",
				PullSecret:       "eyJhdXRocyI6eyJjbG91ZC5vcGVuc2hpZnQuY29tIjp7ImF1dGgiOiJkR1Z6ZEE9PSJ9fX0=",
			},
		}
		Expect(runtime.PrepareConfig(cfg)).To(Succeed())

		scheme, err := util.BuildScheme()
		Expect(err).NotTo(HaveOccurred())
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()

		rt, err := runtime.New(context.Background(), cfg, nil, runtime.Options{
			KubernetesClient:    k8sClient,
			DisableMonitor:      true,
			DisableRegistration: true,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(rt.ClusterService()).NotTo(BeNil())
		Expect(rt.HealthChecker()).NotTo(BeNil())
		Expect(rt.Close()).To(Succeed())
	})

	It("requires RestConfig when Kubernetes client is injected and monitor is enabled", func() {
		cfg := &config.Config{
			Registration: config.RegistrationConfig{ProviderName: "acm-cluster-sp"},
			Cluster: config.ClusterConfig{
				ClusterNamespace: "clusters",
				PullSecret:       "eyJhdXRocyI6eyJjbG91ZC5vcGVuc2hpZnQuY29tIjp7ImF1dGgiOiJkR1Z6ZEE9PSJ9fX0=",
			},
			Monitoring: config.MonitoringConfig{NATSUrl: "nats://127.0.0.1:4222"},
		}
		Expect(runtime.PrepareConfig(cfg)).To(Succeed())

		scheme, err := util.BuildScheme()
		Expect(err).NotTo(HaveOccurred())
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()

		_, err = runtime.New(context.Background(), cfg, nil, runtime.Options{
			KubernetesClient: k8sClient,
		})
		Expect(err).To(MatchError("RestConfig is required when KubernetesClient is set and status monitor is enabled"))
	})
})

var _ = Describe("LoadConfig embedded", func() {
	It("defaults NATS URL from fallback and provider name", func() {
		GinkgoT().Setenv("SP_CLUSTER_NAMESPACE", "clusters")
		GinkgoT().Setenv("SP_PULL_SECRET", "c2VjcmV0")
		GinkgoT().Setenv("SP_NATS_URL", "")

		cfg, err := runtime.LoadConfig(true, "nats://agent:4222")
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Monitoring.NATSUrl).To(Equal("nats://agent:4222"))
		Expect(cfg.Registration.ProviderName).To(Equal("acm-cluster-sp"))
	})

	It("does not require SP_ENDPOINT or DCM_REGISTRATION_URL", func() {
		GinkgoT().Setenv("SP_CLUSTER_NAMESPACE", "clusters")
		GinkgoT().Setenv("SP_PULL_SECRET", "c2VjcmV0")

		_, err := runtime.LoadConfig(true, "nats://agent:4222")
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("MapOperationError", func() {
	It("maps ALREADY_EXISTS to 409", func() {
		opErr := runtime.MapOperationError(service.NewAlreadyExistsError("cluster exists"))
		Expect(opErr).NotTo(BeNil())
		Expect(opErr.StatusCode).To(Equal(http.StatusConflict))
		Expect(opErr.Message).To(Equal("cluster exists"))
	})
})
