package baremetal_test

import (
	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster/baremetal"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster/clustertest"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const testNamespace = clustertest.TestNamespace

func defaultConfig() config.ClusterConfig {
	cfg := clustertest.DefaultConfig()
	cfg.AgentNamespace = "agent-ns"
	cfg.InfraEnvLabelKey = "infraenvs.agent-install.openshift.io"
	return cfg
}

func newTestService(cfg config.ClusterConfig) (*baremetal.Service, client.Client) {
	c := clustertest.BuildFakeClient(nil, nil)
	return baremetal.New(c, cfg), c
}

func newTestServiceWithInterceptor(cfg config.ClusterConfig, objs []client.Object, fns interceptor.Funcs) (*baremetal.Service, client.Client) {
	c := clustertest.BuildFakeClient(objs, &fns)
	return baremetal.New(c, cfg), c
}

// ── Request fixtures ────────────────────────────────────────────────

func validCreateCluster() v1alpha1.Cluster {
	platform := v1alpha1.Baremetal
	return v1alpha1.Cluster{
		Spec: v1alpha1.ClusterSpec{
			Version:     "1.30",
			ServiceType: v1alpha1.ClusterSpecServiceTypeCluster,
			Metadata: v1alpha1.ClusterMetadata{
				Name: "test-cluster",
			},
			Nodes: &v1alpha1.ClusterNodes{
				ControlPlane: &v1alpha1.ControlPlaneSpec{
					Count:   util.Ptr(v1alpha1.N3),
					Cpu:     util.Ptr(4),
					Memory:  util.Ptr("16GB"),
					Storage: util.Ptr("120GB"),
				},
				Workers: &v1alpha1.WorkerSpec{
					Count:   util.Ptr(3),
					Cpu:     util.Ptr(8),
					Memory:  util.Ptr("32GB"),
					Storage: util.Ptr("500GB"),
				},
			},
			ProviderHints: &v1alpha1.ProviderHints{
				Acm: &v1alpha1.ACMProviderHints{
					Platform: &platform,
					InfraEnv: util.Ptr("my-infra"),
				},
			},
		},
	}
}
