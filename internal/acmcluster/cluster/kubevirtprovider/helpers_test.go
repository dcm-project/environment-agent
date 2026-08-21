package kubevirtprovider_test

import (
	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster/clustertest"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster/kubevirtprovider"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const testNamespace = clustertest.TestNamespace

func defaultConfig() config.ClusterConfig {
	return clustertest.DefaultConfig()
}

func newTestService(cfg config.ClusterConfig, objs ...client.Object) (*kubevirtprovider.Service, client.Client) {
	c := clustertest.BuildFakeClient(objs, nil)
	return kubevirtprovider.New(c, cfg), c
}

func newTestServiceWithInterceptor(cfg config.ClusterConfig, objs []client.Object, fns interceptor.Funcs) (*kubevirtprovider.Service, client.Client) {
	c := clustertest.BuildFakeClient(objs, &fns)
	return kubevirtprovider.New(c, cfg), c
}

// ── HostedCluster fixtures ──────────────────────────────────────────

func buildHostedCluster(name, namespace string, opts ...clustertest.HCOption) *hyperv1.HostedCluster {
	return clustertest.BuildHostedCluster(name, namespace, opts...)
}

func withDCMLabels(instanceID string) clustertest.HCOption {
	return clustertest.WithDCMLabels(instanceID)
}

// ── Request fixtures ────────────────────────────────────────────────

func validCreateCluster() v1alpha1.Cluster {
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
		},
	}
}
