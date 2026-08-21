package cluster_test

import (
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster/clustertest"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const testNamespace = clustertest.TestNamespace

func defaultConfig() config.ClusterConfig {
	return clustertest.DefaultConfig()
}

func buildFakeClient(objs []client.Object, fns *interceptor.Funcs) client.Client {
	return clustertest.BuildFakeClient(objs, fns)
}

// ── HostedCluster fixtures ──────────────────────────────────────────

type hcOption = clustertest.HCOption

func withConditions(conditions ...metav1.Condition) hcOption {
	return func(hc *hyperv1.HostedCluster) {
		hc.Status.Conditions = conditions
	}
}

func withKubeConfigRef() hcOption {
	return func(hc *hyperv1.HostedCluster) {
		hc.Status.KubeConfig = &corev1.LocalObjectReference{Name: "my-cluster-admin-kubeconfig"}
	}
}

func withAPIEndpoint() hcOption {
	return func(hc *hyperv1.HostedCluster) {
		hc.Status.ControlPlaneEndpoint = hyperv1.APIEndpoint{Host: "api.cluster.example.com", Port: 6443}
	}
}

func withDCMLabels(instanceID string) hcOption {
	return clustertest.WithDCMLabels(instanceID)
}

func withBaseDomain(domain string) hcOption {
	return func(hc *hyperv1.HostedCluster) {
		hc.Spec.DNS.BaseDomain = domain
	}
}

func buildHostedCluster(name string, opts ...hcOption) *hyperv1.HostedCluster {
	return clustertest.BuildHostedCluster(name, testNamespace, opts...)
}

// ── NodePool fixtures ───────────────────────────────────────────────

type npOption func(*hyperv1.NodePool)

func withNPDCMLabels(instanceID string) npOption {
	return func(np *hyperv1.NodePool) {
		if np.Labels == nil {
			np.Labels = make(map[string]string)
		}
		np.Labels["dcm.project/managed-by"] = "dcm"
		np.Labels["dcm.project/dcm-instance-id"] = instanceID
		np.Labels["dcm.project/dcm-service-type"] = "cluster"
	}
}

func buildNodePool(name, namespace string, opts ...npOption) *hyperv1.NodePool {
	np := &hyperv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: hyperv1.NodePoolSpec{
			ClusterName: name,
			Platform: hyperv1.NodePoolPlatform{
				Type: hyperv1.KubevirtPlatform,
			},
		},
	}
	for _, opt := range opts {
		opt(np)
	}
	return np
}

// ── Condition presets ───────────────────────────────────────────────

func readyConditions() []metav1.Condition {
	return []metav1.Condition{
		{Type: "Available", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()},
		{Type: "Progressing", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now()},
	}
}

func provisioningConditions() []metav1.Condition {
	return []metav1.Condition{
		{Type: "Progressing", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()},
		{Type: "Available", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now()},
	}
}

func unavailableConditions() []metav1.Condition {
	return []metav1.Condition{
		{Type: "Available", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now()},
		{Type: "Progressing", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now()},
	}
}

func buildKubeconfigSecret(name, namespace string, kubeconfigData []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			"kubeconfig": kubeconfigData,
		},
	}
}
