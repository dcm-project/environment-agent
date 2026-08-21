// Package clustertest provides shared test helpers for cluster service tests.
package clustertest

import (
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/registration"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	TestNamespace  = "test-clusters"
	TestBaseDomain = "example.com"
)

// DefaultConfig returns a base Config suitable for most tests.
func DefaultConfig() config.ClusterConfig {
	return config.ClusterConfig{
		ClusterNamespace:  TestNamespace,
		BaseDomain:        TestBaseDomain,
		PullSecretName:    "test-sp-pull-secret",
		ConsoleURIPattern: "https://console-openshift-console.apps.{name}.{base_domain}",
		VersionMatrix:     map[string]string(registration.DefaultCompatibilityMatrix),
	}
}

// BuildFakeClient creates a fake k8s client with the HyperShift scheme,
// default ClusterImageSets, and optional interceptor functions.
func BuildFakeClient(objs []client.Object, fns *interceptor.Funcs) client.Client {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = hyperv1.AddToScheme(scheme)

	restMapper := meta.NewDefaultRESTMapper(nil)
	restMapper.Add(util.ClusterImageSetGVK, meta.RESTScopeRoot)

	allObjs := append(DefaultClusterImageSets(), objs...)

	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRESTMapper(restMapper).
		WithObjects(allObjs...)

	if fns != nil {
		builder = builder.WithInterceptorFuncs(*fns)
	}

	return builder.Build()
}

// DefaultClusterImageSets returns the standard set of ClusterImageSets used in tests.
func DefaultClusterImageSets() []client.Object {
	return []client.Object{
		BuildClusterImageSet("ocp-4.15.2", "quay.io/openshift-release-dev/ocp-release:4.15.2-x86_64"),
		BuildClusterImageSet("ocp-4.17.0", "quay.io/openshift-release-dev/ocp-release:4.17.0-x86_64"),
	}
}

// BuildClusterImageSet creates an unstructured ClusterImageSet for testing.
func BuildClusterImageSet(name, releaseImage string) *unstructured.Unstructured {
	cis := &unstructured.Unstructured{}
	cis.SetGroupVersionKind(util.ClusterImageSetGVK)
	cis.SetName(name)
	_ = unstructured.SetNestedField(cis.Object, releaseImage, "spec", "releaseImage")
	return cis
}

// HCOption is a functional option for building HostedCluster fixtures.
type HCOption func(*hyperv1.HostedCluster)

// WithDCMLabels sets the standard DCM labels on a HostedCluster fixture.
func WithDCMLabels(instanceID string) HCOption {
	return func(hc *hyperv1.HostedCluster) {
		if hc.Labels == nil {
			hc.Labels = make(map[string]string)
		}
		hc.Labels["dcm.project/managed-by"] = "dcm"
		hc.Labels["dcm.project/dcm-instance-id"] = instanceID
		hc.Labels["dcm.project/dcm-service-type"] = "cluster"
	}
}

// BuildHostedCluster creates a HostedCluster fixture with KubevirtPlatform defaults.
func BuildHostedCluster(name, namespace string, opts ...HCOption) *hyperv1.HostedCluster {
	hc := &hyperv1.HostedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: hyperv1.HostedClusterSpec{
			Platform: hyperv1.PlatformSpec{
				Type: hyperv1.KubevirtPlatform,
			},
			Release: hyperv1.Release{
				Image: "quay.io/openshift-release-dev/ocp-release:4.17.0-x86_64",
			},
		},
	}
	for _, opt := range opts {
		opt(hc)
	}
	return hc
}
