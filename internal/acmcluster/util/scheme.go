package util

import (
	"fmt"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// BuildScheme constructs the runtime scheme with all types required by the
// service provider: core/v1, HyperShift, and platform-specific health-probe
// GVKs registered as unstructured types.
//
// Platform GVKs (KubeVirt, Agent) are registered via AddKnownTypeWithName with
// unstructured types so that Scheme().Recognizes() returns true for health
// checks, without importing the full platform API modules. This is intentional
// scheme membership for the Recognizes() gate in health.checkCRDAvailable;
// actual List() calls go through the unstructured/RESTMapper path.
//
// WARNING: If typed platform API modules (kubevirt.io/api, agent-install.openshift.io)
// are ever added to go.mod and their AddToScheme is called on the same scheme,
// AddKnownTypeWithName will panic due to double-registration of different Go
// types for the same GVK. Remove the unstructured registrations below first.
func BuildScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()

	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("registering core/v1 types: %w", err)
	}
	if err := hyperv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("registering HyperShift types: %w", err)
	}

	scheme.AddKnownTypeWithName(KubevirtVMIGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(KubevirtVMIListGVK, &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(AgentGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(AgentListGVK, &unstructured.UnstructuredList{})

	return scheme, nil
}
