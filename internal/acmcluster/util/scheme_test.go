package util_test

import (
	"testing"

	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestBuildSchemeRecognizesAllHealthGVKs is the regression test for FLPATH-4644.
// Production scheme construction was missing platform GVKs (KubeVirt, Agent),
// causing Scheme().Recognizes() to return false and health to always report
// unhealthy. This test ensures BuildScheme() registers every GVK that the
// health checker probes.
func TestBuildSchemeRecognizesAllHealthGVKs(t *testing.T) {
	scheme, err := util.BuildScheme()
	if err != nil {
		t.Fatalf("BuildScheme() returned unexpected error: %v", err)
	}

	gvks := []struct {
		name string
		gvk  schema.GroupVersionKind
	}{
		{"HostedCluster", util.HostedClusterGVK},
		{"HostedClusterList", util.HostedClusterListGVK},
		{"KubevirtVMI", util.KubevirtVMIGVK},
		{"KubevirtVMIList", util.KubevirtVMIListGVK},
		{"Agent", util.AgentGVK},
		{"AgentList", util.AgentListGVK},
		{"Secret (corev1)", schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Secret"}},
	}

	for _, tc := range gvks {
		if !scheme.Recognizes(tc.gvk) {
			t.Errorf("scheme does not recognize %s GVK %s", tc.name, tc.gvk)
		}
	}
}
