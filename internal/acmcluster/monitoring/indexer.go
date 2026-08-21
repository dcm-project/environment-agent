package monitoring

import (
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// InstanceIDIndex is the name of the secondary index on dcm-instance-id label.
const InstanceIDIndex = "instanceID"

// InstanceIDIndexFunc extracts the dcm-instance-id label from an unstructured object
// for use as a SharedIndexInformer secondary index.
func InstanceIDIndexFunc(obj any) ([]string, error) {
	uns, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, nil
	}
	id := extractInstanceID(uns)
	if id == "" {
		return []string{}, nil
	}
	return []string{id}, nil
}

// extractInstanceID returns the dcm-instance-id label value, or empty if missing.
func extractInstanceID(obj *unstructured.Unstructured) string {
	labels := obj.GetLabels()
	if labels == nil {
		return ""
	}
	return labels[cluster.LabelInstanceID]
}
