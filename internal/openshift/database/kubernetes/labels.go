package kubernetes

import (
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	"k8s.io/apimachinery/pkg/labels"
)

func dcmLabels(instanceID string) map[string]string { return dcm.Labels(instanceID) }
func instanceSelector(instanceID string) string     { return dcm.InstanceSelector(instanceID) }
func dcmSelector() string                           { return dcm.Selector() }

// mergeLabels merges DCM base labels with user labels into a new map.
// Base labels overwrite user labels on collision — DCM labels always win
// (defense-in-depth against label corruption).
func mergeLabels(base, user map[string]string) map[string]string {
	return labels.Merge(labels.Set(user), labels.Set(base))
}
