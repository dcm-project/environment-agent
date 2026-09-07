package kubernetes

import (
	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func dcmLabels(instanceID string) map[string]string { return dcm.Labels(instanceID) }
func instanceSelector(instanceID string) string     { return dcm.InstanceSelector(instanceID) }
func dcmSelector() string                           { return dcm.Selector() }

func isDCMManagedService(svc *corev1.Service, networkID string) bool {
	if svc == nil || svc.Labels == nil {
		return false
	}
	return svc.Labels[dcm.LabelManagedBy] == dcm.ValueManagedByDCM &&
		svc.Labels[dcm.LabelServiceType] == dcm.ValueServiceType &&
		svc.Labels[dcm.LabelInstanceID] == networkID
}

// mergeLabels merges DCM base labels with user labels into a new map.
// Base labels overwrite user labels on collision — DCM labels always win
// (defense-in-depth against label corruption).
func mergeLabels(base, user map[string]string) map[string]string {
	return labels.Merge(labels.Set(user), labels.Set(base))
}
