package kubernetes

import (
	"github.com/dcm-project/environment-agent/internal/openshift/storage/dcm"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func dcmLabels(instanceID string) map[string]string { return dcm.Labels(instanceID) }
func dcmSelector() string                           { return dcm.Selector() }

func isDCMManagedPVC(pvc *corev1.PersistentVolumeClaim, volumeID string) bool {
	if pvc == nil || pvc.Labels == nil {
		return false
	}
	return pvc.Labels[dcm.LabelManagedBy] == dcm.ValueManagedByDCM &&
		pvc.Labels[dcm.LabelServiceType] == dcm.ValueServiceType &&
		pvc.Labels[dcm.LabelInstanceID] == volumeID
}

// mergeLabels merges DCM base labels with user labels into a new map.
// Base labels overwrite user labels on collision — DCM labels always win
// (defense-in-depth against label corruption).
func mergeLabels(base, user map[string]string) map[string]string {
	return labels.Merge(labels.Set(user), labels.Set(base))
}
