// Package dcm defines shared constants and conventions for the DCM service provider.
package dcm

import "k8s.io/apimachinery/pkg/labels"

const (
	LabelManagedBy    = "dcm.project/managed-by"
	LabelInstanceID   = "dcm.project/dcm-instance-id"
	LabelServiceType  = "dcm.project/dcm-service-type"
	ValueManagedByDCM = "dcm"
	ValueServiceType  = "storage"
)

// ReservedLabelKeys is the set of label keys managed by DCM.
// User-supplied labels must not use these keys.
var ReservedLabelKeys = map[string]bool{
	LabelManagedBy:   true,
	LabelInstanceID:  true,
	LabelServiceType: true,
}

// Labels returns the standard DCM labels for a given instance ID.
func Labels(instanceID string) map[string]string {
	return map[string]string{
		LabelManagedBy:   ValueManagedByDCM,
		LabelInstanceID:  instanceID,
		LabelServiceType: ValueServiceType,
	}
}

// InstanceSelector returns a label selector string matching a specific instance.
func InstanceSelector(instanceID string) string {
	return labels.Set{
		LabelInstanceID:  instanceID,
		LabelManagedBy:   ValueManagedByDCM,
		LabelServiceType: ValueServiceType,
	}.String()
}

// Selector returns a label selector string matching all DCM-managed volumes.
func Selector() string {
	return labels.Set{
		LabelManagedBy:   ValueManagedByDCM,
		LabelServiceType: ValueServiceType,
	}.String()
}
