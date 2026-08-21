// Package cluster provides shared labels, conversion utilities,
// and platform-agnostic operations for cluster service provider implementations.
package cluster

const (
	LabelManagedBy   = "dcm.project/managed-by"
	LabelInstanceID  = "dcm.project/dcm-instance-id"
	LabelServiceType = "dcm.project/dcm-service-type"
	ValueManagedBy   = "dcm"
	ValueServiceType = "cluster"
)

// DCMLabels returns the standard set of DCM labels for a cluster resource.
func DCMLabels(instanceID string) map[string]string {
	return map[string]string{
		LabelManagedBy:   ValueManagedBy,
		LabelInstanceID:  instanceID,
		LabelServiceType: ValueServiceType,
	}
}
