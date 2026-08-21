package status

import (
	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MapNodePoolConditionsToStatus maps NodePool conditions to a DCM ClusterStatus.
// Returns (status, false) if the NodePool has no conditions, indicating
// it should be excluded from composite status computation.
//
// Precedence (highest to lowest):
//  1. UpdatingVersion=True OR UpdatingConfig=True → PROVISIONING
//  2. Ready=False → UNAVAILABLE
//  3. Ready=True → READY
//  4. default (conditions present but none matched) → PENDING
func MapNodePoolConditionsToStatus(conditions []metav1.Condition) (v1alpha1.ClusterStatus, bool) {
	if len(conditions) == 0 {
		return "", false
	}

	if conditionIsTrue(conditions, "UpdatingVersion") || conditionIsTrue(conditions, "UpdatingConfig") {
		return v1alpha1.ClusterStatusPROVISIONING, true
	}

	if conditionIsFalse(conditions, "Ready") {
		return v1alpha1.ClusterStatusUNAVAILABLE, true
	}

	if conditionIsTrue(conditions, "Ready") {
		return v1alpha1.ClusterStatusREADY, true
	}

	return v1alpha1.ClusterStatusPENDING, true
}
