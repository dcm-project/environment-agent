// Package status maps HyperShift conditions to DCM cluster statuses.
package status

import (
	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MapConditionsToStatus maps HostedCluster conditions to a DCM ClusterStatus.
// Accepts generic []metav1.Condition so it can be shared across platforms.
//
// Precedence (highest to lowest):
//  1. deletionTimestamp set → DELETING
//  2. Degraded=True → FAILED
//  3. Available=True → READY (Progressing state is irrelevant when Available=True)
//  4. Progressing=True → PROVISIONING
//  5. Available=False AND Progressing=False (both explicitly present) → UNAVAILABLE
//  6. default → PENDING
func MapConditionsToStatus(conditions []metav1.Condition, deletionTimestamp *metav1.Time) v1alpha1.ClusterStatus {
	if deletionTimestamp != nil {
		return v1alpha1.ClusterStatusDELETING
	}

	if conditionIsTrue(conditions, "Degraded") {
		return v1alpha1.ClusterStatusFAILED
	}

	if conditionIsTrue(conditions, "Available") {
		return v1alpha1.ClusterStatusREADY
	}

	if conditionIsTrue(conditions, "Progressing") {
		return v1alpha1.ClusterStatusPROVISIONING
	}

	if conditionIsFalse(conditions, "Available") && conditionIsFalse(conditions, "Progressing") {
		return v1alpha1.ClusterStatusUNAVAILABLE
	}

	return v1alpha1.ClusterStatusPENDING
}

// conditionIsTrue returns true if the named condition exists with Status=True.
func conditionIsTrue(conditions []metav1.Condition, condType string) bool {
	for i := range conditions {
		if conditions[i].Type == condType {
			return conditions[i].Status == metav1.ConditionTrue
		}
	}
	return false
}

// conditionIsFalse returns true if the named condition exists with Status=False.
func conditionIsFalse(conditions []metav1.Condition, condType string) bool {
	for i := range conditions {
		if conditions[i].Type == condType {
			return conditions[i].Status == metav1.ConditionFalse
		}
	}
	return false
}
