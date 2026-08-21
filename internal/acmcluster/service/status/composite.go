package status

import (
	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
)

// StatusPriority defines the severity ordering for worst-of computation.
// Higher value = worse status. Used by ComputeCompositeStatus to pick
// the worse of HostedCluster and NodePool statuses.
//
// DELETED (7) is included for completeness but is never exercised in
// composite computation — DELETED comes from delete handlers, not mappers.
var StatusPriority = map[v1alpha1.ClusterStatus]int{
	v1alpha1.ClusterStatusREADY:        1,
	v1alpha1.ClusterStatusPENDING:      2,
	v1alpha1.ClusterStatusPROVISIONING: 3,
	v1alpha1.ClusterStatusUNAVAILABLE:  4,
	v1alpha1.ClusterStatusFAILED:       5,
	v1alpha1.ClusterStatusDELETING:     6,
	v1alpha1.ClusterStatusDELETED:      7,
}

// ComputeCompositeStatus returns the worst-of HostedCluster and NodePool
// statuses. If npStatus is nil, the HC status is returned as-is.
func ComputeCompositeStatus(hcStatus v1alpha1.ClusterStatus, npStatus *v1alpha1.ClusterStatus) v1alpha1.ClusterStatus {
	if npStatus == nil {
		return hcStatus
	}
	if StatusPriority[*npStatus] > StatusPriority[hcStatus] {
		return *npStatus
	}
	return hcStatus
}
