package monitoring

import (
	"sync"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

// instanceState holds per-instance tracking for dual-resource monitoring.
type instanceState struct {
	hcStatus      v1alpha1.ClusterStatus // "" = not seen
	npStatus      v1alpha1.ClusterStatus // "" = not seen/excluded
	hcMsg         string
	npMsg         string
	lastPublished v1alpha1.ClusterStatus
	seenHC        string // HC resource name, "" = not seen
	seenNP        string // NP resource name, "" = not seen
}

// compositeMessage returns the message from whichever resource drives the composite.
func (inst *instanceState) compositeMessage() string {
	if inst.npStatus != "" && status.StatusPriority[inst.npStatus] > status.StatusPriority[inst.hcStatus] {
		return inst.npMsg
	}
	return inst.hcMsg
}

// reconcileState manages per-instance state keyed by DCM instance ID.
type reconcileState struct {
	instances map[string]*instanceState
	mu        sync.Mutex
}

func newReconcileState() *reconcileState {
	return &reconcileState{instances: make(map[string]*instanceState)}
}

func (s *reconcileState) getOrCreate(instanceID string) *instanceState {
	inst, ok := s.instances[instanceID]
	if !ok {
		inst = &instanceState{}
		s.instances[instanceID] = inst
	}
	return inst
}

// reconcileResource maps an unstructured HC or NP to a composite StatusEvent.
func (m *StatusMonitor) reconcileResource(obj *unstructured.Unstructured, resType resourceType, state *reconcileState) (string, *StatusEvent) {
	instanceID := extractInstanceID(obj)
	if instanceID == "" {
		m.logger.Warn("skipping resource with missing dcm-instance-id",
			"name", obj.GetName(), "namespace", obj.GetNamespace())
		return "", nil
	}

	conditions := extractConditions(obj)

	state.mu.Lock()
	defer state.mu.Unlock()

	inst := state.getOrCreate(instanceID)

	// Duplicate detection (per resource type).
	var seenName *string
	switch resType {
	case resourceTypeHostedCluster:
		seenName = &inst.seenHC
	case resourceTypeNodePool:
		seenName = &inst.seenNP
	}
	if *seenName != "" && *seenName != obj.GetName() {
		m.logger.Warn("duplicate dcm-instance-id detected",
			"instanceID", instanceID,
			"resource", obj.GetName(),
			"existingResource", *seenName,
		)
	} else if *seenName == "" {
		*seenName = obj.GetName()
	}

	// Map conditions using appropriate mapper.
	switch resType {
	case resourceTypeHostedCluster:
		hcStatus := status.MapConditionsToStatus(conditions, obj.GetDeletionTimestamp())
		if inst.hcStatus == hcStatus {
			return "", nil
		}
		inst.hcMsg = extractHostedClusterMessage(conditions, hcStatus)
		inst.hcStatus = hcStatus

	case resourceTypeNodePool:
		npStatus, ok := status.MapNodePoolConditionsToStatus(conditions)
		if !ok {
			return "", nil
		}
		inst.npMsg = extractNodePoolMessage(conditions, npStatus)
		if inst.npStatus == npStatus {
			return "", nil
		}
		inst.npStatus = npStatus
		// If HC hasn't been seen yet, store NP status but don't publish.
		// HC reconcile will include this NP status in the composite.
		if inst.hcStatus == "" {
			return "", nil
		}
	}

	// Compute composite.
	var npPtr *v1alpha1.ClusterStatus
	if inst.npStatus != "" {
		npPtr = &inst.npStatus
	}
	composite := status.ComputeCompositeStatus(inst.hcStatus, npPtr)

	if inst.lastPublished == composite {
		return "", nil
	}
	inst.lastPublished = composite

	msg := inst.compositeMessage()
	return instanceID, &StatusEvent{InstanceID: instanceID, Status: composite, Message: msg}
}

// handleDeleteHostedCluster processes an HC delete event.
func (m *StatusMonitor) handleDeleteHostedCluster(obj any, state *reconcileState) (string, *StatusEvent) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	uns, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return "", nil
	}
	id := extractInstanceID(uns)
	if id == "" {
		m.logger.Warn("skipping resource with missing dcm-instance-id",
			"name", uns.GetName(), "namespace", uns.GetNamespace())
		return "", nil
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.instances, id)

	return id, &StatusEvent{
		InstanceID: id,
		Status:     v1alpha1.ClusterStatusDELETED,
	}
}

// handleDeleteNodePool processes an NP delete event — recomputes composite, no DELETED.
func (m *StatusMonitor) handleDeleteNodePool(obj any, state *reconcileState) (string, *StatusEvent) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	uns, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return "", nil
	}
	id := extractInstanceID(uns)
	if id == "" {
		m.logger.Warn("skipping resource with missing dcm-instance-id",
			"name", uns.GetName(), "namespace", uns.GetNamespace())
		return "", nil
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	inst, exists := state.instances[id]
	if !exists || inst.hcStatus == "" {
		return "", nil
	}
	inst.npStatus = ""
	inst.npMsg = ""
	inst.seenNP = ""

	composite := inst.hcStatus
	if inst.lastPublished == composite {
		return "", nil
	}
	inst.lastPublished = composite
	return id, &StatusEvent{InstanceID: id, Status: composite, Message: inst.hcMsg}
}

// extractConditions parses status.conditions from an unstructured resource.
func extractConditions(obj *unstructured.Unstructured) []metav1.Condition {
	statusObj, ok := obj.Object["status"].(map[string]any)
	if !ok {
		return nil
	}
	condSlice, ok := statusObj["conditions"].([]any)
	if !ok {
		return nil
	}

	conditions := make([]metav1.Condition, 0, len(condSlice))
	for _, item := range condSlice {
		condMap, ok := item.(map[string]any)
		if !ok {
			continue
		}
		c := metav1.Condition{}
		if t, ok := condMap["type"].(string); ok {
			c.Type = t
		}
		if s, ok := condMap["status"].(string); ok {
			c.Status = metav1.ConditionStatus(s)
		}
		if msg, ok := condMap["message"].(string); ok {
			c.Message = msg
		}
		conditions = append(conditions, c)
	}
	return conditions
}

// extractHostedClusterMessage returns a human-readable message for HC FAILED or UNAVAILABLE statuses.
func extractHostedClusterMessage(conditions []metav1.Condition, dcmStatus v1alpha1.ClusterStatus) string {
	switch dcmStatus {
	case v1alpha1.ClusterStatusFAILED:
		for i := range conditions {
			if conditions[i].Type == "Degraded" {
				return conditions[i].Message
			}
		}
	case v1alpha1.ClusterStatusUNAVAILABLE:
		for i := range conditions {
			if conditions[i].Type == "Available" {
				return conditions[i].Message
			}
		}
	}
	return ""
}

// extractNodePoolMessage returns "NodePool: <msg>" for NP-driven statuses.
func extractNodePoolMessage(conditions []metav1.Condition, dcmStatus v1alpha1.ClusterStatus) string {
	switch dcmStatus {
	case v1alpha1.ClusterStatusUNAVAILABLE:
		for i := range conditions {
			if conditions[i].Type == "Ready" {
				return "NodePool: " + conditions[i].Message
			}
		}
	case v1alpha1.ClusterStatusPROVISIONING:
		for i := range conditions {
			if conditions[i].Type == "UpdatingVersion" || conditions[i].Type == "UpdatingConfig" {
				return "NodePool: " + conditions[i].Message
			}
		}
	}
	return ""
}
