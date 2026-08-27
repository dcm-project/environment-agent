package monitoring

import (
	"fmt"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	corev1listers "k8s.io/client-go/listers/core/v1"
)

// terminalPVCWarningReasons are Warning Event reasons treated as unrecoverable
// PVC bind/provision failures (enhancement FAILED mapping beyond ClaimLost).
var terminalPVCWarningReasons = map[string]struct{}{
	"FailedBinding":      {},
	"ProvisioningFailed": {},
	"FailedProvisioning": {},
	"AllocationFailed":   {},
}

func isTerminalPVCWarning(event *corev1.Event) bool {
	if event == nil || event.Type != corev1.EventTypeWarning {
		return false
	}
	if event.InvolvedObject.Kind != "PersistentVolumeClaim" {
		return false
	}
	_, ok := terminalPVCWarningReasons[event.Reason]
	return ok
}

func (m *StatusMonitor) handleWarningEvent(obj any, pvcLister corev1listers.PersistentVolumeClaimLister, debouncer *Debouncer) {
	event, ok := obj.(*corev1.Event)
	if !ok {
		return
	}
	if !isTerminalPVCWarning(event) {
		return
	}

	ns := event.InvolvedObject.Namespace
	if ns == "" {
		ns = m.cfg.Namespace
	}
	pvc, err := pvcLister.PersistentVolumeClaims(ns).Get(event.InvolvedObject.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			m.logger.Error("looking up PVC for warning event",
				"error", err,
				"pvc", event.InvolvedObject.Name,
				"namespace", ns,
			)
		}
		return
	}

	instanceID := ExtractInstanceID(pvc)
	if instanceID == "" {
		return // not a DCM-managed volume
	}

	msg := event.Message
	if msg == "" {
		msg = fmt.Sprintf("PVC failure: %s", event.Reason)
	} else {
		msg = fmt.Sprintf("%s: %s", event.Reason, msg)
	}

	m.submitIfChanged(debouncer, StatusEvent{
		InstanceID: instanceID,
		Status:     v1alpha1.FAILED,
		Message:    msg,
	})
}
