package monitoring

import (
	"fmt"
	"strings"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	k8sutil "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	hibernationAnnotation = "cnpg.io/hibernation"
	podRoleLabel          = "cnpg.io/podRole"
	podRoleInstance       = "instance"
	jobRoleLabel          = "cnpg.io/jobRole"
)

// transientWaitingReasons are ordinary initialization states that do not
// indicate a rollout failure; pods in these states should remain PENDING.
var transientWaitingReasons = map[string]bool{
	"ContainerCreating": true,
	"PodInitializing":   true,
}

// ReconcileStatus derives the DCM database status from the current state of a
// CNPG Cluster and its associated instance pods and PVCs. It returns the
// status, a human-readable message, and whether the result should be published.
func ReconcileStatus(cluster *cnpgv1.Cluster, pods []*corev1.Pod, pvcs []*corev1.PersistentVolumeClaim) (v1alpha1.DatabaseStatus, string, bool) {
	if cluster == nil && len(pods) == 0 && len(pvcs) == 0 {
		return v1alpha1.DELETED, "resource no longer exists", true
	}

	// Stuck-rollout detection takes precedence over pod-phase mapping and is
	// evaluated regardless of Pod existence.
	if cluster != nil {
		if cluster.DeletionTimestamp != nil {
			return v1alpha1.DELETING, "cluster deleting", true
		}
		if status, msg, ok := stuckRolloutStatus(cluster, pods); ok {
			return status, msg, true
		}
	}

	// Hibernation is unsupported; report FAILED regardless of whether pods are
	// still present (they may not have drained yet when the annotation is set).
	if cluster != nil {
		if toggle, ok := cluster.Annotations[hibernationAnnotation]; ok && toggle == "on" {
			return v1alpha1.FAILED, "cluster hibernating", true
		}
	}

	// Filter pods before MatchReplicas: Non instance pods would have no PVC
	// and would cause a mismatch error
	var resolvedPods []corev1.Pod
	excludedPodNames := make(map[string]struct{})
	for _, pod := range pods {
		if podRoleInstance != pod.Labels[podRoleLabel] {
			excludedPodNames[pod.Name] = struct{}{}
			continue
		}
		resolvedPods = append(resolvedPods, *pod)
	}
	var resolvedPvcs []corev1.PersistentVolumeClaim
	for _, pvc := range pvcs {
		if podName, ok := pvc.Labels[k8sutil.InstanceLabel]; ok {
			if _, excluded := excludedPodNames[podName]; excluded {
				continue
			}
		}
		resolvedPvcs = append(resolvedPvcs, *pvc)
	}

	if len(resolvedPods) != 0 || len(resolvedPvcs) != 0 {
		replicas, err := k8sutil.MatchReplicas(resolvedPods, resolvedPvcs)
		if err != nil {
			return v1alpha1.PENDING, fmt.Sprintf("%s: %d pods and %d pvcs", err.Error(), len(resolvedPods), len(resolvedPvcs)), true
		}

		msg := ""
		resultStatus := v1alpha1.RUNNING
		anyMapped := false
		for _, replica := range replicas {
			status, ok := k8sutil.MapReplicaStatus(replica)
			if !ok {
				continue
			}
			anyMapped = true

			if status == v1alpha1.FAILED {
				if reason := extractPodFailureReason(replica.Pod); reason != "" {
					msg = reason
				}
			}

			resultStatus = k8sutil.StatusHigherPriority(status, resultStatus)
		}
		if !anyMapped {
			return "", "", false
		}
		if msg == "" {
			for _, replica := range replicas {
				if status, ok := k8sutil.MapReplicaStatus(replica); ok && status == resultStatus {
					if m := replica.Pod.Status.Message; m != "" {
						msg = m
						break
					}
				}
			}
		}

		// If the cluster exists and matched fewer replicas than desired, a
		// pod+PVC pair disappeared silently. MatchReplicas won't detect this
		// because the remaining pairs still match — we must compare the count.
		if cluster != nil && cluster.Spec.Instances > 0 && len(replicas) < cluster.Spec.Instances {
			missing := cluster.Spec.Instances - len(replicas)
			resultStatus = k8sutil.StatusHigherPriority(v1alpha1.PENDING, resultStatus)
			if msg == "" {
				msg = fmt.Sprintf("waiting for %d missing replica(s)", missing)
			}
		}

		return resultStatus, msg, true
	}

	// Cluster-only path: no active instance pods or PVCs remain after filtering.
	// Guard against cluster being nil (e.g. only job pods were present).
	if cluster == nil {
		return v1alpha1.DELETED, "resource no longer exists", true
	}

	if cluster.Status.Phase == cnpgv1.PhaseWaitingForInstancesToBeActive ||
		cluster.Status.Phase == cnpgv1.PhaseCreatingReplica {
		return v1alpha1.PENDING, cluster.Status.Phase, true
	}
	return v1alpha1.PENDING, "waiting for pods", true
}

// stuckRolloutStatus reports FAILED when the CNPG Cluster is unhealthy (not in
// PhaseHealthy or Ready condition false) and instance pods show failure reasons,
// or when the cluster is in failover.
func stuckRolloutStatus(cluster *cnpgv1.Cluster, pods []*corev1.Pod) (v1alpha1.DatabaseStatus, string, bool) {
	isReady := false
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == string(cnpgv1.ConditionClusterReady) && cond.Status == metav1.ConditionTrue {
			isReady = true
			break
		}
	}
	if cluster.Status.Phase != cnpgv1.PhaseHealthy || !isReady {
		var reason string
		for _, pod := range pods {
			// Exclude job pods (e.g. CNPG init/backup jobs) and successfully
			// completed pods — only instance pods carry meaningful failure evidence.
			if pod.Labels[podRoleLabel] != podRoleInstance || pod.Status.Phase == corev1.PodSucceeded {
				continue
			}
			if r := extractPodFailureReason(pod); r != "" {
				reason = r
			}
		}
		base := strings.TrimSuffix(cluster.Status.PhaseReason, ".")
		if base == "" {
			base = cluster.Status.Phase
		}
		if reason == "" {
			// No pod-level evidence, but PhaseFailOver unambiguously indicates a
			// cluster problem even before pods surface container statuses.
			if cluster.Status.Phase != cnpgv1.PhaseFailOver {
				return "", "", false
			}
			return v1alpha1.FAILED, base, true
		}
		return v1alpha1.FAILED, fmt.Sprintf("%s (%s)", base, reason), true
	}

	return "", "", false
}

// extractPodFailureReason attempts to find a specific failure reason from
// pod container statuses (e.g., CrashLoopBackOff).
func extractPodFailureReason(pod *corev1.Pod) string {
	if pod.Status.Phase == corev1.PodPending {
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse {
				return cond.Reason
			}
		}
		// Pod is scheduled but still pending — inspect container waiting states
		// for actionable reasons such as ErrImagePull or ImagePullBackOff.
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" && !transientWaitingReasons[cs.State.Waiting.Reason] {
			return cs.State.Waiting.Reason
		}
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" && cs.State.Terminated.ExitCode != 0 {
			return cs.State.Terminated.Reason
		}
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" && !transientWaitingReasons[cs.State.Waiting.Reason] {
			return cs.State.Waiting.Reason
		}
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" && cs.State.Terminated.ExitCode != 0 {
			return cs.State.Terminated.Reason
		}
	}
	return ""
}
