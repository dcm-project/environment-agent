package monitoring_test

import (
	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	k8sutils "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Status Reconciliation", func() {
	buildCluster := func(phase string, instances int, conditions ...metav1.Condition) *cnpgv1.Cluster { //nolint:unparam // instances varies (1/2/3) but unparam misses callers inside nested Context closures
		return &cnpgv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
			Spec:       cnpgv1.ClusterSpec{Instances: instances},
			Status:     cnpgv1.ClusterStatus{Conditions: conditions, Phase: phase},
		}
	}

	instancePodLabels := map[string]string{"cnpg.io/podRole": "instance"}

	// Helper to build a Pod with the given phase.
	buildPod := func(phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Labels: instancePodLabels},
			Status:     corev1.PodStatus{Phase: phase},
		}
	}

	// Helper to build a Pod with the given phase and container statuses.
	buildPodWithContainerStatuses := func(phase corev1.PodPhase, containerStatuses ...corev1.ContainerStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Labels: instancePodLabels},
			Status:     corev1.PodStatus{Phase: phase, ContainerStatuses: containerStatuses},
		}
	}

	buildPodWithConditions := func(phase corev1.PodPhase, conditions ...corev1.PodCondition) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Labels: instancePodLabels},
			Status:     corev1.PodStatus{Phase: phase, Conditions: conditions},
		}
	}

	buildPvc := func() *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pvc",
				Labels: map[string]string{
					k8sutils.InstanceLabel: "test-pod",
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
	}

	buildNamedPod := func(name string, phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: instancePodLabels},
			Status:     corev1.PodStatus{Phase: phase},
		}
	}

	buildMatchedPvc := func(podName string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   podName + "-pvc",
				Labels: map[string]string{k8sutils.InstanceLabel: podName},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
	}

	notReadyCluster := metav1.Condition{
		Type:    string(cnpgv1.ConditionClusterReady),
		Status:  metav1.ConditionFalse,
		Reason:  string(cnpgv1.ClusterIsNotReady),
		Message: "Cluster Is Not Ready",
	}

	stuckRolloutPod := corev1.PodCondition{
		Type:    corev1.PodScheduled,
		Status:  corev1.ConditionFalse,
		Reason:  corev1.PodReasonUnschedulable,
		Message: "0/3 nodes are available: 3 Insufficient cpu.",
	}

	It("should derive status from Pod phase when both resources exist", func() {
		cluster := buildCluster(cnpgv1.PhaseHealthy, 1,
			metav1.Condition{Type: string(cnpgv1.ConditionClusterReady), Status: metav1.ConditionTrue},
		)
		pod := buildPod(corev1.PodRunning)
		pvc := buildPvc()

		status, _, publish := monitoring.ReconcileStatus(cluster, []*corev1.Pod{pod}, []*corev1.PersistentVolumeClaim{pvc})

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.RUNNING))
	})

	It("should return PENDING when cluster Ready=False and no Pod", func() {
		cluster := buildCluster(string(cnpgv1.PhaseWaitingForInstancesToBeActive), 1,
			metav1.Condition{Type: string(cnpgv1.ConditionClusterReady), Status: metav1.ConditionFalse},
		)

		status, _, publish := monitoring.ReconcileStatus(cluster, nil, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.PENDING))
	})

	It("should return FAILED when Cluster not healthy and Pod unschedulable", func() {
		cluster := buildCluster(string(cnpgv1.PhaseWaitingForInstancesToBeActive), 1, notReadyCluster)
		pod := buildPodWithConditions(corev1.PodPending, stuckRolloutPod)

		status, _, publish := monitoring.ReconcileStatus(cluster, []*corev1.Pod{pod}, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.FAILED))
	})

	It("should return PENDING when Cluster Ready=True but no Pod exists", func() {
		cluster := buildCluster(cnpgv1.PhaseHealthy, 1,
			metav1.Condition{Type: string(cnpgv1.ConditionClusterReady), Status: metav1.ConditionTrue},
		)

		status, _, publish := monitoring.ReconcileStatus(cluster, nil, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.PENDING))
	})

	It("should return DELETED when neither resource exists", func() {
		status, _, publish := monitoring.ReconcileStatus(nil, nil, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.DELETED))
	})

	It("should return PENDING (not FAILED) when Pod is Pending and Cluster Ready=False without a pod scheduling failure", func() {
		cluster := buildCluster(cnpgv1.PhaseWaitingForInstancesToBeActive, 1, notReadyCluster)
		pod := buildPod(corev1.PodPending)

		status, _, publish := monitoring.ReconcileStatus(cluster, []*corev1.Pod{pod}, nil)

		Expect(status).To(Equal(v1alpha1.PENDING))
		Expect(publish).To(BeTrue())
		Expect(status).NotTo(Equal(v1alpha1.FAILED))
	})

	It("should fall back to the cluster phase reason when the pod is nil", func() {
		cluster := buildCluster(cnpgv1.PhaseWaitingForInstancesToBeActive, 1, metav1.Condition{
			Type:    string(cnpgv1.ConditionClusterReady),
			Status:  metav1.ConditionFalse,
			Reason:  string(cnpgv1.ClusterIsNotReady),
			Message: "",
		})
		cluster.Status.PhaseReason = "Some instances are not yet active. Please wait."

		status, message, publish := monitoring.ReconcileStatus(cluster, nil, nil)

		Expect(status).To(Equal(v1alpha1.PENDING))
		Expect(publish).To(BeTrue())
		Expect(message).To(Equal(string(cnpgv1.PhaseWaitingForInstancesToBeActive)))
	})

	Context("multi-replica clusters", func() {
		readyCluster := metav1.Condition{Type: string(cnpgv1.ConditionClusterReady), Status: metav1.ConditionTrue}

		It("should return RUNNING when all replicas are Running", func() {
			cluster := buildCluster(cnpgv1.PhaseHealthy, 3, readyCluster)
			pods := []*corev1.Pod{
				buildNamedPod("pod-1", corev1.PodRunning),
				buildNamedPod("pod-2", corev1.PodRunning),
				buildNamedPod("pod-3", corev1.PodRunning),
			}
			pvcs := []*corev1.PersistentVolumeClaim{
				buildMatchedPvc("pod-1"),
				buildMatchedPvc("pod-2"),
				buildMatchedPvc("pod-3"),
			}

			status, _, publish := monitoring.ReconcileStatus(cluster, pods, pvcs)

			Expect(publish).To(BeTrue())
			Expect(status).To(Equal(v1alpha1.RUNNING))
		})

		It("should return PENDING when any replica is Pending", func() {
			cluster := buildCluster(cnpgv1.PhaseHealthy, 3, readyCluster)
			pods := []*corev1.Pod{
				buildNamedPod("pod-1", corev1.PodRunning),
				buildNamedPod("pod-2", corev1.PodPending),
				buildNamedPod("pod-3", corev1.PodRunning),
			}
			pvcs := []*corev1.PersistentVolumeClaim{
				buildMatchedPvc("pod-1"),
				buildMatchedPvc("pod-2"),
				buildMatchedPvc("pod-3"),
			}

			status, _, publish := monitoring.ReconcileStatus(cluster, pods, pvcs)

			Expect(publish).To(BeTrue())
			Expect(status).To(Equal(v1alpha1.PENDING))
		})

		It("should return FAILED when any replica is Failed", func() {
			cluster := buildCluster(cnpgv1.PhaseHealthy, 3, readyCluster)
			pods := []*corev1.Pod{
				buildNamedPod("pod-1", corev1.PodRunning),
				buildNamedPod("pod-2", corev1.PodRunning),
				buildNamedPod("pod-3", corev1.PodFailed),
			}
			pvcs := []*corev1.PersistentVolumeClaim{
				buildMatchedPvc("pod-1"),
				buildMatchedPvc("pod-2"),
				buildMatchedPvc("pod-3"),
			}

			status, _, publish := monitoring.ReconcileStatus(cluster, pods, pvcs)

			Expect(publish).To(BeTrue())
			Expect(status).To(Equal(v1alpha1.FAILED))
		})

		It("should source the fallback message from the pod driving the result status, not pods[0]", func() {
			// pod-1 (index 0) is Running with an empty message; pod-2 is Pending and
			// has a non-empty Status.Message. The fallback must use pod-2's message.
			cluster := buildCluster(cnpgv1.PhaseHealthy, 3, readyCluster)
			pod2 := buildNamedPod("pod-2", corev1.PodPending)
			pod2.Status.Message = "0/3 nodes are available"
			pods := []*corev1.Pod{
				buildNamedPod("pod-1", corev1.PodRunning),
				pod2,
				buildNamedPod("pod-3", corev1.PodRunning),
			}
			pvcs := []*corev1.PersistentVolumeClaim{
				buildMatchedPvc("pod-1"),
				buildMatchedPvc("pod-2"),
				buildMatchedPvc("pod-3"),
			}

			status, msg, publish := monitoring.ReconcileStatus(cluster, pods, pvcs)

			Expect(publish).To(BeTrue())
			Expect(status).To(Equal(v1alpha1.PENDING))
			Expect(msg).To(Equal("0/3 nodes are available"))
		})

		It("should return FAILED when only one of multiple pods has a scheduling failure", func() {
			cluster := buildCluster(string(cnpgv1.PhaseWaitingForInstancesToBeActive), 3, notReadyCluster)
			pods := []*corev1.Pod{
				buildNamedPod("pod-1", corev1.PodRunning),
				buildPodWithConditions(corev1.PodPending, stuckRolloutPod),
				buildNamedPod("pod-3", corev1.PodPending),
			}

			status, msg, publish := monitoring.ReconcileStatus(cluster, pods, nil)

			Expect(publish).To(BeTrue())
			Expect(status).To(Equal(v1alpha1.FAILED))
			Expect(msg).To(ContainSubstring(string(corev1.PodReasonUnschedulable)))
		})

		It("should return PENDING when a replica pod and PVC both disappear together", func() {
			// Cluster wants 2 replicas but only 1 pod+PVC pair is present.
			// MatchReplicas succeeds (1 matched pair, no mismatches), so without the
			// Spec.Instances guard we would falsely report RUNNING.
			cluster := buildCluster(cnpgv1.PhaseHealthy, 2, readyCluster)
			pods := []*corev1.Pod{buildNamedPod("pod-1", corev1.PodRunning)}
			pvcs := []*corev1.PersistentVolumeClaim{buildMatchedPvc("pod-1")}

			status, msg, publish := monitoring.ReconcileStatus(cluster, pods, pvcs)

			Expect(publish).To(BeTrue())
			Expect(status).To(Equal(v1alpha1.PENDING))
			Expect(msg).To(ContainSubstring("missing replica"))
		})

		It("should return FAILED when one replica succeeded and another failed", func() {
			// A Succeeded pod has no mapped phase (MapReplicaStatus returns ok=false).
			cluster := buildCluster(cnpgv1.PhaseHealthy, 2, readyCluster)
			pods := []*corev1.Pod{
				buildNamedPod("pod-1", corev1.PodSucceeded),
				buildNamedPod("pod-2", corev1.PodFailed),
			}
			pvcs := []*corev1.PersistentVolumeClaim{
				buildMatchedPvc("pod-1"),
				buildMatchedPvc("pod-2"),
			}

			status, _, publish := monitoring.ReconcileStatus(cluster, pods, pvcs)

			Expect(publish).To(BeTrue())
			Expect(status).To(Equal(v1alpha1.FAILED))
		})
	})

	It("should enrich the fallback reason constant with the Pod's waiting reason when Message is empty and pod is present", func() {
		cluster := buildCluster(cnpgv1.PhaseWaitingForInstancesToBeActive, 1, metav1.Condition{
			Type:    string(cnpgv1.ConditionClusterReady),
			Status:  metav1.ConditionFalse,
			Reason:  string(cnpgv1.ClusterIsNotReady),
			Message: "",
		})
		cluster.Status.PhaseReason = "Some instances are not yet active. Please wait."
		pod := buildPodWithContainerStatuses(corev1.PodRunning,
			corev1.ContainerStatus{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
		)

		status, message, publish := monitoring.ReconcileStatus(cluster, []*corev1.Pod{pod}, nil)

		Expect(status).To(Equal(v1alpha1.FAILED))
		Expect(publish).To(BeTrue())
		Expect(message).To(Equal("Some instances are not yet active. Please wait (CrashLoopBackOff)"))
	})

	It("should return RUNNING when a healthy cluster has a CNPG job pod alongside a running instance pod", func() {
		// CNPG job pods (e.g. initdb) carry cnpg.io/jobRole but no cnpg.io/instanceName,
		// so MatchReplicas cannot pair them with a PVC and would return an error.
		// The fix must strip job pods before calling MatchReplicas so a healthy
		// single-instance cluster with one running instance pod reports RUNNING.
		cluster := buildCluster(cnpgv1.PhaseHealthy, 1,
			metav1.Condition{Type: string(cnpgv1.ConditionClusterReady), Status: metav1.ConditionTrue},
		)
		instancePod := buildNamedPod("cluster-1", corev1.PodRunning)
		jobPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "cluster-1-initdb",
				Labels: map[string]string{"cnpg.io/jobRole": "initdb"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
		}
		pvcs := []*corev1.PersistentVolumeClaim{buildMatchedPvc("cluster-1")}

		status, _, publish := monitoring.ReconcileStatus(cluster, []*corev1.Pod{instancePod, jobPod}, pvcs)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.RUNNING),
			"job pod must be excluded before MatchReplicas so the healthy cluster reports RUNNING")
	})

	It("should not treat a succeeded CNPG job pod as a stuck-rollout failure", func() {
		// CNPG creates job pods (e.g. for bootstrap init) that complete successfully.
		// A Succeeded pod with reason=Completed and ExitCode=0 must not be mistaken
		// for a failing instance pod while the cluster is not yet healthy.
		cluster := buildCluster(string(cnpgv1.PhaseWaitingForInstancesToBeActive), 1, notReadyCluster)
		cluster.Status.PhaseReason = "Some instances are not yet active. Please wait."

		// A job pod: phase=Succeeded, no cnpg.io/podRole=instance label, container reason=Completed, ExitCode=0.
		jobPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "init-job-pod",
				Labels: map[string]string{"cnpg.io/jobRole": "initdb"},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodSucceeded,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "initdb",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								Reason:   "Completed",
								ExitCode: 0,
							},
						},
					},
				},
			},
		}

		status, _, publish := monitoring.ReconcileStatus(cluster, []*corev1.Pod{jobPod}, nil)

		// The job pod must not supply failure evidence; with no instance-pod evidence,
		// stuckRolloutStatus finds no pod reason and should not return FAILED
		// (PhaseWaitingForInstancesToBeActive is not PhaseFailOver).
		Expect(publish).To(BeTrue())
		Expect(status).NotTo(Equal(v1alpha1.FAILED),
			"a succeeded job pod must not produce FAILED during provisioning")
	})

	It("should return FAILED when cluster hibernation annotation is set and no pods exist", func() {
		cluster := buildCluster(cnpgv1.PhaseHealthy, 1,
			metav1.Condition{Type: string(cnpgv1.ConditionClusterReady), Status: metav1.ConditionTrue},
		)
		cluster.Annotations = map[string]string{"cnpg.io/hibernation": "on"}

		status, message, publish := monitoring.ReconcileStatus(cluster, nil, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.FAILED))
		Expect(message).To(Equal("cluster hibernating"))
	})

	It("should return FAILED when cluster hibernation annotation is set and a matched running replica still exists", func() {
		// Hibernation drains pods gradually; the annotation may be "on" while pods
		// are still present. The check must fire before the pod/PVC mapping branch.
		cluster := buildCluster(cnpgv1.PhaseHealthy, 1,
			metav1.Condition{Type: string(cnpgv1.ConditionClusterReady), Status: metav1.ConditionTrue},
		)
		cluster.Annotations = map[string]string{"cnpg.io/hibernation": "on"}
		pods := []*corev1.Pod{buildNamedPod("test-pod", corev1.PodRunning)}
		pvcs := []*corev1.PersistentVolumeClaim{buildMatchedPvc("test-pod")}

		status, message, publish := monitoring.ReconcileStatus(cluster, pods, pvcs)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.FAILED))
		Expect(message).To(Equal("cluster hibernating"))
	})

	It("should return PENDING (not FAILED) when a container is in ContainerCreating state", func() {
		cluster := buildCluster(string(cnpgv1.PhaseWaitingForInstancesToBeActive), 1, notReadyCluster)
		pod := buildPodWithContainerStatuses(corev1.PodPending,
			corev1.ContainerStatus{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}},
		)

		status, _, publish := monitoring.ReconcileStatus(cluster, []*corev1.Pod{pod}, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.PENDING))
		Expect(status).NotTo(Equal(v1alpha1.FAILED))
	})

	It("should return PENDING (not FAILED) when a container is in PodInitializing state", func() {
		cluster := buildCluster(string(cnpgv1.PhaseWaitingForInstancesToBeActive), 1, notReadyCluster)
		pod := buildPodWithContainerStatuses(corev1.PodPending,
			corev1.ContainerStatus{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}},
		)

		status, _, publish := monitoring.ReconcileStatus(cluster, []*corev1.Pod{pod}, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.PENDING))
		Expect(status).NotTo(Equal(v1alpha1.FAILED))
	})

	It("should return FAILED when a pending instance pod has an init container in ImagePullBackOff", func() {
		cluster := buildCluster(string(cnpgv1.PhaseWaitingForInstancesToBeActive), 1, notReadyCluster)
		cluster.Status.PhaseReason = "Waiting for instances to be active"
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Labels: instancePodLabels},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				InitContainerStatuses: []corev1.ContainerStatus{
					{Name: "init", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}},
				},
			},
		}

		status, message, publish := monitoring.ReconcileStatus(cluster, []*corev1.Pod{pod}, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.FAILED))
		Expect(message).To(ContainSubstring("ImagePullBackOff"))
	})

	It("should surface container waiting failures on a scheduled Pending pod", func() {
		// Pod is Pending but scheduled (PodScheduled=True). Before the fix,
		// extractPodFailureReason returned "" after the scheduling condition loop,
		// silencing image-pull failures that only show up in container statuses.
		cluster := buildCluster(string(cnpgv1.PhaseWaitingForInstancesToBeActive), 1, notReadyCluster)
		cluster.Status.PhaseReason = "Waiting for instances to be active"
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Labels: instancePodLabels},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				},
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull"}}},
				},
			},
		}

		status, message, publish := monitoring.ReconcileStatus(cluster, []*corev1.Pod{pod}, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.FAILED))
		Expect(message).To(ContainSubstring("ErrImagePull"))
	})
})
