package monitoring_test

import (
	v1alpha1 "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/container/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Status Reconciliation", func() {
	// Helper to build a Deployment with optional conditions and replica count.
	buildDeployment := func(conditions []appsv1.DeploymentCondition, replicas int32) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "test-deploy"},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
			Status:     appsv1.DeploymentStatus{Conditions: conditions},
		}
	}

	// Helper to build a Pod with the given phase.
	buildPod := func(phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			Status:     corev1.PodStatus{Phase: phase},
		}
	}

	// Helper to build a Pod with the given phase and container statuses.
	buildPodWithContainerStatuses := func(phase corev1.PodPhase, containerStatuses []corev1.ContainerStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			Status:     corev1.PodStatus{Phase: phase, ContainerStatuses: containerStatuses},
		}
	}

	// Helper to build the stuck-rollout Deployment condition shared by
	// TC-U080/081/082/084.
	stuckRolloutCondition := appsv1.DeploymentCondition{
		Type:    appsv1.DeploymentProgressing,
		Status:  corev1.ConditionFalse,
		Reason:  "ProgressDeadlineExceeded",
		Message: `ReplicaSet "test-deploy-abc" has timed out progressing.`,
	}

	It("should derive status from Pod phase when both resources exist (TC-U031)", func() {
		deploy := buildDeployment([]appsv1.DeploymentCondition{
			{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue},
		}, 1)
		pod := buildPod(corev1.PodRunning)

		status, _, publish := monitoring.ReconcileStatus(deploy, pod)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.RUNNING))
	})

	It("should return PENDING when Deployment Available=False and no Pod (TC-U032)", func() {
		deploy := buildDeployment([]appsv1.DeploymentCondition{
			{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionFalse},
		}, 1)

		status, _, publish := monitoring.ReconcileStatus(deploy, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.PENDING))
	})

	It("should return FAILED when Deployment ReplicaFailure=True and no Pod (TC-U033)", func() {
		deploy := buildDeployment([]appsv1.DeploymentCondition{
			{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue},
		}, 1)

		status, _, publish := monitoring.ReconcileStatus(deploy, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.FAILED))
	})

	It("should return FAILED when Deployment Replicas=0 and no Pod (TC-U034)", func() {
		deploy := buildDeployment(nil, 0)

		status, _, publish := monitoring.ReconcileStatus(deploy, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.FAILED))
	})

	It("should return PENDING when Deployment Available=True but no Pod exists (TC-U060)", func() {
		deploy := buildDeployment([]appsv1.DeploymentCondition{
			{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue},
		}, 1)

		status, _, publish := monitoring.ReconcileStatus(deploy, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.PENDING))
	})

	It("should return DELETED when neither resource exists (TC-U035)", func() {
		status, _, publish := monitoring.ReconcileStatus(nil, nil)

		Expect(publish).To(BeTrue())
		Expect(status).To(Equal(v1alpha1.DELETED))
	})

	It("should return FAILED when Pod is Pending and Deployment has a stuck-rollout condition (TC-U080)", func() {
		deploy := buildDeployment([]appsv1.DeploymentCondition{stuckRolloutCondition}, 1)
		pod := buildPod(corev1.PodPending)

		status, message, publish := monitoring.ReconcileStatus(deploy, pod)

		Expect(status).To(Equal(v1alpha1.FAILED))
		Expect(publish).To(BeTrue())
		Expect(message).To(ContainSubstring(`ReplicaSet "test-deploy-abc" has timed out progressing.`))
	})

	It("should return FAILED with CrashLoopBackOff-enriched message when Pod is Running with a waiting container and Deployment has a stuck-rollout condition (TC-U081)", func() {
		deploy := buildDeployment([]appsv1.DeploymentCondition{stuckRolloutCondition}, 1)
		pod := buildPodWithContainerStatuses(corev1.PodRunning, []corev1.ContainerStatus{
			{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
		})

		status, message, publish := monitoring.ReconcileStatus(deploy, pod)

		Expect(status).To(Equal(v1alpha1.FAILED))
		Expect(publish).To(BeTrue())
		Expect(message).To(Equal(`ReplicaSet "test-deploy-abc" has timed out progressing. (CrashLoopBackOff)`))
	})

	It("should return FAILED with the bare condition message when pod is nil and Deployment has a stuck-rollout condition (TC-U082)", func() {
		deploy := buildDeployment([]appsv1.DeploymentCondition{stuckRolloutCondition}, 1)

		status, message, publish := monitoring.ReconcileStatus(deploy, nil)

		Expect(status).To(Equal(v1alpha1.FAILED))
		Expect(publish).To(BeTrue())
		Expect(message).To(Equal(`ReplicaSet "test-deploy-abc" has timed out progressing.`))
	})

	It("should return PENDING (not FAILED) when Pod is Pending and Deployment Progressing=True without a deadline-exceeded reason (TC-U083)", func() {
		deploy := buildDeployment([]appsv1.DeploymentCondition{
			{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue},
		}, 1)
		pod := buildPod(corev1.PodPending)

		status, _, publish := monitoring.ReconcileStatus(deploy, pod)

		Expect(status).To(Equal(v1alpha1.PENDING))
		Expect(publish).To(BeTrue())
		Expect(status).NotTo(Equal(v1alpha1.FAILED))
	})

	It("should enrich the stuck-rollout message with the Pod's waiting reason only when a Pod is present (TC-U084)", func() {
		// TC-U084a: Pod Running with CrashLoopBackOff container -> message
		// contains both the condition message and the reason, and equals
		// their exact enriched combination.
		deployWithPod := buildDeployment([]appsv1.DeploymentCondition{stuckRolloutCondition}, 1)
		podWithCrashLoop := buildPodWithContainerStatuses(corev1.PodRunning, []corev1.ContainerStatus{
			{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
		})

		statusA, messageA, publishA := monitoring.ReconcileStatus(deployWithPod, podWithCrashLoop)

		Expect(statusA).To(Equal(v1alpha1.FAILED))
		Expect(publishA).To(BeTrue())
		Expect(messageA).To(ContainSubstring(`ReplicaSet "test-deploy-abc" has timed out progressing.`))
		Expect(messageA).To(ContainSubstring("CrashLoopBackOff"))
		Expect(messageA).To(Equal(`ReplicaSet "test-deploy-abc" has timed out progressing. (CrashLoopBackOff)`))

		// TC-U084b: pod == nil -> message equals the condition's Message
		// alone, with no "(...)" suffix.
		deployNoPod := buildDeployment([]appsv1.DeploymentCondition{stuckRolloutCondition}, 1)

		statusB, messageB, publishB := monitoring.ReconcileStatus(deployNoPod, nil)

		Expect(statusB).To(Equal(v1alpha1.FAILED))
		Expect(publishB).To(BeTrue())
		Expect(messageB).To(Equal(`ReplicaSet "test-deploy-abc" has timed out progressing.`))
	})
})
