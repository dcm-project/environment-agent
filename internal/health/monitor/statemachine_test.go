package monitor_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
)

var _ = Describe("Health State Machine", Label("unit"), func() {
	Describe("Failure counter and Unavailable transition", func() {
		It("transitions to Unavailable after failureThreshold consecutive failures (UT-HMN-010)", func() {
			sm := monitor.NewStateMachine(3, v1alpha1.Ready)

			sm.RecordResult(monitor.CheckFailed)
			sm.RecordResult(monitor.CheckFailed)
			sm.RecordResult(monitor.CheckFailed)

			Expect(sm.State()).To(Equal(v1alpha1.Unavailable))
		})

		It("remains Ready when failures below threshold (UT-HMN-011)", func() {
			sm := monitor.NewStateMachine(3, v1alpha1.Ready)

			sm.RecordResult(monitor.CheckFailed)
			sm.RecordResult(monitor.CheckFailed)

			Expect(sm.State()).To(Equal(v1alpha1.Ready))
			Expect(sm.FailureCounter()).To(Equal(2))
		})

		It("transitions immediately with threshold=1 (UT-HMN-012)", func() {
			sm := monitor.NewStateMachine(1, v1alpha1.Ready)

			sm.RecordResult(monitor.CheckFailed)

			Expect(sm.State()).To(Equal(v1alpha1.Unavailable))
		})
	})

	Describe("Healthy response behavior", func() {
		It("resets failure counter on healthy response (UT-HMN-020)", func() {
			sm := monitor.NewStateMachine(3, v1alpha1.Ready)

			sm.RecordResult(monitor.CheckFailed)
			sm.RecordResult(monitor.CheckFailed)
			Expect(sm.FailureCounter()).To(Equal(2), "precondition: counter must be 2 after 2 failures")

			sm.RecordResult(monitor.CheckHealthy)
			Expect(sm.FailureCounter()).To(Equal(0))
			Expect(sm.State()).To(Equal(v1alpha1.Ready))
		})
	})

	Describe("Unhealthy response behavior", func() {
		It("does NOT increment failure counter on unhealthy response (UT-HMN-030)", func() {
			sm := monitor.NewStateMachine(3, v1alpha1.Ready)

			sm.RecordResult(monitor.CheckFailed)
			sm.RecordResult(monitor.CheckFailed)
			sm.RecordResult(monitor.CheckUnhealthy)

			Expect(sm.FailureCounter()).To(Equal(0))
			Expect(sm.State()).To(Equal(v1alpha1.Unhealthy))
		})
	})

	Describe("Recovery transitions", func() {
		It("transitions from Unhealthy to Ready on healthy response (UT-HMN-040)", func() {
			sm := monitor.NewStateMachine(3, v1alpha1.Unhealthy)

			sm.RecordResult(monitor.CheckHealthy)

			Expect(sm.State()).To(Equal(v1alpha1.Ready))
			Expect(sm.FailureCounter()).To(Equal(0))
		})
	})

	Describe("Failure during Unhealthy state", func() {
		It("increments counter during Unhealthy state (UT-HMN-050)", func() {
			sm := monitor.NewStateMachine(3, v1alpha1.Unhealthy)

			sm.RecordResult(monitor.CheckFailed)
			Expect(sm.FailureCounter()).To(Equal(1), "precondition: counter at 1")
			Expect(sm.State()).To(Equal(v1alpha1.Unhealthy), "precondition: still Unhealthy")

			sm.RecordResult(monitor.CheckFailed)
			Expect(sm.FailureCounter()).To(Equal(2))
			Expect(sm.State()).To(Equal(v1alpha1.Unhealthy))
		})
	})

	Describe("Unavailable recovery", func() {
		It("transitions from Unavailable to Ready on healthy (UT-HMN-060)", func() {
			sm := monitor.NewStateMachine(3, v1alpha1.Ready)

			// Drive to Unavailable via threshold failures (counter should reach 3)
			sm.RecordResult(monitor.CheckFailed)
			sm.RecordResult(monitor.CheckFailed)
			sm.RecordResult(monitor.CheckFailed)
			Expect(sm.State()).To(Equal(v1alpha1.Unavailable), "precondition: must reach Unavailable")

			sm.RecordResult(monitor.CheckHealthy)

			Expect(sm.State()).To(Equal(v1alpha1.Ready))
			Expect(sm.FailureCounter()).To(Equal(0))
		})

		It("transitions from Unavailable to Unhealthy on unhealthy (UT-HMN-065)", func() {
			sm := monitor.NewStateMachine(3, v1alpha1.Ready)

			// Drive to Unavailable via threshold failures (counter should reach 3)
			sm.RecordResult(monitor.CheckFailed)
			sm.RecordResult(monitor.CheckFailed)
			sm.RecordResult(monitor.CheckFailed)
			Expect(sm.State()).To(Equal(v1alpha1.Unavailable), "precondition: must reach Unavailable")

			sm.RecordResult(monitor.CheckUnhealthy)

			Expect(sm.State()).To(Equal(v1alpha1.Unhealthy))
			Expect(sm.FailureCounter()).To(Equal(0))
		})
	})
})
