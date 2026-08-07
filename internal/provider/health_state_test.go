package provider_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/provider"
)

var _ = Describe("InMemoryHealthTracker", Label("unit"), func() {
	It("returns stored state and false for unknown IDs", func() {
		tracker := provider.NewInMemoryHealthTracker()
		now := time.Now().UTC()

		tracker.SetState("sp-1", v1alpha1.Ready, now)

		state, ok := tracker.GetState("sp-1")
		Expect(ok).To(BeTrue())
		Expect(state.Status).To(Equal(v1alpha1.Ready))
		Expect(state.LastCheckTime).To(Equal(now))

		_, ok = tracker.GetState("unknown")
		Expect(ok).To(BeFalse())
	})

	It("overwrites previous state for the same ID", func() {
		tracker := provider.NewInMemoryHealthTracker()
		t1 := time.Now().UTC()
		t2 := t1.Add(time.Second)

		tracker.SetState("sp-1", v1alpha1.Ready, t1)
		tracker.SetState("sp-1", v1alpha1.Unhealthy, t2)

		state, ok := tracker.GetState("sp-1")
		Expect(ok).To(BeTrue())
		Expect(state.Status).To(Equal(v1alpha1.Unhealthy))
		Expect(state.LastCheckTime).To(Equal(t2))
	})

	It("SetState with empty ID is a no-op", func() {
		tracker := provider.NewInMemoryHealthTracker()
		now := time.Now().UTC()

		tracker.SetState("", v1alpha1.Ready, now)

		_, ok := tracker.GetState("")
		Expect(ok).To(BeFalse())
	})

	It("removes state and is idempotent for missing IDs", func() {
		tracker := provider.NewInMemoryHealthTracker()
		now := time.Now().UTC()

		tracker.SetState("sp-1", v1alpha1.Ready, now)
		tracker.DeleteState("sp-1")

		_, ok := tracker.GetState("sp-1")
		Expect(ok).To(BeFalse())

		tracker.DeleteState("nonexistent")
	})
})
