package cloudevent_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/cloudevent"
)

var _ = Describe("NewCloudEvent", Label("unit"), func() {
	It("includes all required CE fields with correct values (UT-XC-CE-010)", func() {
		event, err := cloudevent.NewCloudEvent("my-agent-456", "dcm.status.create")
		Expect(err).NotTo(HaveOccurred())
		Expect(event.SpecVersion()).To(Equal("1.0"))
		Expect(event.ID()).NotTo(BeEmpty())
		Expect(event.Source()).To(Equal("dcm/agents/my-agent-456"))
		Expect(event.Type()).To(Equal("dcm.status.create"))
		Expect(event.Time().IsZero()).To(BeFalse())
	})

	It("produces distinct IDs on successive calls (UT-XC-CE-030)", func() {
		e1, err1 := cloudevent.NewCloudEvent("agent-a", "dcm.test")
		Expect(err1).NotTo(HaveOccurred())
		e2, err2 := cloudevent.NewCloudEvent("agent-a", "dcm.test")
		Expect(err2).NotTo(HaveOccurred())
		Expect(e1.ID()).NotTo(Equal(e2.ID()))
	})
})

var _ = Describe("FormatSource", Label("unit"), func() {
	It("formats source as dcm/agents/{agentId} (UT-XC-CE-020)", func() {
		result, err := cloudevent.FormatSource("my-agent-456")
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("dcm/agents/my-agent-456"))
	})

	It("rejects empty agentId (UT-XC-CE-021)", func() {
		_, err := cloudevent.FormatSource("")
		Expect(err).To(MatchError(ContainSubstring("empty")))
	})

	It("preserves special characters in agentId (UT-XC-CE-022)", func() {
		result, err := cloudevent.FormatSource("special!chars")
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("dcm/agents/special!chars"))
	})
})
