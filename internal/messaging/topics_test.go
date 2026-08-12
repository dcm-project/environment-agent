package messaging_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/messaging"
)

var _ = Describe("DeriveTopicNames", Label("unit"), func() {
	DescribeTable("derives base, main, retry, and cancel topic names",
		func(agentName, override, expectBase, expectMain, expectRetry, expectCancel string) {
			result := messaging.DeriveTopicNames(agentName, override)
			Expect(result.Base).To(Equal(expectBase))
			Expect(result.Main).To(Equal(expectMain))
			Expect(result.Retry).To(Equal(expectRetry))
			Expect(result.Cancel).To(Equal(expectCancel))
		},
		Entry("from agent name when no override (UT-MSG-010)",
			"agent-prod-1", "", "agent-prod-1", "dcm.agent.agent-prod-1", "agent-prod-1.retry", "dcm.agent.agent-prod-1.cancel"),
		Entry("explicit topic name overrides agent name (UT-MSG-020)",
			"agent-prod-1", "custom-topic", "custom-topic", "dcm.agent.custom-topic", "custom-topic.retry", "dcm.agent.custom-topic.cancel"),
	)

	It("derives deterministic consumer/stream names from Base (UT-MSG-025)", func() {
		result := messaging.DeriveTopicNames("agent-prod-1", "")
		Expect(result.MainConsumer()).To(Equal("agent-prod-1-consumer"))
		Expect(result.CancelConsumer()).To(Equal("agent-prod-1-cancel-consumer"))
		Expect(result.RetryStream()).To(Equal("agent-prod-1-retry"))
		Expect(result.RetryConsumer()).To(Equal("agent-prod-1-retry-consumer"))
	})
})

var _ = Describe("ValidateTopicName", Label("unit"), func() {
	DescribeTable("enforces NATS subject token rules",
		func(name, errSubstring string) {
			err := messaging.ValidateTopicName(name)
			if errSubstring == "" {
				Expect(err).NotTo(HaveOccurred())
			} else {
				Expect(err).To(MatchError(ContainSubstring(errSubstring)))
			}
		},
		Entry("rejects space (UT-MSG-030)", "agent prod", "invalid characters"),
		Entry("rejects wildcard * (UT-MSG-031)", "agent.*", "invalid characters"),
		Entry("rejects full wildcard > (UT-MSG-032)", "agent>", "invalid characters"),
		Entry("rejects exceeding 255 chars (UT-MSG-033)", strings.Repeat("a", 256), "255"),
		Entry("accepts dot separator (UT-MSG-034)", "agent-prod.1", ""),
		Entry("accepts alphanum + hyphens (UT-MSG-035)", "agent-prod-1", ""),
		Entry("rejects empty string (UT-MSG-036)", "", "empty"),
		Entry("rejects ! (UT-MSG-037)", "test!", "invalid"),
		Entry("rejects @ (UT-MSG-038)", "test@", "invalid"),
		Entry("rejects / (UT-MSG-039)", "test/a", "invalid"),
		Entry("accepts underscore (UT-MSG-040)", "test_topic", ""),
		Entry("accepts 255-char boundary (UT-MSG-041)", strings.Repeat("a", 255), ""),
		Entry("rejects reserved dcm.agent. prefix (UT-MSG-042)", "dcm.agent.foo", "reserved"),
		Entry("rejects exact reserved base dcm.agent with no trailing dot (UT-MSG-043)", "dcm.agent", "reserved"),
		Entry("rejects leading dot (UT-MSG-044)", ".agent-prod", "empty dot-separated tokens"),
		Entry("rejects trailing dot (UT-MSG-045)", "agent-prod.", "empty dot-separated tokens"),
		Entry("rejects consecutive dots (UT-MSG-046)", "agent..prod", "empty dot-separated tokens"),
	)
})
