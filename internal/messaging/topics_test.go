package messaging_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/messaging"
)

var _ = Describe("DeriveTopicNames", Label("unit"), func() {
	DescribeTable("derives main, retry, and cancel topic names",
		func(agentName, override, expectMain, expectRetry, expectCancel string) {
			result := messaging.DeriveTopicNames(agentName, override)
			Expect(result.Main).To(Equal(expectMain))
			Expect(result.Retry).To(Equal(expectRetry))
			Expect(result.Cancel).To(Equal(expectCancel))
		},
		Entry("from agent name when no override (UT-MSG-010)",
			"agent-prod-1", "", "agent-prod-1", "agent-prod-1.retry", "agent-prod-1.cancel"),
		Entry("explicit topic name overrides agent name (UT-MSG-020)",
			"agent-prod-1", "custom-topic", "custom-topic", "custom-topic.retry", "custom-topic.cancel"),
	)
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
	)
})
