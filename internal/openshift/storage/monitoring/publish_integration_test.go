package monitoring_test

import (
	"encoding/json"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Status Monitor", func() {
	Describe("NATS Publishing", func() {
		It("should construct a valid CloudEvent with correct fields (TC-I047)", func() {
			data, err := monitoring.NewStatusCloudEvent("dcm.storage", "k8s-storage-sp", "abc-123", v1alpha1.RUNNING, "PVC is bound")
			Expect(err).NotTo(HaveOccurred())
			Expect(data).NotTo(BeNil())

			var ce map[string]any
			Expect(json.Unmarshal(data, &ce)).To(Succeed())

			Expect(ce).To(HaveKeyWithValue("specversion", "1.0"))
			Expect(ce).To(HaveKeyWithValue("source", "dcm/providers/k8s-storage-sp"))
			Expect(ce).To(HaveKeyWithValue("type", "dcm.status.storage"))
			Expect(ce).To(HaveKeyWithValue("datacontenttype", "application/json"))
			Expect(ce).To(HaveKeyWithValue("subject", "dcm.storage"))
			Expect(ce).To(HaveKey("id"))
			Expect(ce).To(HaveKey("time"))

			ceData, ok := ce["data"].(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(ceData).To(HaveKeyWithValue("id", "abc-123"))
			Expect(ceData).To(HaveKeyWithValue("status", "RUNNING"))
			Expect(ceData).To(HaveKeyWithValue("message", "PVC is bound"))
		})

		It("should include failure reason in FAILED event message (TC-I048)", func() {
			data, err := monitoring.NewStatusCloudEvent("dcm.storage", "k8s-storage-sp", "abc-123", v1alpha1.FAILED, "PVC is lost")
			Expect(err).NotTo(HaveOccurred())

			var ce map[string]any
			Expect(json.Unmarshal(data, &ce)).To(Succeed())

			ceData, ok := ce["data"].(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(ceData).To(HaveKeyWithValue("status", "FAILED"))
			Expect(ceData["message"]).To(ContainSubstring("PVC is lost"))
		})
	})
})
