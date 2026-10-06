package monitoring_test

import (
	"encoding/json"

	v1alpha1 "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/network/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func ceData(raw []byte) map[string]any {
	var ce struct {
		Data map[string]any `json:"data"`
	}
	Expect(json.Unmarshal(raw, &ce)).To(Succeed())
	return ce.Data
}

var _ = Describe("NewStatusCloudEvent", func() {
	It("TC-U130: includes output_spec.endpoints in the payload when outputs are present", func() {
		outputSpec := map[string]any{
			"endpoints": []map[string]any{{"address": "34.123.45.67", "port": 80, "scope": "external"}},
		}

		data, err := monitoring.NewStatusCloudEvent("dcm.network", testProviderName, "network-123", v1alpha1.READY, "service is ready", outputSpec)
		Expect(err).NotTo(HaveOccurred())

		Expect(ceData(data)).To(HaveKeyWithValue("output_spec", map[string]any{
			"endpoints": []any{map[string]any{"address": "34.123.45.67", "port": 80.0, "scope": "external"}},
		}))
	})

	It("TC-U131: omits output_spec from the payload when outputs are nil", func() {
		data, err := monitoring.NewStatusCloudEvent("dcm.network", testProviderName, "network-123", v1alpha1.PENDING, "waiting for external IP assignment", nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(ceData(data)).NotTo(HaveKey("output_spec"))
	})
})
