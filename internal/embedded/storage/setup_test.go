package storage_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/embedded/storage"
)

var _ = Describe("Enabled", Label("unit"), func() {
	It("returns true when storage is listed in AGENT_EMBEDDED_SPS", func() {
		Expect(storage.Enabled([]string{"container", "storage"})).To(BeTrue())
	})

	It("returns false when storage is not listed", func() {
		Expect(storage.Enabled([]string{"container", "cluster"})).To(BeFalse())
		Expect(storage.Enabled(nil)).To(BeFalse())
	})
})
