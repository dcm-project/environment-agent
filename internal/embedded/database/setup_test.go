package database_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/embedded/database"
)

var _ = Describe("Enabled", Label("unit"), func() {
	It("returns true when database is listed in AGENT_EMBEDDED_SPS", func() {
		Expect(database.Enabled([]string{"container", "database"})).To(BeTrue())
	})

	It("returns false when database is not listed", func() {
		Expect(database.Enabled([]string{"container", "cluster"})).To(BeFalse())
		Expect(database.Enabled(nil)).To(BeFalse())
	})
})
