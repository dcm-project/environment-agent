package store_test

import (
	"context"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/provider/store"
)

var _ = Describe("FileStore", Label("unit"), func() {
	var (
		fs      *store.FileStore
		tmpDir  string
		ctx     context.Context
		validSP store.StoredProvider
	)

	BeforeEach(func() {
		ctx = context.Background()
		tmpDir = GinkgoT().TempDir()
		var err error
		fs, err = store.NewFileStore(filepath.Join(tmpDir, "providers.json"))
		Expect(err).NotTo(HaveOccurred())

		validSP = store.StoredProvider{
			ID:            "sp-001",
			Name:          "my-provider",
			Endpoint:      "http://localhost:8080",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
			Type:          "external",
			CreateTime:    time.Now(),
			UpdateTime:    time.Now(),
		}
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("Save", func() {
		It("rejects provider with empty ID", func() {
			sp := validSP
			sp.ID = ""
			err := fs.Save(ctx, sp)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid provider record"))
			Expect(err.Error()).To(ContainSubstring("missing id"))
		})

		It("rejects provider with empty Name", func() {
			sp := validSP
			sp.Name = ""
			err := fs.Save(ctx, sp)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid provider record"))
			Expect(err.Error()).To(ContainSubstring("missing name"))
		})

		It("accepts valid provider and persists it", func() {
			err := fs.Save(ctx, validSP)
			Expect(err).NotTo(HaveOccurred())

			providers, err := fs.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(providers).To(HaveLen(1))
			Expect(providers[0].ID).To(Equal("sp-001"))
		})
	})
})
