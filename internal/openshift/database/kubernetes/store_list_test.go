package kubernetes_test

import (
	"context"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	cnpgstore "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var _ = Describe("K8s Store", func() {
	Describe("List Operations", func() {
		var s *cnpgstore.CnpgDatabaseStore
		var client *kubernetes.Interface
		var dynClient *dynamic.Interface

		BeforeEach(func() {
			client = new(kubernetes.Interface)
			dynClient = new(dynamic.Interface)
			s, *client, *dynClient = newTestStore(defaultConfig())
		})
		// List supports pagination over Clusters
		It("supports pagination over Clusters", func() {
			// Pre-create 5 Clusters
			for i := 0; i < 5; i++ {
				name := fmt.Sprintf("app-%d", i)
				id := fmt.Sprintf("id-%d", i)
				err := createFakeCluster(dynClient, testNS, name, id, withMajorVersion(18), withArtifacts(client, id))
				Expect(err).NotTo(HaveOccurred())
			}

			result, err := s.List(context.Background(), 2, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Results).NotTo(BeNil())
			Expect(*result.Results).To(HaveLen(2))
			Expect(result.NextPageToken).NotTo(BeNil())
			Expect(*result.NextPageToken).NotTo(BeEmpty())
		})

		// List with page_token returns subsequent page
		It("returns subsequent page with page_token", func() {
			// Pre-create 5 Deployments
			for i := 0; i < 5; i++ {
				name := fmt.Sprintf("app-%d", i)
				id := fmt.Sprintf("id-%d", i)
				err := createFakeCluster(dynClient, testNS, name, id, withArtifacts(client, id), withMajorVersion(18))
				Expect(err).NotTo(HaveOccurred())
			}

			// Get first page
			firstPage, err := s.List(context.Background(), 2, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(firstPage.NextPageToken).NotTo(BeNil())

			// Get second page
			secondPage, err := s.List(context.Background(), 2, *firstPage.NextPageToken)
			Expect(err).NotTo(HaveOccurred())
			Expect(secondPage).NotTo(BeNil())
			Expect(secondPage.Results).NotTo(BeNil())

			// Verify no overlap
			firstIDs := make(map[string]bool)
			for _, d := range *firstPage.Results {
				firstIDs[*d.Id] = true
			}
			for _, c := range *secondPage.Results {
				Expect(firstIDs).NotTo(HaveKey(*c.Id), "second page should not overlap with first")
			}
		})

		// TC-I036: List defaults to page size of 50
		It("defaults to page size of 50", func() {
			// Pre-create 75 Clusters
			for i := 0; i < 75; i++ {
				name := fmt.Sprintf("app-%03d", i)
				id := fmt.Sprintf("id-%03d", i)
				err := createFakeCluster(dynClient, testNS, name, id, withArtifacts(client, id), withMajorVersion(18))
				Expect(err).NotTo(HaveOccurred())
			}

			result, err := s.List(context.Background(), 0, "") // 0 means default
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Results).NotTo(BeNil())
			Expect(len(*result.Results)).To(BeNumerically("<=", 50))
			Expect(result.NextPageToken).NotTo(BeNil())
			Expect(*result.NextPageToken).NotTo(BeEmpty())
		})

		// List returns error for invalid page_token
		It("returns error for invalid page_token", func() {
			// Pre-create at least one Cluster so the store has data
			err := createFakeCluster(dynClient, testNS, clusterName, testId)
			Expect(err).NotTo(HaveOccurred())

			_, err = s.List(context.Background(), 10, "not-a-valid-token")

			Expect(err).To(HaveOccurred())
			Expect(store.IsInvalidArgument(err)).To(BeTrue(), "expected InvalidArgumentError, got: %v", err)
		})

		// List returns empty result when no Clusters exist
		It("returns empty result when no Deployments exist (TC-I089)", func() {
			result, err := s.List(context.Background(), 10, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Results).NotTo(BeNil())
			Expect(*result.Results).To(BeEmpty())
			Expect(result.NextPageToken).To(BeNil())
		})

		// TC-I086: List returns error for negative page_token offset
		It("returns error for negative page_token offset (TC-I086)", func() {
			err := createFakeCluster(dynClient, testNS, clusterName, testId)
			Expect(err).NotTo(HaveOccurred())

			// Encode "-5" as base64 to craft a negative offset token
			negativeToken := "LTU=" // base64("-5")
			_, err = s.List(context.Background(), 10, negativeToken)

			var invalidErr *store.InvalidArgumentError
			Expect(errors.As(err, &invalidErr)).To(BeTrue(), "expected InvalidArgumentError, got: %v", err)
		})
	})
})
