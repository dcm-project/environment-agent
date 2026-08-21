package acmcluster_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	acmv1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/embedded/acmcluster"
	"github.com/dcm-project/environment-agent/internal/routing"
)

func TestACMCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embedded ACM Cluster Suite")
}

type fakeClusterService struct {
	createErr error
	deleteErr error
	lastID    string
	lastSpec  acmv1.Cluster
}

func (f *fakeClusterService) Create(_ context.Context, id string, cluster acmv1.Cluster) (*acmv1.Cluster, error) {
	f.lastID = id
	f.lastSpec = cluster
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &acmv1.Cluster{Id: &id}, nil
}

func (f *fakeClusterService) Get(context.Context, string) (*acmv1.Cluster, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeClusterService) List(context.Context, int, string) (*acmv1.ClusterList, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeClusterService) Delete(_ context.Context, id string) error {
	f.lastID = id
	return f.deleteErr
}

var _ = Describe("Handler", func() {
	var (
		svc     *fakeClusterService
		handler *acmcluster.Handler
	)

	BeforeEach(func() {
		svc = &fakeClusterService{}
		handler = acmcluster.NewHandler(svc, nil)
	})

	It("creates a cluster from the inbound spec", func() {
		spec, err := json.Marshal(acmv1.Cluster{
			Spec: acmv1.ClusterSpec{
				Metadata: acmv1.ClusterMetadata{Name: "test-cluster"},
			},
		})
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "cluster-1",
			ServiceType: acmcluster.ServiceType,
			Spec:        spec,
			EventID:     "ce-1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(svc.lastID).To(Equal("cluster-1"))
		Expect(svc.lastSpec.Spec.Metadata.Name).To(Equal("test-cluster"))
	})

	It("maps service errors to SP response errors on create", func() {
		svc.createErr = errors.New("boom")
		spec, err := json.Marshal(acmv1.Cluster{
			Spec: acmv1.ClusterSpec{
				Metadata: acmv1.ClusterMetadata{Name: "dup"},
			},
		})
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID: "cluster-1",
			Spec:       spec,
		})
		var spErr *routing.SPResponseError
		Expect(errors.As(err, &spErr)).To(BeTrue())
		Expect(spErr.StatusCode).To(Equal(http.StatusInternalServerError))
	})

	It("deletes a cluster by resource ID", func() {
		err := handler.DeleteResource(context.Background(), routing.DeleteResourceRequest{
			ResourceID: "cluster-9",
			EventID:    "ce-9",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(svc.lastID).To(Equal("cluster-9"))
	})
})

var _ = Describe("Enabled", func() {
	It("detects cluster in AGENT_EMBEDDED_SPS", func() {
		Expect(acmcluster.Enabled([]string{"container", "cluster"})).To(BeTrue())
		Expect(acmcluster.Enabled([]string{"container"})).To(BeFalse())
		Expect(acmcluster.Enabled(nil)).To(BeFalse())
	})
})
