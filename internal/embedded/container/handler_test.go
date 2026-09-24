package container_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	containerapi "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/embedded/container"
	"github.com/dcm-project/environment-agent/internal/openshift/container/store"
	"github.com/dcm-project/environment-agent/internal/routing"
)

func TestContainer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embedded Container Suite")
}

type fakeContainerStore struct {
	createErr error
	deleteErr error
	lastID    string
	lastSpec  containerapi.ContainerSpec
}

func (f *fakeContainerStore) Create(_ context.Context, spec containerapi.ContainerSpec, id string) (*containerapi.Container, error) {
	f.lastID = id
	f.lastSpec = spec
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &containerapi.Container{Id: &id}, nil
}

func (f *fakeContainerStore) Delete(_ context.Context, id string) error {
	f.lastID = id
	return f.deleteErr
}

func (f *fakeContainerStore) CheckHealth(context.Context) error {
	return nil
}

func validContainerSpec() containerapi.ContainerSpec {
	return containerapi.ContainerSpec{
		ServiceType: containerapi.ContainerSpecServiceTypeContainer,
		Metadata: containerapi.ContainerMetadata{
			Name: "my-container",
		},
		Image: containerapi.ContainerImage{
			Reference: "nginx:latest",
		},
		Resources: containerapi.ContainerResources{
			Cpu: containerapi.ContainerCpu{Min: "1000m", Max: "2000m"},
			Memory: containerapi.ContainerMemory{
				Min: "1GB",
				Max: "2GB",
			},
		},
	}
}

var _ = Describe("Handler", func() {
	var (
		repo    *fakeContainerStore
		handler routing.EmbeddedHandler
	)

	BeforeEach(func() {
		repo = &fakeContainerStore{}
		handler = container.NewContainerHandler(repo)
	})

	It("creates a container from a wrapped resource spec", func() {
		spec, err := json.Marshal(containerapi.Container{
			Spec: validContainerSpec(),
		})
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "container-1",
			ServiceType: container.ServiceType,
			Spec:        spec,
			EventID:     "ce-1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("container-1"))
		Expect(repo.lastSpec.Metadata.Name).To(Equal("my-container"))
	})

	It("creates a container from a bare ContainerSpec", func() {
		spec, err := json.Marshal(validContainerSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "container-2",
			ServiceType: container.ServiceType,
			Spec:        spec,
			EventID:     "ce-2",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("container-2"))
		Expect(repo.lastSpec.Metadata.Name).To(Equal("my-container"))
	})

	It("rejects an empty wrapped spec", func() {
		spec, err := json.Marshal(containerapi.Container{})
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "container-3",
			ServiceType: container.ServiceType,
			Spec:        spec,
			EventID:     "ce-3",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusBadRequest))
		Expect(repo.lastID).To(BeEmpty())
	})

	It("rejects reserved DCM labels before create", func() {
		badSpec := validContainerSpec()
		labels := map[string]string{"dcm.project/managed-by": "user"}
		badSpec.Metadata.Labels = &labels
		spec, err := json.Marshal(badSpec)
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "container-4",
			ServiceType: container.ServiceType,
			Spec:        spec,
			EventID:     "ce-4",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusBadRequest))
		Expect(spErr.Message).To(ContainSubstring("reserved by DCM"))
		Expect(repo.lastID).To(BeEmpty())
	})

	It("maps store errors to SP response errors", func() {
		repo.createErr = &store.ConflictError{Message: "already exists"}

		spec, err := json.Marshal(validContainerSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "container-1",
			ServiceType: container.ServiceType,
			Spec:        spec,
			EventID:     "ce-1",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusConflict))
	})

	It("maps not-found delete errors", func() {
		repo.deleteErr = &store.NotFoundError{ID: "container-1"}

		err := handler.DeleteResource(context.Background(), routing.DeleteResourceRequest{
			ResourceID:  "container-1",
			ServiceType: container.ServiceType,
			EventID:     "ce-3",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusNotFound))
	})

	It("deletes by resource ID", func() {
		err := handler.DeleteResource(context.Background(), routing.DeleteResourceRequest{
			ResourceID:  "container-1",
			ServiceType: container.ServiceType,
			EventID:     "ce-4",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("container-1"))
	})

	It("detects container in AGENT_EMBEDDED_SPS", func() {
		Expect(container.Enabled([]string{"cluster", "container"})).To(BeTrue())
		Expect(container.Enabled([]string{"cluster"})).To(BeFalse())
		Expect(container.Enabled(nil)).To(BeFalse())
	})
})
