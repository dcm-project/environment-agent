package storage_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	storageapi "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/embedded/storage"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/store"
	"github.com/dcm-project/environment-agent/internal/routing"
)

func TestStorage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embedded Storage Suite")
}

type fakeVolumeStore struct {
	createErr error
	deleteErr error
	lastID    string
	lastSpec  storageapi.StorageSpec
}

func (f *fakeVolumeStore) Create(_ context.Context, spec storageapi.StorageSpec, id string) (*storageapi.Volume, error) {
	f.lastID = id
	f.lastSpec = spec
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &storageapi.Volume{Id: &id}, nil
}

func (f *fakeVolumeStore) Get(context.Context, string) (*storageapi.Volume, error) {
	return nil, nil
}

func (f *fakeVolumeStore) List(context.Context, int32, string) (*storageapi.VolumeList, error) {
	return nil, nil
}

func (f *fakeVolumeStore) Delete(_ context.Context, id string) error {
	f.lastID = id
	return f.deleteErr
}

func (f *fakeVolumeStore) CheckHealth(context.Context) error {
	return nil
}

func validStorageSpec() storageapi.StorageSpec {
	return storageapi.StorageSpec{
		ServiceType: storageapi.Storage,
		Capacity:    "10Gi",
		Metadata: storageapi.VolumeMetadata{
			Name: "my-volume",
		},
	}
}

var _ = Describe("Handler", func() {
	var (
		repo    *fakeVolumeStore
		handler routing.EmbeddedHandler
	)

	BeforeEach(func() {
		repo = &fakeVolumeStore{}
		handler = storage.NewStorageHandler(repo)
	})

	It("creates a volume from a wrapped resource spec", func() {
		spec, err := json.Marshal(storageapi.Volume{
			Spec: validStorageSpec(),
		})
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "volume-1",
			ServiceType: storage.ServiceType,
			Spec:        spec,
			EventID:     "ce-1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("volume-1"))
		Expect(repo.lastSpec.ServiceType).To(Equal(storageapi.Storage))
		Expect(repo.lastSpec.Capacity).To(Equal("10Gi"))
		Expect(repo.lastSpec.Metadata.Name).To(Equal("volume-1"))
		Expect(repo.lastSpec.ProviderHints).To(BeNil())
	})

	It("creates a volume from a bare StorageSpec", func() {
		spec, err := json.Marshal(validStorageSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "volume-2",
			ServiceType: storage.ServiceType,
			Spec:        spec,
			EventID:     "ce-2",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("volume-2"))
		Expect(repo.lastSpec.ServiceType).To(Equal(storageapi.Storage))
		Expect(repo.lastSpec.Capacity).To(Equal("10Gi"))
		Expect(repo.lastSpec.Metadata.Name).To(Equal("volume-2"))
		Expect(repo.lastSpec.ProviderHints).To(BeNil())
	})

	It("rejects reserved volume id health", func() {
		spec, err := json.Marshal(validStorageSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "health",
			ServiceType: storage.ServiceType,
			Spec:        spec,
			EventID:     "ce-3",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusBadRequest))
	})

	It("maps store not found on delete", func() {
		repo.deleteErr = &store.NotFoundError{ID: "missing"}
		err := handler.DeleteResource(context.Background(), routing.DeleteResourceRequest{
			ResourceID:  "missing",
			ServiceType: storage.ServiceType,
			EventID:     "ce-4",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusNotFound))
	})
})
