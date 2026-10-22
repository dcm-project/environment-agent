package database_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	databaseapi "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/embedded/database"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	"github.com/dcm-project/environment-agent/internal/routing"
)

func TestDatabase(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embedded Database Suite")
}

type fakeDatabaseStore struct {
	createErr error
	deleteErr error
	lastID    string
	lastSpec  databaseapi.DatabaseSpec
}

func (f *fakeDatabaseStore) Create(_ context.Context, spec databaseapi.DatabaseSpec, id string) (*databaseapi.Database, error) {
	f.lastID = id
	f.lastSpec = spec
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &databaseapi.Database{Id: &id}, nil
}

func (f *fakeDatabaseStore) Delete(_ context.Context, id string) error {
	f.lastID = id
	return f.deleteErr
}

func (f *fakeDatabaseStore) CheckHealth(context.Context) error {
	return nil
}

func validDatabaseSpec() databaseapi.DatabaseSpec {
	return databaseapi.DatabaseSpec{
		ServiceType: databaseapi.DatabaseSpecServiceTypeDatabase,
		Metadata: databaseapi.DatabaseMetadata{
			Name: "my-database",
		},
		Resources: databaseapi.DatabaseResources{
			Cpu: databaseapi.DatabaseCpu{Min: "1000m", Max: "2000m"},
			Memory: databaseapi.DatabaseMemory{
				Min: "1GB",
				Max: "2GB",
			},
			Storage: "30GB",
		},
		Engine: databaseapi.Postgresql,
	}
}

var _ = Describe("Handler", func() {
	var (
		repo    *fakeDatabaseStore
		handler routing.EmbeddedHandler
	)

	BeforeEach(func() {
		repo = &fakeDatabaseStore{}
		handler = database.NewDatabaseHandler(repo)
	})

	It("creates a database from a wrapped resource spec", func() {
		spec, err := json.Marshal(databaseapi.Database{
			Spec: validDatabaseSpec(),
		})
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "database-1",
			ServiceType: database.ServiceType,
			Spec:        spec,
			EventID:     "de-1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("database-1"))
		Expect(repo.lastSpec.Metadata.Name).To(Equal("my-database"))
	})

	It("creates a database from a bare DatabaseSpec", func() {
		spec, err := json.Marshal(validDatabaseSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "database-2",
			ServiceType: database.ServiceType,
			Spec:        spec,
			EventID:     "de-2",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("database-2"))
		Expect(repo.lastSpec.Metadata.Name).To(Equal("my-database"))
	})

	It("rejects an empty wrapped spec", func() {
		spec, err := json.Marshal(databaseapi.Database{})
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "database-3",
			ServiceType: database.ServiceType,
			Spec:        spec,
			EventID:     "de-3",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusBadRequest))
		Expect(repo.lastID).To(BeEmpty())
	})

	It("rejects reserved DCM labels before create", func() {
		badSpec := validDatabaseSpec()
		labels := map[string]string{"dcm.project/managed-by": "user"}
		badSpec.Metadata.Labels = &labels
		spec, err := json.Marshal(badSpec)
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "database-4",
			ServiceType: database.ServiceType,
			Spec:        spec,
			EventID:     "de-4",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusBadRequest))
		Expect(spErr.Message).To(ContainSubstring("reserved by DCM"))
		Expect(repo.lastID).To(BeEmpty())
	})

	It("rejects an uppercase metadata.name before store creation", func() {
		badSpec := validDatabaseSpec()
		badSpec.Metadata.Name = "MyDatabase"
		spec, err := json.Marshal(badSpec)
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "database-5",
			ServiceType: database.ServiceType,
			Spec:        spec,
			EventID:     "de-5",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusBadRequest))
		Expect(spErr.Message).To(ContainSubstring("metadata.name"))
		Expect(repo.lastID).To(BeEmpty())
	})

	It("rejects an invalid resource ID before store creation", func() {
		spec, err := json.Marshal(validDatabaseSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "INVALID_ID",
			ServiceType: database.ServiceType,
			Spec:        spec,
			EventID:     "de-6",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusBadRequest))
		Expect(spErr.Message).To(ContainSubstring("database ID"))
		Expect(repo.lastID).To(BeEmpty())
	})

	It("maps store errors to SP response errors", func() {
		repo.createErr = &store.ConflictError{Message: "already exists"}

		spec, err := json.Marshal(validDatabaseSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "database-1",
			ServiceType: database.ServiceType,
			Spec:        spec,
			EventID:     "de-1",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusConflict))
	})

	It("maps not-found delete errors", func() {
		repo.deleteErr = &store.NotFoundError{ID: "database-1"}

		err := handler.DeleteResource(context.Background(), routing.DeleteResourceRequest{
			ResourceID:  "database-1",
			ServiceType: database.ServiceType,
			EventID:     "de-3",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusNotFound))
	})

	It("deletes by resource ID", func() {
		err := handler.DeleteResource(context.Background(), routing.DeleteResourceRequest{
			ResourceID:  "database-1",
			ServiceType: database.ServiceType,
			EventID:     "de-4",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("database-1"))
	})
})
