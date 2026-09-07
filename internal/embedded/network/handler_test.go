package network_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	networkapi "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/embedded/network"
	"github.com/dcm-project/environment-agent/internal/openshift/network/store"
	"github.com/dcm-project/environment-agent/internal/routing"
)

func TestNetwork(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embedded Network Suite")
}

type fakeNetworkStore struct {
	createErr error
	deleteErr error
	lastID    string
	lastSpec  networkapi.NetworkSpec
}

func (f *fakeNetworkStore) Create(_ context.Context, spec networkapi.NetworkSpec, id string) (*networkapi.Network, error) {
	f.lastID = id
	f.lastSpec = spec
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &networkapi.Network{Id: &id}, nil
}

func (f *fakeNetworkStore) Get(context.Context, string) (*networkapi.Network, error) {
	return nil, nil
}

func (f *fakeNetworkStore) List(context.Context, int32, string) (*networkapi.NetworkList, error) {
	return nil, nil
}

func (f *fakeNetworkStore) Delete(_ context.Context, id string) error {
	f.lastID = id
	return f.deleteErr
}

func (f *fakeNetworkStore) CheckHealth(context.Context) error {
	return nil
}

func validNetworkSpec() networkapi.NetworkSpec {
	portName := "http"
	return networkapi.NetworkSpec{
		ServiceType: networkapi.NetworkSpecServiceTypeNetwork,
		Ports: []networkapi.PortSpec{
			{
				Name: &portName,
				Port: 8080,
			},
		},
		Metadata: networkapi.NetworkMetadata{
			Name: "my-network",
		},
	}
}

var _ = Describe("Handler", func() {
	var (
		repo    *fakeNetworkStore
		handler routing.EmbeddedHandler
	)

	BeforeEach(func() {
		repo = &fakeNetworkStore{}
		handler = network.NewNetworkHandler(repo)
	})

	It("creates a network from a wrapped resource spec", func() {
		spec, err := json.Marshal(networkapi.Network{
			Spec: validNetworkSpec(),
		})
		Expect(err).NotTo(HaveOccurred())

		req := routing.CreateResourceRequest{
			ResourceID: "my-network",
			Spec:       json.RawMessage(spec),
		}
		err = handler.CreateResource(context.Background(), req)
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("my-network"))
		Expect(repo.lastSpec.ServiceType).To(Equal(networkapi.NetworkSpecServiceTypeNetwork))
	})

	It("creates a network from a plain spec", func() {
		spec, err := json.Marshal(validNetworkSpec())
		Expect(err).NotTo(HaveOccurred())

		req := routing.CreateResourceRequest{
			ResourceID: "test-network",
			Spec:       json.RawMessage(spec),
		}
		err = handler.CreateResource(context.Background(), req)
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("test-network"))
	})

	It("rejects reserved network id health", func() {
		spec, err := json.Marshal(validNetworkSpec())
		Expect(err).NotTo(HaveOccurred())

		req := routing.CreateResourceRequest{
			ResourceID: "health",
			Spec:       json.RawMessage(spec),
		}
		err = handler.CreateResource(context.Background(), req)
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusBadRequest))
	})

	It("deletes a network", func() {
		req := routing.DeleteResourceRequest{
			ResourceID: "my-network",
		}
		err := handler.DeleteResource(context.Background(), req)
		Expect(err).NotTo(HaveOccurred())
		Expect(repo.lastID).To(Equal("my-network"))
	})

	It("maps NotFoundError to 404", func() {
		repo.deleteErr = &store.NotFoundError{Resource: "network", ID: "missing"}
		req := routing.DeleteResourceRequest{
			ResourceID: "missing",
		}
		err := handler.DeleteResource(context.Background(), req)
		var spErr *routing.SPResponseError
		Expect(err).To(BeAssignableToTypeOf(spErr))
		spErr = err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusNotFound))
	})

	It("maps ConflictError to 409", func() {
		repo.createErr = &store.ConflictError{Resource: "network", Field: "metadata.name", Value: "duplicate"}
		spec, _ := json.Marshal(validNetworkSpec())
		req := routing.CreateResourceRequest{
			ResourceID: "duplicate",
			Spec:       json.RawMessage(spec),
		}
		err := handler.CreateResource(context.Background(), req)
		var spErr *routing.SPResponseError
		Expect(err).To(BeAssignableToTypeOf(spErr))
		spErr = err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusConflict))
	})

	It("maps InvalidArgumentError to 400", func() {
		repo.createErr = &store.InvalidArgumentError{Field: "ports", Message: "invalid port"}
		spec, _ := json.Marshal(validNetworkSpec())
		req := routing.CreateResourceRequest{
			ResourceID: "test",
			Spec:       json.RawMessage(spec),
		}
		err := handler.CreateResource(context.Background(), req)
		var spErr *routing.SPResponseError
		Expect(err).To(BeAssignableToTypeOf(spErr))
		spErr = err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusBadRequest))
	})
})
