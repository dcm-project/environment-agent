package vm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	vmapi "github.com/dcm-project/environment-agent/api/vm/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubevirtv1 "kubevirt.io/api/core/v1"

	"github.com/dcm-project/environment-agent/internal/embedded/vm"
	"github.com/dcm-project/environment-agent/internal/routing"
)

func TestVM(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embedded VM Suite")
}

type fakeVMClient struct {
	createErr error
	deleteErr error
	getVM     *kubevirtv1.VirtualMachine
	getErr    error
	lastID    string
}

func (f *fakeVMClient) CheckHealth(context.Context) error {
	return nil
}

func (f *fakeVMClient) GetVirtualMachine(_ context.Context, vmID string) (*kubevirtv1.VirtualMachine, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.getVM != nil {
		return f.getVM, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Group: "kubevirt.io", Resource: "virtualmachines"}, vmID)
}

func (f *fakeVMClient) CreateVirtualMachine(_ context.Context, vm *kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	return vm, nil
}

func (f *fakeVMClient) DeleteVirtualMachine(_ context.Context, vmID string) error {
	f.lastID = vmID
	return f.deleteErr
}

type fakeVMMapper struct {
	mapErr error
	lastID string
}

func (f *fakeVMMapper) VMSpecToVirtualMachine(spec *vmapi.VMSpec, vmID string) (*kubevirtv1.VirtualMachine, error) {
	f.lastID = vmID
	if f.mapErr != nil {
		return nil, f.mapErr
	}
	return &kubevirtv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Metadata.Name,
			Namespace: "default",
		},
	}, nil
}

func validVMSpec() vmapi.VMSpec {
	return vmapi.VMSpec{
		ServiceType: vmapi.Vm,
		Metadata: vmapi.ServiceMetadata{
			Name: "test-vm",
		},
		GuestOs: vmapi.GuestOS{Type: "ubuntu"},
		Vcpu:    vmapi.Vcpu{Count: 2},
		Memory:  vmapi.Memory{Size: "2Gi"},
		Storage: vmapi.Storage{
			Disks: []vmapi.Disk{{Name: "boot", Capacity: "10Gi"}},
		},
	}
}

var _ = Describe("Handler", func() {
	var (
		client  *fakeVMClient
		mapper  *fakeVMMapper
		handler routing.EmbeddedHandler
	)

	BeforeEach(func() {
		client = &fakeVMClient{}
		mapper = &fakeVMMapper{}
		handler = vm.NewVMHandler(client, mapper)
	})

	It("creates a VM from a wrapped resource spec", func() {
		spec, err := json.Marshal(struct {
			Spec vmapi.VMSpec `json:"spec"`
		}{Spec: validVMSpec()})
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "vm-1",
			ServiceType: vm.ServiceType,
			Spec:        spec,
			EventID:     "ce-1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(mapper.lastID).To(Equal("vm-1"))
	})

	It("creates a VM from a bare VMSpec", func() {
		spec, err := json.Marshal(validVMSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "vm-2",
			ServiceType: vm.ServiceType,
			Spec:        spec,
			EventID:     "ce-2",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(mapper.lastID).To(Equal("vm-2"))
	})

	It("rejects an empty wrapped spec", func() {
		spec, err := json.Marshal(struct {
			Spec vmapi.VMSpec `json:"spec"`
		}{})
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "vm-3",
			ServiceType: vm.ServiceType,
			Spec:        spec,
			EventID:     "ce-3",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusBadRequest))
		Expect(mapper.lastID).To(BeEmpty())
	})

	It("returns conflict when the VM already exists", func() {
		client.getVM = &kubevirtv1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: "existing"},
		}
		spec, err := json.Marshal(validVMSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "vm-1",
			ServiceType: vm.ServiceType,
			Spec:        spec,
			EventID:     "ce-3",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusConflict))
	})

	It("maps kubernetes create errors", func() {
		client.createErr = apierrors.NewConflict(
			schema.GroupResource{Group: "kubevirt.io", Resource: "virtualmachines"},
			"test-vm",
			fmt.Errorf("already exists"),
		)
		spec, err := json.Marshal(validVMSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "vm-1",
			ServiceType: vm.ServiceType,
			Spec:        spec,
			EventID:     "ce-4",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusConflict))
	})

	It("keeps the original message for a non-status create error", func() {
		client.createErr = fmt.Errorf("connection refused")
		spec, err := json.Marshal(validVMSpec())
		Expect(err).NotTo(HaveOccurred())

		err = handler.CreateResource(context.Background(), routing.CreateResourceRequest{
			ResourceID:  "vm-1",
			ServiceType: vm.ServiceType,
			Spec:        spec,
			EventID:     "ce-7",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusInternalServerError))
		Expect(spErr.Message).To(ContainSubstring("connection refused"))
	})

	It("maps kubernetes delete errors", func() {
		client.deleteErr = apierrors.NewNotFound(
			schema.GroupResource{Group: "kubevirt.io", Resource: "virtualmachines"},
			"vm-1",
		)

		err := handler.DeleteResource(context.Background(), routing.DeleteResourceRequest{
			ResourceID:  "vm-1",
			ServiceType: vm.ServiceType,
			EventID:     "ce-5",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusNotFound))
		Expect(routing.IsRetryable(err)).To(BeFalse(), "deleting an absent VM is terminal, not worth retrying")
	})

	It("does not mistake a plain delete failure for an absent VM", func() {
		client.deleteErr = fmt.Errorf("etcdserver: request timed out")

		err := handler.DeleteResource(context.Background(), routing.DeleteResourceRequest{
			ResourceID:  "vm-1",
			ServiceType: vm.ServiceType,
			EventID:     "ce-5b",
		})
		Expect(err).To(BeAssignableToTypeOf(&routing.SPResponseError{}))
		spErr := err.(*routing.SPResponseError)
		Expect(spErr.StatusCode).To(Equal(http.StatusInternalServerError))
		Expect(routing.IsRetryable(err)).To(BeTrue())
	})

	It("deletes by resource ID", func() {
		err := handler.DeleteResource(context.Background(), routing.DeleteResourceRequest{
			ResourceID:  "vm-1",
			ServiceType: vm.ServiceType,
			EventID:     "ce-6",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(client.lastID).To(Equal("vm-1"))
	})

	It("detects vm in AGENT_EMBEDDED_SPS", func() {
		Expect(vm.Enabled([]string{"cluster", "vm"})).To(BeTrue())
		Expect(vm.Enabled([]string{"cluster"})).To(BeFalse())
		Expect(vm.Enabled(nil)).To(BeFalse())
	})
})
