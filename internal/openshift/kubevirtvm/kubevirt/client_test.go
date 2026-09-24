package kubevirt

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

func newTestClient(handler http.Handler) (*Client, *httptest.Server) {
	ts := httptest.NewServer(handler)

	config := &rest.Config{
		Host:    ts.URL,
		APIPath: "/apis",
		ContentConfig: rest.ContentConfig{
			GroupVersion:         &schema.GroupVersion{Group: "kubevirt.io", Version: "v1"},
			NegotiatedSerializer: serializer.WithoutConversionCodecFactory{CodecFactory: kubevirtCodecs},
		},
	}

	rc, err := rest.RESTClientFor(config)
	if err != nil {
		panic(err)
	}

	return &Client{
		restClient: rc,
		namespace:  "default",
		timeout:    5 * time.Second,
	}, ts
}

func writeJSON(w http.ResponseWriter, status int, obj interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(obj); err != nil {
		panic(err)
	}
}

func writeError(w http.ResponseWriter) {
	code := http.StatusInternalServerError
	writeJSON(w, code, &metav1.Status{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status:   metav1.StatusFailure,
		Message:  "internal error",
		Code:     int32(code),
	})
}

var _ = Describe("Client", func() {
	Describe("CreateVirtualMachine", func() {
		It("should return created VM on success", func() {
			responseVM := &kubevirtv1.VirtualMachine{
				TypeMeta:   metav1.TypeMeta{APIVersion: "kubevirt.io/v1", Kind: "VirtualMachine"},
				ObjectMeta: metav1.ObjectMeta{Name: "test-vm", Namespace: "default"},
			}

			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusCreated, responseVM)
			}))
			defer ts.Close()

			result, err := c.CreateVirtualMachine(context.Background(), &kubevirtv1.VirtualMachine{})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Name).To(Equal("test-vm"))
		})

		It("should return error on API failure", func() {
			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeError(w)
			}))
			defer ts.Close()

			_, err := c.CreateVirtualMachine(context.Background(), &kubevirtv1.VirtualMachine{})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("GetVirtualMachine", func() {
		It("should return VM from list on success", func() {
			responseList := &kubevirtv1.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{APIVersion: "kubevirt.io/v1", Kind: "VirtualMachineList"},
				Items: []kubevirtv1.VirtualMachine{
					{ObjectMeta: metav1.ObjectMeta{Name: "found-vm", Namespace: "default"}},
				},
			}

			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, responseList)
			}))
			defer ts.Close()

			result, err := c.GetVirtualMachine(context.Background(), "vm-123")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Name).To(Equal("found-vm"))
		})

		It("should return a typed not-found error for empty list", func() {
			responseList := &kubevirtv1.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{APIVersion: "kubevirt.io/v1", Kind: "VirtualMachineList"},
				Items:    []kubevirtv1.VirtualMachine{},
			}

			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, responseList)
			}))
			defer ts.Close()

			_, err := c.GetVirtualMachine(context.Background(), "vm-123")
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "empty label-selector list must be a 404, not a 500")

			var statusErr *apierrors.StatusError
			Expect(errors.As(err, &statusErr)).To(BeTrue())
			Expect(statusErr.ErrStatus.Code).To(BeNumerically("==", http.StatusNotFound))
			Expect(statusErr.ErrStatus.Details.Name).To(Equal("vm-123"))
		})

		It("should return error on API failure", func() {
			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeError(w)
			}))
			defer ts.Close()

			_, err := c.GetVirtualMachine(context.Background(), "vm-123")
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsNotFound(err)).To(BeFalse(), "an API failure must not be mistaken for an absent VM")
		})
	})

	Describe("ListVirtualMachines", func() {
		It("should return items on success", func() {
			responseList := &kubevirtv1.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{APIVersion: "kubevirt.io/v1", Kind: "VirtualMachineList"},
				Items: []kubevirtv1.VirtualMachine{
					{ObjectMeta: metav1.ObjectMeta{Name: "vm-1"}},
					{ObjectMeta: metav1.ObjectMeta{Name: "vm-2"}},
				},
			}

			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, responseList)
			}))
			defer ts.Close()

			items, err := c.ListVirtualMachines(context.Background(), metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(items).To(HaveLen(2))
			Expect(items[0].Name).To(Equal("vm-1"))
			Expect(items[1].Name).To(Equal("vm-2"))
		})

		It("should return empty list", func() {
			responseList := &kubevirtv1.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{APIVersion: "kubevirt.io/v1", Kind: "VirtualMachineList"},
				Items:    []kubevirtv1.VirtualMachine{},
			}

			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, responseList)
			}))
			defer ts.Close()

			items, err := c.ListVirtualMachines(context.Background(), metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(items).To(BeEmpty())
		})

		It("should return error on API failure", func() {
			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeError(w)
			}))
			defer ts.Close()

			_, err := c.ListVirtualMachines(context.Background(), metav1.ListOptions{})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("DeleteVirtualMachine", func() {
		It("should delete successfully", func() {
			vmList := &kubevirtv1.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{APIVersion: "kubevirt.io/v1", Kind: "VirtualMachineList"},
				Items: []kubevirtv1.VirtualMachine{
					{ObjectMeta: metav1.ObjectMeta{Name: "test-vm", Namespace: "default"}},
				},
			}

			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodGet:
					writeJSON(w, http.StatusOK, vmList)
				case http.MethodDelete:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			err := c.DeleteVirtualMachine(context.Background(), "vm-123")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return error when get-lookup fails", func() {
			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeError(w)
			}))
			defer ts.Close()

			err := c.DeleteVirtualMachine(context.Background(), "vm-123")
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsNotFound(err)).To(BeFalse())
		})

		It("should return a typed not-found error for an unknown instance ID", func() {
			emptyList := &kubevirtv1.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{APIVersion: "kubevirt.io/v1", Kind: "VirtualMachineList"},
				Items:    []kubevirtv1.VirtualMachine{},
			}

			deleted := false
			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodGet:
					writeJSON(w, http.StatusOK, emptyList)
				case http.MethodDelete:
					deleted = true
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			err := c.DeleteVirtualMachine(context.Background(), "never-created")
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
			Expect(deleted).To(BeFalse(), "no DELETE should be issued for a VM that does not exist")
		})
	})

	Describe("UpdateVirtualMachine", func() {
		It("should return updated VM on success", func() {
			responseVM := &kubevirtv1.VirtualMachine{
				TypeMeta:   metav1.TypeMeta{APIVersion: "kubevirt.io/v1", Kind: "VirtualMachine"},
				ObjectMeta: metav1.ObjectMeta{Name: "updated-vm", Namespace: "default"},
			}

			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, responseVM)
			}))
			defer ts.Close()

			result, err := c.UpdateVirtualMachine(context.Background(), &kubevirtv1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{Name: "updated-vm"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Name).To(Equal("updated-vm"))
		})

		It("should return error on API failure", func() {
			c, ts := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeError(w)
			}))
			defer ts.Close()

			_, err := c.UpdateVirtualMachine(context.Background(), &kubevirtv1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{Name: "test-vm"},
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("DynamicClient", func() {
		It("should return the dynamic client", func() {
			c := &Client{}
			Expect(c.DynamicClient()).To(BeNil())
		})
	})
})
