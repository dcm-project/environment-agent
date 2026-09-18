package kubernetes_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"

	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	k8sstore "github.com/dcm-project/environment-agent/internal/openshift/network/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/network/store"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var _ = Describe("List", func() {
	var (
		client *fake.Clientset
		s      *k8sstore.K8sNetworkStore
		logger *slog.Logger
		ctx    context.Context
	)

	BeforeEach(func() {
		client = fake.NewClientset()
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
		s = k8sstore.NewK8sNetworkStore(client, k8sstore.K8sConfig{Namespace: "default"}, logger)
		ctx = context.Background()
	})

	It("TC-U020: lists all DCM-managed networks", func() {
		for i := 1; i <= 3; i++ {
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("svc-%d", i),
					Namespace: "default",
					Labels: map[string]string{
						dcm.LabelManagedBy:   dcm.ValueManagedByDCM,
						dcm.LabelInstanceID:  fmt.Sprintf("id-%d", i),
						dcm.LabelServiceType: dcm.ValueServiceType,
					},
				},
				Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 80}}},
			}
			_, err := client.CoreV1().Services("default").Create(ctx, svc, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
		}

		result, err := s.List(ctx, 0, "")

		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(BeNil())
		Expect(*result.Results).To(HaveLen(3))
	})

	It("TC-U021: returns empty list when no networks exist", func() {
		result, err := s.List(ctx, 0, "")

		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(BeNil())
		Expect(*result.Results).To(BeEmpty())
		Expect(result.NextPageToken).To(BeNil())
	})

	It("TC-U022: paginates results with page_token", func() {
		for i := 1; i <= 5; i++ {
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("svc-%d", i),
					Namespace: "default",
					Labels: map[string]string{
						dcm.LabelManagedBy:   dcm.ValueManagedByDCM,
						dcm.LabelInstanceID:  fmt.Sprintf("id-%d", i),
						dcm.LabelServiceType: dcm.ValueServiceType,
					},
				},
				Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 80}}},
			}
			_, err := client.CoreV1().Services("default").Create(ctx, svc, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
		}

		client.PrependReactor("list", "services", paginationReactor(5))

		page1, err := s.List(ctx, 2, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(*page1.Results).To(HaveLen(2))
		Expect(page1.NextPageToken).NotTo(BeNil())

		page2, err := s.List(ctx, 2, *page1.NextPageToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(*page2.Results).To(HaveLen(2))
	})

	It("TC-U023: returns InvalidArgumentError for invalid page_token", func() {
		client.PrependReactor("list", "services", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewBadRequest("invalid continue token")
		})

		result, err := s.List(ctx, 10, "invalid-token")

		Expect(result).To(BeNil())
		Expect(err).To(HaveOccurred())

		var invalid *store.InvalidArgumentError
		Expect(err).To(BeAssignableToTypeOf(invalid))
		invalid = err.(*store.InvalidArgumentError)
		Expect(invalid.Field).To(Equal("page_token"))
	})
})

func paginationReactor(totalItems int) k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
		listAction, ok := action.(k8stesting.ListActionImpl)
		if !ok {
			return false, nil, nil
		}

		continueToken := listAction.ListOptions.Continue
		limit := listAction.ListOptions.Limit

		start := 0
		if continueToken != "" {
			var err error
			start, err = strconv.Atoi(continueToken)
			if err != nil {
				return true, nil, apierrors.NewBadRequest("invalid continue token")
			}
		}

		allServices := make([]corev1.Service, 0, totalItems)
		for i := 1; i <= totalItems; i++ {
			allServices = append(allServices, corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("svc-%d", i),
					Namespace: "default",
					Labels: map[string]string{
						dcm.LabelManagedBy:   dcm.ValueManagedByDCM,
						dcm.LabelInstanceID:  fmt.Sprintf("id-%d", i),
						dcm.LabelServiceType: dcm.ValueServiceType,
					},
				},
				Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 80}}},
			})
		}

		if start >= len(allServices) {
			return true, &corev1.ServiceList{Items: []corev1.Service{}}, nil
		}

		end := start + int(limit)
		if limit == 0 || end > len(allServices) {
			end = len(allServices)
		}

		result := &corev1.ServiceList{
			Items: allServices[start:end],
		}

		if end < len(allServices) {
			result.Continue = strconv.Itoa(end)
		}

		return true, result, nil
	}
}
