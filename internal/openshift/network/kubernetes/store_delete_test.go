package kubernetes_test

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	k8sstore "github.com/dcm-project/environment-agent/internal/openshift/network/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/network/store"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var _ = Describe("Delete", func() {
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

	It("TC-U017: deletes the service successfully", func() {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "delete-me",
				Namespace: "default",
				Labels: map[string]string{
					dcm.LabelManagedBy:   dcm.ValueManagedByDCM,
					dcm.LabelInstanceID:  "delete-id",
					dcm.LabelServiceType: dcm.ValueServiceType,
				},
			},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 80}}},
		}
		_, err := client.CoreV1().Services("default").Create(ctx, svc, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		err = s.Delete(ctx, "delete-id")

		Expect(err).NotTo(HaveOccurred())

		_, err = client.CoreV1().Services("default").Get(ctx, "delete-me", metav1.GetOptions{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("TC-U018: returns NotFoundError for non-existent network", func() {
		err := s.Delete(ctx, "non-existent-id")

		Expect(err).To(HaveOccurred())

		var notFound *store.NotFoundError
		Expect(errors.As(err, &notFound)).To(BeTrue())
		Expect(notFound.ID).To(Equal("non-existent-id"))
	})

	It("TC-U019: returns ConflictError when multiple services have same instance ID", func() {
		labels := map[string]string{
			dcm.LabelManagedBy:   dcm.ValueManagedByDCM,
			dcm.LabelInstanceID:  "dup-delete-id",
			dcm.LabelServiceType: dcm.ValueServiceType,
		}

		svc1 := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "dup-1", Namespace: "default", Labels: labels},
			Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 80}}},
		}
		svc2 := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "dup-2", Namespace: "default", Labels: labels},
			Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 80}}},
		}

		_, err := client.CoreV1().Services("default").Create(ctx, svc1, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = client.CoreV1().Services("default").Create(ctx, svc2, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		err = s.Delete(ctx, "dup-delete-id")

		Expect(err).To(HaveOccurred())

		var conflict *store.ConflictError
		Expect(errors.As(err, &conflict)).To(BeTrue())
	})
})
