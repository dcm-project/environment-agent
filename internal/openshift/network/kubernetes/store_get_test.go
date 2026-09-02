package kubernetes_test

import (
	"context"
	"errors"
	"io"
	"log/slog"

	v1alpha1 "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	k8sstore "github.com/dcm-project/environment-agent/internal/openshift/network/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/network/store"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var _ = Describe("Get", func() {
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

	It("TC-U012: returns network for existing service", func() {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-service",
				Namespace: "default",
				Labels: map[string]string{
					dcm.LabelManagedBy:   dcm.ValueManagedByDCM,
					dcm.LabelInstanceID:  "network-123",
					dcm.LabelServiceType: dcm.ValueServiceType,
				},
			},
			Spec: corev1.ServiceSpec{
				Type:  corev1.ServiceTypeClusterIP,
				Ports: []corev1.ServicePort{{Port: 80}},
			},
		}
		_, err := client.CoreV1().Services("default").Create(ctx, svc, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		network, err := s.Get(ctx, "network-123")

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())
		Expect(*network.Id).To(Equal("network-123"))
		Expect(network.Spec.Metadata.Name).To(Equal("test-service"))
	})

	It("TC-U013: returns NotFoundError for non-existent network", func() {
		network, err := s.Get(ctx, "non-existent-id")

		Expect(network).To(BeNil())
		Expect(err).To(HaveOccurred())

		var notFound *store.NotFoundError
		Expect(errors.As(err, &notFound)).To(BeTrue())
		Expect(notFound.Resource).To(Equal("network"))
		Expect(notFound.ID).To(Equal("non-existent-id"))
	})

	It("TC-U014: returns ConflictError for duplicate instance IDs", func() {
		labels := map[string]string{
			dcm.LabelManagedBy:   dcm.ValueManagedByDCM,
			dcm.LabelInstanceID:  "duplicate-id",
			dcm.LabelServiceType: dcm.ValueServiceType,
		}

		svc1 := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "service-1", Namespace: "default", Labels: labels},
			Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 80}}},
		}
		svc2 := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "service-2", Namespace: "default", Labels: labels},
			Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 80}}},
		}

		_, err := client.CoreV1().Services("default").Create(ctx, svc1, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = client.CoreV1().Services("default").Create(ctx, svc2, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		network, err := s.Get(ctx, "duplicate-id")

		Expect(network).To(BeNil())
		Expect(err).To(HaveOccurred())

		var conflict *store.ConflictError
		Expect(errors.As(err, &conflict)).To(BeTrue())
		Expect(conflict.Message).To(ContainSubstring("multiple services"))
	})

	It("TC-U015: returns status READY for ClusterIP service", func() {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ready-service",
				Namespace: "default",
				Labels: map[string]string{
					dcm.LabelManagedBy:   dcm.ValueManagedByDCM,
					dcm.LabelInstanceID:  "ready-id",
					dcm.LabelServiceType: dcm.ValueServiceType,
				},
			},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 80}}},
		}
		_, err := client.CoreV1().Services("default").Create(ctx, svc, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		network, err := s.Get(ctx, "ready-id")

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())
		Expect(*network.Status).To(Equal(v1alpha1.READY))
	})

	It("TC-U016: returns status PENDING for LoadBalancer without external IP", func() {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pending-lb",
				Namespace: "default",
				Labels: map[string]string{
					dcm.LabelManagedBy:   dcm.ValueManagedByDCM,
					dcm.LabelInstanceID:  "pending-id",
					dcm.LabelServiceType: dcm.ValueServiceType,
				},
			},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Ports: []corev1.ServicePort{{Port: 80}}},
			Status: corev1.ServiceStatus{
				LoadBalancer: corev1.LoadBalancerStatus{Ingress: []corev1.LoadBalancerIngress{}},
			},
		}
		_, err := client.CoreV1().Services("default").Create(ctx, svc, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		network, err := s.Get(ctx, "pending-id")

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())
		Expect(*network.Status).To(Equal(v1alpha1.PENDING))
	})
})
