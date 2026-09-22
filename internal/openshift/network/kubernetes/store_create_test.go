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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var _ = Describe("Create", func() {
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

	It("TC-U001: creates ClusterIP service when routing_level is nil and no node_ports", func() {
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata: v1alpha1.NetworkMetadata{
				Name: "test-clusterip",
			},
			Ports: []v1alpha1.PortSpec{
				{Port: 80, Protocol: ptr(v1alpha1.TCP)},
			},
		}
		instanceID := "test-id-001"

		network, err := s.Create(ctx, spec, instanceID)

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())
		Expect(network.Id).To(Equal(&instanceID))
		Expect(network.Spec.Metadata.Name).To(Equal("test-clusterip"))

		svc, err := client.CoreV1().Services("default").Get(ctx, "test-clusterip", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))

		Expect(svc.Labels[dcm.LabelManagedBy]).To(Equal(dcm.ValueManagedByDCM))
		Expect(svc.Labels[dcm.LabelInstanceID]).To(Equal(instanceID))
		Expect(svc.Labels[dcm.LabelServiceType]).To(Equal(dcm.ValueServiceType))

		Expect(svc.Spec.Ports).To(HaveLen(1))
		Expect(svc.Spec.Ports[0].Port).To(Equal(int32(80)))
		Expect(svc.Spec.Ports[0].Protocol).To(Equal(corev1.ProtocolTCP))
	})

	It("TC-U002: creates NodePort service when node_ports present", func() {
		portName := "http"
		nodePorts := map[string]int32{"http": 30080}
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata: v1alpha1.NetworkMetadata{
				Name: "test-nodeport",
			},
			Ports: []v1alpha1.PortSpec{
				{Port: 80, Name: &portName, Protocol: ptr(v1alpha1.TCP)},
			},
			ProviderHints: &v1alpha1.ProviderHints{
				Kubernetes: &v1alpha1.KubernetesProviderHints{
					NodePorts: &nodePorts,
				},
			},
		}
		instanceID := "test-id-002"

		network, err := s.Create(ctx, spec, instanceID)

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())
		Expect(network.Id).To(Equal(&instanceID))

		svc, err := client.CoreV1().Services("default").Get(ctx, "test-nodeport", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeNodePort))

		Expect(svc.Spec.Ports).To(HaveLen(1))
		Expect(svc.Spec.Ports[0].Port).To(Equal(int32(80)))
		Expect(svc.Spec.Ports[0].NodePort).To(Equal(int32(30080)))
		Expect(svc.Spec.Ports[0].Name).To(Equal("http"))
	})

	It("TC-U003: creates LoadBalancer service when routing_level is network", func() {
		routingLevel := v1alpha1.NetworkSpecRoutingLevelNetwork
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata: v1alpha1.NetworkMetadata{
				Name: "test-loadbalancer",
			},
			Ports: []v1alpha1.PortSpec{
				{Port: 443, Protocol: ptr(v1alpha1.TCP)},
			},
			RoutingLevel: &routingLevel,
		}
		instanceID := "test-id-003"

		network, err := s.Create(ctx, spec, instanceID)

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())
		Expect(network.Id).To(Equal(&instanceID))

		svc, err := client.CoreV1().Services("default").Get(ctx, "test-loadbalancer", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeLoadBalancer))

		Expect(network.Spec.RoutingLevel).NotTo(BeNil())
		Expect(*network.Spec.RoutingLevel).To(Equal(v1alpha1.NetworkSpecRoutingLevelNetwork))
	})

	It("TC-U004: rejects application routing_level with InvalidArgumentError", func() {
		routingLevel := v1alpha1.NetworkSpecRoutingLevelApplication
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata: v1alpha1.NetworkMetadata{
				Name: "test-invalid",
			},
			Ports: []v1alpha1.PortSpec{
				{Port: 80, Protocol: ptr(v1alpha1.TCP)},
			},
			RoutingLevel: &routingLevel,
		}
		instanceID := "test-id-004"

		network, err := s.Create(ctx, spec, instanceID)

		Expect(network).To(BeNil())
		Expect(err).To(HaveOccurred())

		var invalidArgErr *store.InvalidArgumentError
		Expect(err).To(BeAssignableToTypeOf(invalidArgErr))
		invalidArgErr = err.(*store.InvalidArgumentError)
		Expect(invalidArgErr.Field).To(Equal("routing_level"))
		Expect(invalidArgErr.Message).To(ContainSubstring("unknown routing_level"))
	})

	It("TC-U005: returns ConflictError when service name already exists", func() {
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata: v1alpha1.NetworkMetadata{
				Name: "existing-service",
			},
			Ports: []v1alpha1.PortSpec{
				{Port: 80, Protocol: ptr(v1alpha1.TCP)},
			},
		}
		instanceID := "test-id-005"

		// Simulate AlreadyExists error from Kubernetes
		client.PrependReactor("create", "services", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "services"}, "existing-service")
		})

		network, err := s.Create(ctx, spec, instanceID)

		Expect(network).To(BeNil())
		Expect(err).To(HaveOccurred())

		var conflictErr *store.ConflictError
		Expect(err).To(BeAssignableToTypeOf(conflictErr))
		conflictErr = err.(*store.ConflictError)
		Expect(conflictErr.Resource).To(Equal("network"))
		Expect(conflictErr.Field).To(Equal("metadata.name"))
		Expect(conflictErr.Value).To(Equal("existing-service"))
	})

	It("TC-U006: returns InvalidArgumentError when K8s rejects invalid spec", func() {
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata: v1alpha1.NetworkMetadata{
				Name: "invalid-spec",
			},
			Ports: []v1alpha1.PortSpec{
				{Port: 80, Protocol: ptr(v1alpha1.TCP)},
			},
		}
		instanceID := "test-id-006"

		client.PrependReactor("create", "services", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewInvalid(schema.GroupKind{Kind: "Service"}, "invalid-spec", nil)
		})

		network, err := s.Create(ctx, spec, instanceID)

		Expect(network).To(BeNil())
		Expect(err).To(HaveOccurred())

		Expect(err.Error()).To(ContainSubstring("invalid service spec"))
		var invalidErr *store.InvalidArgumentError
		Expect(errors.As(err, &invalidErr)).To(BeTrue())
	})

	It("returns ConflictError when K8s reports NodePort already allocated", func() {
		httpName := "http"
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata: v1alpha1.NetworkMetadata{
				Name: "nodeport-conflict",
			},
			Ports: []v1alpha1.PortSpec{
				{Port: 80, Protocol: ptr(v1alpha1.TCP), Name: &httpName},
			},
			ProviderHints: &v1alpha1.ProviderHints{
				Kubernetes: &v1alpha1.KubernetesProviderHints{
					NodePorts: &map[string]int32{"http": 30123},
				},
			},
		}
		instanceID := "nodeport-conflict-id"

		client.PrependReactor("create", "services", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, &apierrors.StatusError{
				ErrStatus: metav1.Status{
					Status:  "Failure",
					Message: `Service "nodeport-conflict" is invalid: spec.ports[0].nodePort: Invalid value: 30123: provided port is already allocated`,
					Reason:  metav1.StatusReasonInvalid,
					Code:    422,
					Details: &metav1.StatusDetails{
						Name: "nodeport-conflict",
						Kind: "Service",
						Causes: []metav1.StatusCause{
							{
								Type:    metav1.CauseTypeFieldValueInvalid,
								Message: "Invalid value: 30123: provided port is already allocated",
								Field:   "spec.ports[0].nodePort",
							},
						},
					},
				},
			}
		})

		network, err := s.Create(ctx, spec, instanceID)

		Expect(network).To(BeNil())
		Expect(err).To(HaveOccurred())

		var conflictErr *store.ConflictError
		Expect(errors.As(err, &conflictErr)).To(BeTrue())
	})

	It("TC-U007: applies DCM labels with instance ID", func() {
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata: v1alpha1.NetworkMetadata{
				Name: "test-dcm-labels",
			},
			Ports: []v1alpha1.PortSpec{
				{Port: 80, Protocol: ptr(v1alpha1.TCP)},
			},
		}
		instanceID := "custom-instance-id-123"

		network, err := s.Create(ctx, spec, instanceID)

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())

		svc, err := client.CoreV1().Services("default").Get(ctx, "test-dcm-labels", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		Expect(svc.Labels[dcm.LabelManagedBy]).To(Equal(dcm.ValueManagedByDCM))
		Expect(svc.Labels[dcm.LabelInstanceID]).To(Equal("custom-instance-id-123"))
		Expect(svc.Labels[dcm.LabelServiceType]).To(Equal(dcm.ValueServiceType))
	})

	It("TC-U008: merges user labels with DCM labels winning on collision", func() {
		userLabels := map[string]string{
			"user-key":           "user-value",
			"app":                "myapp",
			dcm.LabelManagedBy:   "evil-override",
			dcm.LabelInstanceID:  "wrong-id",
			dcm.LabelServiceType: "wrong-type",
		}
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata: v1alpha1.NetworkMetadata{
				Name:   "test-label-merge",
				Labels: &userLabels,
			},
			Ports: []v1alpha1.PortSpec{
				{Port: 80, Protocol: ptr(v1alpha1.TCP)},
			},
		}
		instanceID := "correct-instance-id"

		network, err := s.Create(ctx, spec, instanceID)

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())

		svc, err := client.CoreV1().Services("default").Get(ctx, "test-label-merge", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		Expect(svc.Labels[dcm.LabelManagedBy]).To(Equal(dcm.ValueManagedByDCM))
		Expect(svc.Labels[dcm.LabelInstanceID]).To(Equal("correct-instance-id"))
		Expect(svc.Labels[dcm.LabelServiceType]).To(Equal(dcm.ValueServiceType))
		Expect(svc.Labels["user-key"]).To(Equal("user-value"))
		Expect(svc.Labels["app"]).To(Equal("myapp"))
	})

	It("TC-U009: maps port name, protocol, port, and target_port correctly", func() {
		portName := "http"
		targetPort := int32(8080)
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata:    v1alpha1.NetworkMetadata{Name: "test-port-details"},
			Ports: []v1alpha1.PortSpec{
				{
					Name:       &portName,
					Protocol:   ptr(v1alpha1.TCP),
					Port:       80,
					TargetPort: &targetPort,
				},
			},
		}
		instanceID := "test-id-009"

		network, err := s.Create(ctx, spec, instanceID)

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())

		svc, err := client.CoreV1().Services("default").Get(ctx, "test-port-details", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(svc.Spec.Ports[0].Name).To(Equal("http"))
		Expect(svc.Spec.Ports[0].Protocol).To(Equal(corev1.ProtocolTCP))
		Expect(svc.Spec.Ports[0].Port).To(Equal(int32(80)))
		Expect(svc.Spec.Ports[0].TargetPort.IntVal).To(Equal(int32(8080)))
	})

	It("TC-U010: applies selector from provider_hints.kubernetes", func() {
		selector := map[string]string{"app": "web", "tier": "frontend"}
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata:    v1alpha1.NetworkMetadata{Name: "test-selector"},
			Ports:       []v1alpha1.PortSpec{{Port: 80, Protocol: ptr(v1alpha1.TCP)}},
			ProviderHints: &v1alpha1.ProviderHints{
				Kubernetes: &v1alpha1.KubernetesProviderHints{
					Selector: &selector,
				},
			},
		}
		instanceID := "test-id-010"

		network, err := s.Create(ctx, spec, instanceID)

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())

		svc, err := client.CoreV1().Services("default").Get(ctx, "test-selector", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(svc.Spec.Selector).To(Equal(selector))
	})

	It("TC-U011: applies cluster_ip from provider_hints for headless service", func() {
		clusterIP := "None"
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata:    v1alpha1.NetworkMetadata{Name: "test-headless"},
			Ports:       []v1alpha1.PortSpec{{Port: 80, Protocol: ptr(v1alpha1.TCP)}},
			ProviderHints: &v1alpha1.ProviderHints{
				Kubernetes: &v1alpha1.KubernetesProviderHints{
					ClusterIp: &clusterIP,
				},
			},
		}
		instanceID := "test-id-011"

		network, err := s.Create(ctx, spec, instanceID)

		Expect(err).NotTo(HaveOccurred())
		Expect(network).NotTo(BeNil())

		svc, err := client.CoreV1().Services("default").Get(ctx, "test-headless", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(svc.Spec.ClusterIP).To(Equal("None"))
	})

	It("TC-U124: rejects node_ports when ports lack names", func() {
		nodePorts := map[string]int32{"http": 30080}
		spec := v1alpha1.NetworkSpec{
			ServiceType: v1alpha1.NetworkSpecServiceTypeNetwork,
			Metadata:    v1alpha1.NetworkMetadata{Name: "test-unnamed-port"},
			Ports: []v1alpha1.PortSpec{
				{Port: 80, Protocol: ptr(v1alpha1.TCP)}, // No name!
			},
			ProviderHints: &v1alpha1.ProviderHints{
				Kubernetes: &v1alpha1.KubernetesProviderHints{
					NodePorts: &nodePorts,
				},
			},
		}
		instanceID := "test-id-012"

		network, err := s.Create(ctx, spec, instanceID)

		Expect(network).To(BeNil())
		Expect(err).To(HaveOccurred())

		var invalidArgErr *store.InvalidArgumentError
		Expect(err).To(BeAssignableToTypeOf(invalidArgErr))
		invalidArgErr = err.(*store.InvalidArgumentError)
		Expect(invalidArgErr.Field).To(Equal("ports"))
		Expect(invalidArgErr.Message).To(ContainSubstring("must have names when node_ports are specified"))
	})
})

func ptr[T any](v T) *T {
	return &v
}
