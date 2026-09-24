package kubernetes_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/container/dcm"
	k8sstore "github.com/dcm-project/environment-agent/internal/openshift/container/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/container/store"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// Compile-time assertion: K8sContainerStore implements ContainerRepository (TC-U024, TC-U089).
var _ store.ContainerRepository = (*k8sstore.K8sContainerStore)(nil)

// newTestStore creates a K8sContainerStore backed by a fake clientset.
func newTestStore(cfg k8sstore.K8sConfig) (*k8sstore.K8sContainerStore, *fake.Clientset) {
	client := fake.NewClientset()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	s := k8sstore.NewK8sContainerStore(client, cfg, logger)
	return s, client
}

// defaultConfig returns a K8sConfig with reasonable defaults for testing.
func defaultConfig() k8sstore.K8sConfig {
	return k8sstore.K8sConfig{
		Namespace:           "default",
		ExternalServiceType: "LoadBalancer",
	}
}

// minimalContainer creates a container with only the required fields set.
func minimalContainer(name string) v1alpha1.ContainerSpec {
	return v1alpha1.ContainerSpec{
		ServiceType: v1alpha1.ContainerSpecServiceTypeContainer,
		Metadata: v1alpha1.ContainerMetadata{
			Name: name,
		},
		Image: v1alpha1.ContainerImage{
			Reference: "nginx:latest",
		},
		Resources: v1alpha1.ContainerResources{
			Cpu: v1alpha1.ContainerCpu{
				Min: "1000m",
				Max: "2000m",
			},
			Memory: v1alpha1.ContainerMemory{
				Min: "1GB",
				Max: "2GB",
			},
		},
	}
}

// containerWithPorts creates a container with the specified network ports (visibility=none).
func containerWithPorts(name string, ports ...int) v1alpha1.ContainerSpec {
	c := minimalContainer(name)
	containerPorts := make([]v1alpha1.ContainerPort, len(ports))
	for i, p := range ports {
		containerPorts[i] = v1alpha1.ContainerPort{
			ContainerPort: p,
			Visibility:    v1alpha1.None,
		}
	}
	c.Network = &v1alpha1.ContainerNetwork{
		Ports: &containerPorts,
	}
	return c
}

// containerWithVisiblePorts creates a container where all ports share the given visibility.
func containerWithVisiblePorts(visibility v1alpha1.ContainerPortVisibility, ports ...int) v1alpha1.ContainerSpec {
	c := minimalContainer("my-app")
	containerPorts := make([]v1alpha1.ContainerPort, len(ports))
	for i, p := range ports {
		containerPorts[i] = v1alpha1.ContainerPort{
			ContainerPort: p,
			Visibility:    visibility,
		}
	}
	c.Network = &v1alpha1.ContainerNetwork{
		Ports: &containerPorts,
	}
	return c
}

// --- Deployment helpers with functional options ---

type fakeDeployOption func(*appsv1.Deployment)

func withDeploymentConditions(conditions []appsv1.DeploymentCondition) fakeDeployOption {
	return func(d *appsv1.Deployment) { d.Status.Conditions = conditions }
}

func withDeploymentStatus(status appsv1.DeploymentStatus) fakeDeployOption {
	return func(d *appsv1.Deployment) { d.Status = status }
}

func createFakeDeployment(client kubernetes.Interface, name, instanceID string, opts ...fakeDeployOption) error {
	labels := dcm.Labels(instanceID)
	replicas := int32(1)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  name,
							Image: "nginx:latest",
						},
					},
				},
			},
		},
	}
	for _, opt := range opts {
		opt(deploy)
	}
	_, err := client.AppsV1().Deployments("default").Create(context.Background(), deploy, metav1.CreateOptions{})
	return err
}

// --- Pod helpers with functional options ---

type fakePodOption func(*corev1.Pod)

func withPodConditions(conditions []corev1.PodCondition) fakePodOption {
	return func(p *corev1.Pod) { p.Status.Conditions = conditions }
}

func withCreationTime(t time.Time) fakePodOption {
	return func(p *corev1.Pod) { p.CreationTimestamp = metav1.NewTime(t) }
}

func createFakePod(client kubernetes.Interface, name, instanceID string, phase corev1.PodPhase, podIP string, opts ...fakePodOption) error {
	labels := dcm.Labels(instanceID)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    labels,
		},
		Status: corev1.PodStatus{
			Phase: phase,
			PodIP: podIP,
		},
	}
	for _, opt := range opts {
		opt(pod)
	}
	_, err := client.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{})
	return err
}

// getCreatedDeployment lists Deployments in the given namespace and returns the first one.
// Use this after s.Create() because GenerateName means the fake client won't assign a fixed name.
func getCreatedDeployment(client kubernetes.Interface, namespace string) (*appsv1.Deployment, error) {
	list, err := client.AppsV1().Deployments(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("no deployments found in namespace %s", namespace)
	}
	return &list.Items[0], nil
}

// getCreatedService lists Services in the given namespace and returns the first one.
// Use this after s.Create() because GenerateName means the fake client won't assign a fixed name.
func getCreatedService(client kubernetes.Interface, namespace string) (*corev1.Service, error) {
	list, err := client.CoreV1().Services(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("no services found in namespace %s", namespace)
	}
	return &list.Items[0], nil
}

// --- Service helpers with functional options ---

type fakeServiceOption func(*corev1.Service)

func withClusterIP(ip string) fakeServiceOption {
	return func(svc *corev1.Service) { svc.Spec.ClusterIP = ip }
}

func withLoadBalancerIP(ip string) fakeServiceOption {
	return func(svc *corev1.Service) {
		svc.Spec.Type = corev1.ServiceTypeLoadBalancer
		svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: ip}}
	}
}

func createFakeService(client kubernetes.Interface, namespace, name, instanceID string, svcType corev1.ServiceType, ports []int32, opts ...fakeServiceOption) error {
	labels := dcm.Labels(instanceID)
	svcPorts := make([]corev1.ServicePort, len(ports))
	for i, p := range ports {
		svcPorts[i] = corev1.ServicePort{
			Port:       p,
			TargetPort: intstr.FromInt32(p),
			Protocol:   corev1.ProtocolTCP,
		}
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: labels,
			Ports:    svcPorts,
		},
	}
	for _, opt := range opts {
		opt(svc)
	}
	_, err := client.CoreV1().Services(namespace).Create(context.Background(), svc, metav1.CreateOptions{})
	return err
}
