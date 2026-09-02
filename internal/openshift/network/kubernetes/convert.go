package kubernetes

import (
	"fmt"

	"github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func buildService(spec v1alpha1.NetworkSpec, cfg K8sConfig, labels map[string]string) *corev1.Service {
	serviceT := inferServiceType(spec)
	ports := resolvePorts(spec.Ports)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Metadata.Name,
			Namespace: cfg.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:  serviceT,
			Ports: ports,
		},
	}

	if spec.ProviderHints != nil && spec.ProviderHints.Kubernetes != nil {
		clusterIP := spec.ProviderHints.Kubernetes.ClusterIp
		nodePorts := spec.ProviderHints.Kubernetes.NodePorts
		selector := spec.ProviderHints.Kubernetes.Selector
		if clusterIP != nil && *clusterIP != "" {
			service.Spec.ClusterIP = *clusterIP
		}
		// If node ports were provided, ports must have defined names
		if nodePorts != nil && len(*nodePorts) > 0 {
			for i, p := range service.Spec.Ports {
				if np, ok := (*nodePorts)[p.Name]; ok {
					service.Spec.Ports[i].NodePort = np
				}
			}
		}
		if selector != nil {
			service.Spec.Selector = *selector
		}
	}

	return service
}

func inferServiceType(spec v1alpha1.NetworkSpec) corev1.ServiceType {
	hasNodePorts := spec.ProviderHints != nil &&
		spec.ProviderHints.Kubernetes != nil &&
		spec.ProviderHints.Kubernetes.NodePorts != nil &&
		len(*spec.ProviderHints.Kubernetes.NodePorts) > 0

	if spec.RoutingLevel == nil {
		if hasNodePorts {
			return corev1.ServiceTypeNodePort
		}
		return corev1.ServiceTypeClusterIP
	}

	return corev1.ServiceTypeLoadBalancer
}

func resolvePorts(specPorts []v1alpha1.PortSpec) []corev1.ServicePort {
	ports := make([]corev1.ServicePort, len(specPorts))
	for i, p := range specPorts {
		port := corev1.ServicePort{
			Port:     p.Port,
			Protocol: corev1.ProtocolTCP,
		}

		if p.Protocol != nil {
			port.Protocol = (corev1.Protocol)(*p.Protocol)
		}

		if p.Name != nil && *p.Name != "" {
			port.Name = *p.Name
		}

		if targetPort := p.TargetPort; targetPort != nil && *targetPort > 0 {
			port.TargetPort = intstr.IntOrString{Type: intstr.Int, IntVal: *targetPort}
		} else {
			port.TargetPort = intstr.FromInt32(p.Port)
		}

		ports[i] = port
	}

	return ports
}

func serviceToNetwork(service *corev1.Service, instanceID string) v1alpha1.Network {
	path := fmt.Sprintf("networks/%s", instanceID)
	id := instanceID
	createTime := service.CreationTimestamp.Time
	updateTime := createTime
	serviceType := v1alpha1.NetworkSpecServiceTypeNetwork

	spec := v1alpha1.NetworkSpec{
		ServiceType: serviceType,
		Ports:       portsFromService(service),
		Metadata: v1alpha1.NetworkMetadata{
			Name:      service.Name,
			Namespace: &service.Namespace,
		},
	}

	// Infer routing level from the service type
	switch service.Spec.Type {
	case corev1.ServiceTypeLoadBalancer:
		networkLevel := v1alpha1.NetworkSpecRoutingLevelNetwork
		spec.RoutingLevel = &networkLevel
	case corev1.ServiceTypeClusterIP, corev1.ServiceTypeNodePort:
		spec.RoutingLevel = nil
	}

	// Reconstruct user labels by filtering out DCM reserved labels.
	if userLabels := userLabelsFromService(service); len(userLabels) > 0 {
		spec.Metadata.Labels = &userLabels
	}

	status := MapServiceStateToStatus(service)
	kubernetes := buildKubernetesState(service)

	return v1alpha1.Network{
		Id:         &id,
		Path:       &path,
		UpdateTime: &updateTime,
		CreateTime: &createTime,
		Spec:       spec,
		Status:     status,
		Kubernetes: kubernetes,
	}
}

func portsFromService(service *corev1.Service) []v1alpha1.PortSpec {
	ports := make([]v1alpha1.PortSpec, len(service.Spec.Ports))
	for i, p := range service.Spec.Ports {
		port := v1alpha1.PortSpec{
			Port:     p.Port,
			Protocol: (*v1alpha1.PortSpecProtocol)(&p.Protocol),
		}

		if p.Name != "" {
			port.Name = &p.Name
		}

		// NodePort - only set if > 0 (ClusterIP services have NodePort=0)
		if p.NodePort > 0 {
			port.NodePort = &p.NodePort
		}

		// TargetPort - handle IntOrString, only set if > 0
		// IntValue() returns int value, or 0 for named ports (strings)
		if targetPort := p.TargetPort.IntValue(); targetPort > 0 {
			tp := int32(targetPort)
			port.TargetPort = &tp
		}

		ports[i] = port
	}

	return ports
}

func userLabelsFromService(service *corev1.Service) map[string]string {
	labels := make(map[string]string)
	for k, v := range service.Labels {
		if !dcm.ReservedLabelKeys[k] {
			labels[k] = v
		}
	}

	return labels
}

// MapServiceToStatus returns the NetworkStatus and message for a Service.
// Reused by monitoring to publish status events.
func MapServiceToStatus(service *corev1.Service) (v1alpha1.NetworkStatus, string) {
	if service == nil {
		return v1alpha1.DELETED, "resource no longer exists"
	}

	if service.Spec.Type == corev1.ServiceTypeLoadBalancer {
		if len(service.Status.LoadBalancer.Ingress) == 0 {
			return v1alpha1.PENDING, "waiting for external IP assignment"
		}
	}

	// ClusterIP, NodePort or LoadBalancer with IP assigned means service is READY
	return v1alpha1.READY, "service is ready"
}

// MapServiceStateToStatus maps a Kubernetes Service to a NetworkStatus.
func MapServiceStateToStatus(service *corev1.Service) *v1alpha1.NetworkStatus {
	status, _ := MapServiceToStatus(service)
	return &status
}

func buildKubernetesState(service *corev1.Service) *v1alpha1.KubernetesState {
	state := &v1alpha1.KubernetesState{}

	svcType := convertServiceType(service.Spec.Type)
	state.Type = &svcType

	// ClusterIP (may be "None" for headless services)
	if service.Spec.ClusterIP != "" {
		state.ClusterIp = &service.Spec.ClusterIP
	}

	if len(service.Spec.Selector) > 0 {
		state.Selector = &service.Spec.Selector
	}

	if service.Spec.Type == corev1.ServiceTypeLoadBalancer {
		if externalIPs := extractExternalIPs(service.Status.LoadBalancer.Ingress); len(externalIPs) > 0 {
			state.ExternalIps = &externalIPs
		}
	}

	return state
}

func convertServiceType(k8sType corev1.ServiceType) v1alpha1.KubernetesStateType {
	switch k8sType {
	case corev1.ServiceTypeClusterIP:
		return v1alpha1.ClusterIP
	case corev1.ServiceTypeNodePort:
		return v1alpha1.NodePort
	case corev1.ServiceTypeLoadBalancer:
		return v1alpha1.LoadBalancer
	default:
		return v1alpha1.ClusterIP
	}
}

func extractExternalIPs(ingress []corev1.LoadBalancerIngress) []string {
	ips := make([]string, 0, len(ingress))
	for _, ing := range ingress {
		if ing.IP != "" {
			ips = append(ips, ing.IP)
		} else if ing.Hostname != "" {
			ips = append(ips, ing.Hostname)
		}
	}
	return ips
}
