package kubernetes

import (
	"fmt"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/container/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/container/units"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// MapPodPhaseToStatus maps a Kubernetes Pod phase to a container status.
// Returns the mapped status and true if mapping exists, or ("", false)
// for phases that should be ignored (e.g., Succeeded per DD-020).
func MapPodPhaseToStatus(phase corev1.PodPhase) (v1alpha1.ContainerStatus, bool) {
	switch phase {
	case corev1.PodPending:
		return v1alpha1.PENDING, true
	case corev1.PodRunning:
		return v1alpha1.RUNNING, true
	case corev1.PodFailed:
		return v1alpha1.FAILED, true
	case corev1.PodUnknown:
		return v1alpha1.UNKNOWN, true
	default:
		// Succeeded and any unknown phases are not mapped
		return "", false
	}
}

// containerFromDeployment reconstructs an API Container from a Kubernetes Deployment.
// It reverse-maps Deployment spec fields back to the API representation.
func containerFromDeployment(deploy *appsv1.Deployment, instanceID string) v1alpha1.Container {
	containers := deploy.Spec.Template.Spec.Containers
	id := instanceID
	path := fmt.Sprintf("containers/%s", instanceID)
	ns := deploy.Namespace
	createTime := deploy.CreationTimestamp.Time
	serviceType := v1alpha1.ContainerSpecServiceTypeContainer

	k8sC := containers[0]

	spec := v1alpha1.ContainerSpec{
		ServiceType: serviceType,
		Image:       v1alpha1.ContainerImage{Reference: k8sC.Image},
		Metadata: v1alpha1.ContainerMetadata{
			Name:      deploy.Name,
			Namespace: &ns,
		},
	}

	// Reconstruct resources from K8s resource requirements.
	spec.Resources = resourcesFromContainer(k8sC)

	// Reconstruct process (command/args/env) if present.
	if proc := processFromContainer(k8sC); proc != nil {
		spec.Process = proc
	}

	// Reconstruct network ports if present.
	// Visibility is set to "none" by default; enrichWithService will update it later.
	if len(k8sC.Ports) > 0 {
		ports := make([]v1alpha1.ContainerPort, len(k8sC.Ports))
		for i, p := range k8sC.Ports {
			ports[i] = v1alpha1.ContainerPort{
				ContainerPort: int(p.ContainerPort),
				Visibility:    v1alpha1.None,
			}
		}
		spec.Network = &v1alpha1.ContainerNetwork{Ports: &ports}
	}

	// Reconstruct user labels by filtering out DCM reserved labels.
	if userLabels := userLabelsFromDeployment(deploy); len(userLabels) > 0 {
		spec.Metadata.Labels = &userLabels
	}

	return v1alpha1.Container{
		Id:         &id,
		Path:       &path,
		CreateTime: &createTime,
		Spec:       spec,
	}
}

// resourcesFromContainer extracts CPU and memory resources from a K8s container spec.
func resourcesFromContainer(k8sC corev1.Container) v1alpha1.ContainerResources {
	res := v1alpha1.ContainerResources{}

	if req, ok := k8sC.Resources.Requests[corev1.ResourceCPU]; ok {
		res.Cpu.Min = units.CPUQuantityToAPI(req)
	}
	if lim, ok := k8sC.Resources.Limits[corev1.ResourceCPU]; ok {
		res.Cpu.Max = units.CPUQuantityToAPI(lim)
	}

	// Ensure ContainerCpu.Min and .Max are both non-empty when only one of
	// requests/limits is set, so API consumers always see a consistent
	// min/max pair.
	if res.Cpu.Min == "" && res.Cpu.Max != "" {
		res.Cpu.Min = res.Cpu.Max
	} else if res.Cpu.Max == "" && res.Cpu.Min != "" {
		res.Cpu.Max = res.Cpu.Min
	}

	if req, ok := k8sC.Resources.Requests[corev1.ResourceMemory]; ok {
		res.Memory.Min = units.MemoryQuantityToAPI(req)
	}
	if lim, ok := k8sC.Resources.Limits[corev1.ResourceMemory]; ok {
		res.Memory.Max = units.MemoryQuantityToAPI(lim)
	}

	return res
}

// processFromContainer extracts process configuration from a K8s container spec.
// Returns nil if no command, args, or env are set.
func processFromContainer(k8sC corev1.Container) *v1alpha1.ContainerProcess {
	if len(k8sC.Command) == 0 && len(k8sC.Args) == 0 && len(k8sC.Env) == 0 {
		return nil
	}

	proc := &v1alpha1.ContainerProcess{}
	if len(k8sC.Command) > 0 {
		cmd := make([]string, len(k8sC.Command))
		copy(cmd, k8sC.Command)
		proc.Command = &cmd
	}
	if len(k8sC.Args) > 0 {
		args := make([]string, len(k8sC.Args))
		copy(args, k8sC.Args)
		proc.Args = &args
	}
	if len(k8sC.Env) > 0 {
		envVars := make([]v1alpha1.ContainerEnvVar, len(k8sC.Env))
		for i, e := range k8sC.Env {
			envVars[i] = v1alpha1.ContainerEnvVar{Name: e.Name, Value: e.Value}
		}
		proc.Env = &envVars
	}
	return proc
}

// userLabelsFromDeployment extracts user-defined labels by filtering out DCM reserved labels.
func userLabelsFromDeployment(deploy *appsv1.Deployment) map[string]string {
	labels := make(map[string]string)
	for k, v := range deploy.Labels {
		if !dcm.ReservedLabelKeys[k] {
			labels[k] = v
		}
	}
	return labels
}

// enrichWithPod populates runtime data from a Pod into the Container.
func enrichWithPod(container *v1alpha1.Container, pod *corev1.Pod) {
	if status, ok := MapPodPhaseToStatus(pod.Status.Phase); ok {
		container.Status = &status
	}

	if pod.Status.PodIP != "" {
		if container.Spec.Network == nil {
			container.Spec.Network = &v1alpha1.ContainerNetwork{}
		}
		container.Spec.Network.Ip = &pod.Status.PodIP
	}

	if t := latestPodTransitionTime(pod); t != nil {
		container.UpdateTime = t
	}
}

// enrichWithService populates service info from a Kubernetes Service and
// infers port visibility from the Service state.
func enrichWithService(container *v1alpha1.Container, svc *corev1.Service) {
	info := &v1alpha1.ServiceInfo{
		Name: &svc.Name,
	}

	if svc.Spec.ClusterIP != "" {
		info.ClusterIp = &svc.Spec.ClusterIP
	}

	svcType := v1alpha1.ServiceInfoType(svc.Spec.Type)
	info.Type = &svcType

	// Build a set of target ports exposed by the Service.
	svcTargetPorts := make(map[int]bool)
	if len(svc.Spec.Ports) > 0 {
		ports := make([]v1alpha1.ServicePort, len(svc.Spec.Ports))
		for i, p := range svc.Spec.Ports {
			protocol := string(p.Protocol)
			ports[i] = v1alpha1.ServicePort{
				Port:       int(p.Port),
				TargetPort: p.TargetPort.IntValue(),
				Protocol:   &protocol,
			}
			svcTargetPorts[p.TargetPort.IntValue()] = true
		}
		info.Ports = &ports
	}

	if len(svc.Status.LoadBalancer.Ingress) > 0 && svc.Status.LoadBalancer.Ingress[0].IP != "" {
		info.ExternalIp = &svc.Status.LoadBalancer.Ingress[0].IP
	}

	container.Service = info

	// Infer port visibility from Service state.
	if container.Spec.Network != nil && container.Spec.Network.Ports != nil && len(*container.Spec.Network.Ports) > 0 {
		visibility := inferVisibility(svc.Spec.Type)
		for i := range *container.Spec.Network.Ports {
			if svcTargetPorts[(*container.Spec.Network.Ports)[i].ContainerPort] {
				(*container.Spec.Network.Ports)[i].Visibility = visibility
			}
			// Ports not in the Service keep their default (none).
		}
	}
}

// inferVisibility maps a K8s Service type to a port visibility value.
func inferVisibility(svcType corev1.ServiceType) v1alpha1.ContainerPortVisibility {
	switch svcType {
	case corev1.ServiceTypeLoadBalancer, corev1.ServiceTypeNodePort:
		return v1alpha1.External
	default:
		return v1alpha1.Internal
	}
}

// latestPodTransitionTime returns the most recent LastTransitionTime from Pod conditions.
func latestPodTransitionTime(pod *corev1.Pod) *time.Time {
	var latest *time.Time
	for i := range pod.Status.Conditions {
		t := pod.Status.Conditions[i].LastTransitionTime.Time
		if t.IsZero() {
			continue
		}
		if latest == nil || t.After(*latest) {
			latest = &t
		}
	}
	return latest
}

// latestDeploymentTransitionTime returns the most recent LastTransitionTime from Deployment conditions.
func latestDeploymentTransitionTime(deploy *appsv1.Deployment) *time.Time {
	var latest *time.Time
	for i := range deploy.Status.Conditions {
		t := deploy.Status.Conditions[i].LastTransitionTime.Time
		if t.IsZero() {
			continue
		}
		if latest == nil || t.After(*latest) {
			latest = &t
		}
	}
	return latest
}

// buildDeployment creates a Kubernetes Deployment from a Container spec.
func buildDeployment(spec v1alpha1.ContainerSpec, id string, cfg K8sConfig, labels map[string]string) (*appsv1.Deployment, error) {
	replicas := int32(1)

	// Selector uses only DCM labels (immutable after creation)
	selectorLabels := dcmLabels(id)

	// CPU resources
	cpuReq, cpuLim, err := units.ConvertCPU(spec.Resources.Cpu)
	if err != nil {
		return nil, fmt.Errorf("converting CPU resources: %w", err)
	}

	memReq, err := units.ConvertMemory(spec.Resources.Memory.Min)
	if err != nil {
		return nil, fmt.Errorf("converting memory.min: %w", err)
	}
	memLim, err := units.ConvertMemory(spec.Resources.Memory.Max)
	if err != nil {
		return nil, fmt.Errorf("converting memory.max: %w", err)
	}

	k8sContainer := corev1.Container{
		Name:  spec.Metadata.Name,
		Image: spec.Image.Reference,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    cpuReq,
				corev1.ResourceMemory: memReq,
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    cpuLim,
				corev1.ResourceMemory: memLim,
			},
		},
	}

	if spec.Process != nil {
		if spec.Process.Command != nil {
			k8sContainer.Command = *spec.Process.Command
		}
		if spec.Process.Args != nil {
			k8sContainer.Args = *spec.Process.Args
		}
		if spec.Process.Env != nil {
			envVars := make([]corev1.EnvVar, len(*spec.Process.Env))
			for i, e := range *spec.Process.Env {
				envVars[i] = corev1.EnvVar{Name: e.Name, Value: e.Value}
			}
			k8sContainer.Env = envVars
		}
	}

	if spec.Network != nil && spec.Network.Ports != nil && len(*spec.Network.Ports) > 0 {
		ports := make([]corev1.ContainerPort, len(*spec.Network.Ports))
		for i, p := range *spec.Network.Ports {
			ports[i] = corev1.ContainerPort{
				ContainerPort: int32(p.ContainerPort),
			}
		}
		k8sContainer.Ports = ports
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: spec.Metadata.Name + "-",
			Namespace:    cfg.Namespace,
			Labels:       labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{k8sContainer},
				},
			},
		},
	}, nil
}

// buildService creates a Kubernetes Service from a Container spec.
// servicePorts contains only the ports with non-none visibility.
func buildService(spec v1alpha1.ContainerSpec, id string, cfg K8sConfig, labels map[string]string, svcType corev1.ServiceType, servicePorts []v1alpha1.ContainerPort) *corev1.Service {
	// Selector uses only DCM labels
	selectorLabels := dcmLabels(id)

	svcPorts := make([]corev1.ServicePort, len(servicePorts))
	for i, p := range servicePorts {
		svcPorts[i] = corev1.ServicePort{
			Name:       fmt.Sprintf("port-%d", p.ContainerPort),
			Port:       int32(p.ContainerPort),
			TargetPort: intstr.FromInt32(int32(p.ContainerPort)),
			Protocol:   corev1.ProtocolTCP,
		}
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: spec.Metadata.Name + "-",
			Namespace:    cfg.Namespace,
			Labels:       labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: selectorLabels,
			Ports:    svcPorts,
		},
	}
}
