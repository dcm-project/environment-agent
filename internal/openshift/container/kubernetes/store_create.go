package kubernetes

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/container/store"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// portsWithVisibility returns the subset of ports whose visibility is not "none".
// Returns nil if no qualifying ports exist.
func portsWithVisibility(spec v1alpha1.ContainerSpec) []v1alpha1.ContainerPort {
	if spec.Network == nil || spec.Network.Ports == nil || len(*spec.Network.Ports) == 0 {
		return nil
	}
	var result []v1alpha1.ContainerPort
	for _, p := range *spec.Network.Ports {
		if p.Visibility != v1alpha1.None {
			result = append(result, p)
		}
	}
	return result
}

// hasExternalPort returns true if any port has visibility "external".
func hasExternalPort(ports []v1alpha1.ContainerPort) bool {
	for _, p := range ports {
		if p.Visibility == v1alpha1.External {
			return true
		}
	}
	return false
}

// resolveServiceType determines the Kubernetes Service type based on port visibility.
// If any port is external, use the configured ExternalServiceType; otherwise ClusterIP.
func resolveServiceType(cfg K8sConfig, ports []v1alpha1.ContainerPort) corev1.ServiceType {
	if hasExternalPort(ports) {
		return corev1.ServiceType(cfg.ExternalServiceType)
	}
	return corev1.ServiceTypeClusterIP
}

// Create creates a new container backed by a Kubernetes Deployment (and
// optionally a Service when ports have non-none visibility).
func (s *K8sContainerStore) Create(ctx context.Context, spec v1alpha1.ContainerSpec, id string) (*v1alpha1.Container, error) {
	labels := dcmLabels(id)
	if spec.Metadata.Labels != nil {
		labels = mergeLabels(labels, *spec.Metadata.Labels)
	}

	// Check for duplicate dcm-instance-id.
	existing, err := s.client.AppsV1().Deployments(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: instanceSelector(id),
	})
	if err != nil {
		return nil, err
	}
	if len(existing.Items) > 0 {
		return nil, &store.ConflictError{Message: fmt.Sprintf("container with instance ID %q already exists", id)}
	}

	// Determine which ports need a Service (visibility != none).
	servicePorts := portsWithVisibility(spec)

	// Create Deployment.
	deploy, err := buildDeployment(spec, id, s.cfg, labels)
	if err != nil {
		return nil, &store.InvalidArgumentError{Message: err.Error()}
	}
	deploy, err = s.client.AppsV1().Deployments(s.cfg.Namespace).Create(ctx, deploy, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, &store.ConflictError{Message: fmt.Sprintf("deployment %q already exists", spec.Metadata.Name)}
		}
		return nil, err
	}

	// Create Service if any ports have non-none visibility.
	if len(servicePorts) > 0 {
		svcType := resolveServiceType(s.cfg, servicePorts)
		svc := buildService(spec, id, s.cfg, labels, svcType, servicePorts)
		_, err = s.client.CoreV1().Services(s.cfg.Namespace).Create(ctx, svc, metav1.CreateOptions{})
		if err != nil {
			// Rollback: delete the just-created Deployment.
			propagation := metav1.DeletePropagationBackground
			rollbackCtx, rollbackCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer rollbackCancel()
			if delErr := s.client.AppsV1().Deployments(s.cfg.Namespace).Delete(rollbackCtx, deploy.Name, metav1.DeleteOptions{
				PropagationPolicy: &propagation,
			}); delErr != nil {
				s.logger.Error("failed to rollback Deployment after Service creation failure",
					"deployment", deploy.Name,
					"namespace", s.cfg.Namespace,
					"rollbackError", delErr,
					"originalError", err,
				)
			}
			return nil, err
		}
	}

	return newContainerResult(spec, id, s.cfg.Namespace), nil
}

// newContainerResult stamps server-assigned fields onto a user-provided spec.
func newContainerResult(spec v1alpha1.ContainerSpec, id, namespace string) *v1alpha1.Container {
	now := time.Now()
	status := v1alpha1.PENDING
	path := fmt.Sprintf("containers/%s", id)

	spec.Metadata.Namespace = &namespace

	return &v1alpha1.Container{
		Id:         &id,
		Path:       &path,
		Status:     &status,
		CreateTime: &now,
		UpdateTime: &now,
		Spec:       spec,
	}
}
