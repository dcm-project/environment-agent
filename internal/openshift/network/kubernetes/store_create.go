package kubernetes

import (
	"context"
	"fmt"

	v1alpha1 "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/network/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Create creates a new network from the given specification.
func (s *K8sNetworkStore) Create(ctx context.Context, spec v1alpha1.NetworkSpec, id string) (*v1alpha1.Network, error) {
	if spec.RoutingLevel != nil && *spec.RoutingLevel == v1alpha1.NetworkSpecRoutingLevelApplication {
		return nil, &store.InvalidArgumentError{
			Field:   "routing_level",
			Message: "application routing not supported",
		}
	}

	if spec.ProviderHints != nil && spec.ProviderHints.Kubernetes != nil {
		nodePorts := spec.ProviderHints.Kubernetes.NodePorts
		if nodePorts != nil && len(*nodePorts) > 0 {
			// Build set of port names for bidirectional validation
			portNames := make(map[string]bool)
			for _, port := range spec.Ports {
				if port.Name == nil || *port.Name == "" {
					return nil, &store.InvalidArgumentError{
						Field:   "ports",
						Message: "all ports must have names when node_ports are specified",
					}
				}
				portNames[*port.Name] = true
				// Validate that the port name exists in the node_ports map
				if _, exists := (*nodePorts)[*port.Name]; !exists {
					return nil, &store.InvalidArgumentError{
						Field:   "ports",
						Message: fmt.Sprintf("port name %q not found in node_ports map", *port.Name),
					}
				}
			}

			// Validate bidirectional: all node_ports keys must reference actual ports
			for nodePortKey := range *nodePorts {
				if !portNames[nodePortKey] {
					return nil, &store.InvalidArgumentError{
						Field:   "provider_hints.kubernetes.node_ports",
						Message: fmt.Sprintf("node_ports key %q does not match any port name", nodePortKey),
					}
				}
			}
		}
	}

	labels := dcmLabels(id)
	if spec.Metadata.Labels != nil {
		labels = mergeLabels(labels, *spec.Metadata.Labels)
	}

	service := buildService(spec, s.cfg, labels)

	created, err := s.client.CoreV1().Services(s.cfg.Namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, &store.ConflictError{
				Resource: "network",
				Field:    "metadata.name",
				Value:    spec.Metadata.Name,
			}
		}
		if apierrors.IsInvalid(err) {
			return nil, fmt.Errorf("invalid service spec: %w", err)
		}
		return nil, fmt.Errorf("failed to create service: %w", err)
	}

	network := serviceToNetwork(created, id)
	return &network, nil
}
