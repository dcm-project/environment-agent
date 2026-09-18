// Package validate provides request validation for network operations.
package validate

import (
	"fmt"
	"regexp"

	v1alpha1 "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/network/store"
)

// reservedNetworkIDs cannot be used because they collide with fixed API paths
// under /api/v1alpha1/networks/.
var reservedNetworkIDs = map[string]bool{
	"health": true,
}

var aep122IDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

func validateNetworkID(id string) error {
	if reservedNetworkIDs[id] {
		return fmt.Errorf("network ID %q is reserved and cannot be used", id)
	}
	if !aep122IDPattern.MatchString(id) {
		return fmt.Errorf("network ID %q must match AEP-122 pattern", id)
	}
	return nil
}

func validateCreateSpec(spec v1alpha1.NetworkSpec) error {
	if spec.ServiceType != v1alpha1.NetworkSpecServiceTypeNetwork {
		return fmt.Errorf("service_type must be %q", v1alpha1.NetworkSpecServiceTypeNetwork)
	}
	if spec.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}
	if len(spec.Ports) == 0 {
		return fmt.Errorf("at least one port is required")
	}
	if spec.RoutingLevel != nil && !spec.RoutingLevel.Valid() {
		return fmt.Errorf("unknown routing_level %q", *spec.RoutingLevel)
	}
	if err := validatePorts(spec.Ports); err != nil {
		return err
	}
	if err := validateNodePorts(spec); err != nil {
		return err
	}
	return nil
}

func validatePorts(ports []v1alpha1.PortSpec) error {
	names := make(map[string]bool)
	for _, p := range ports {
		if p.Port < 1 || p.Port > 65535 {
			return fmt.Errorf("port %d out of range 1-65535", p.Port)
		}
		if p.TargetPort != nil && (*p.TargetPort < 1 || *p.TargetPort > 65535) {
			return fmt.Errorf("target_port %d out of range 1-65535", *p.TargetPort)
		}
		if p.Protocol != nil && !p.Protocol.Valid() {
			return fmt.Errorf("unknown protocol %q", *p.Protocol)
		}
		if p.Name != nil && *p.Name != "" {
			if names[*p.Name] {
				return fmt.Errorf("duplicate port name %q", *p.Name)
			}
			names[*p.Name] = true
		}
	}
	return nil
}

func validateNodePorts(spec v1alpha1.NetworkSpec) error {
	if spec.ProviderHints == nil || spec.ProviderHints.Kubernetes == nil {
		return nil
	}
	nodePorts := spec.ProviderHints.Kubernetes.NodePorts
	if nodePorts == nil || len(*nodePorts) == 0 {
		return nil
	}
	for name, np := range *nodePorts {
		if np < 30000 || np > 32767 {
			return fmt.Errorf("node_port %d for %q out of range 30000-32767", np, name)
		}
	}
	return nil
}

func validateUserLabels(labels *map[string]string) error {
	if labels == nil {
		return nil
	}
	for k := range *labels {
		if dcm.ReservedLabelKeys[k] {
			return fmt.Errorf("label %q is reserved by DCM and cannot be set by the user", k)
		}
	}
	return nil
}

// ValidateCreate checks network ID, spec fields, and reserved labels for embedded create.
func ValidateCreate(id string, spec v1alpha1.NetworkSpec) error {
	if err := validateNetworkID(id); err != nil {
		return &store.InvalidArgumentError{Message: err.Error()}
	}
	if err := validateCreateSpec(spec); err != nil {
		return &store.InvalidArgumentError{Message: err.Error()}
	}
	if err := validateUserLabels(spec.Metadata.Labels); err != nil {
		return &store.InvalidArgumentError{Message: err.Error()}
	}
	return nil
}
