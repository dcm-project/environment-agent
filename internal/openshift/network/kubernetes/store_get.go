package kubernetes

import (
	"context"
	"fmt"

	"github.com/dcm-project/environment-agent/api/network/v1alpha1"
)

// Get retrieves a network by its ID.
func (s *K8sNetworkStore) Get(ctx context.Context, networkID string) (*v1alpha1.Network, error) {
	service, err := s.findService(ctx, networkID)
	if err != nil {
		return nil, fmt.Errorf("finding service: %w", err)
	}

	network := serviceToNetwork(service, networkID)
	return &network, nil
}
