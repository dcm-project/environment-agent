// Package kubevirtprovider implements the KubeVirt platform Create operation.
package kubevirtprovider

import (
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Service implements Create for the KubeVirt platform.
type Service struct {
	client client.Client
	config config.ClusterConfig
}

// New creates a new KubeVirt cluster service.
func New(c client.Client, cfg config.ClusterConfig) *Service {
	return &Service{client: c, config: cfg}
}
