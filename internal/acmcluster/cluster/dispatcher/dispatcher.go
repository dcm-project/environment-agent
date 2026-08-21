// Package dispatcher implements a composite ClusterService that dispatches
// operations to platform-specific sub-services.
package dispatcher

import (
	"context"
	"fmt"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster/baremetal"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster/kubevirtprovider"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ service.ClusterService = (*Service)(nil)

// Service dispatches cluster operations to the appropriate platform sub-service.
type Service struct {
	client   client.Client
	config   config.ClusterConfig
	services map[v1alpha1.ACMProviderHintsPlatform]platformCreator
}

// New creates a composite dispatcher service. Only platforms listed in
// enabledPlatforms are instantiated and available for dispatch.
func New(c client.Client, cfg config.ClusterConfig, enabledPlatforms []string) *Service {
	enabled := make(map[string]struct{}, len(enabledPlatforms))
	for _, p := range enabledPlatforms {
		enabled[p] = struct{}{}
	}

	services := make(map[v1alpha1.ACMProviderHintsPlatform]platformCreator)
	if _, ok := enabled["kubevirt"]; ok {
		services[v1alpha1.Kubevirt] = kubevirtprovider.New(c, cfg)
	}
	if _, ok := enabled["baremetal"]; ok {
		services[v1alpha1.Baremetal] = baremetal.New(c, cfg)
	}

	return &Service{
		client:   c,
		config:   cfg,
		services: services,
	}
}

func (s *Service) Create(ctx context.Context, id string, req v1alpha1.Cluster) (*v1alpha1.Cluster, error) {
	platform, svc := s.serviceForPlatform(req)
	if svc == nil {
		return nil, service.NewUnprocessableEntityError(fmt.Sprintf("unsupported platform %q", platform))
	}
	return svc.Create(ctx, id, req)
}

// Get retrieves a cluster by DCM ID (platform-agnostic).
func (s *Service) Get(ctx context.Context, id string) (*v1alpha1.Cluster, error) {
	return cluster.GetCluster(ctx, s.client, s.config, id)
}

func (s *Service) List(ctx context.Context, pageSize int, pageToken string) (*v1alpha1.ClusterList, error) {
	return cluster.ListClusters(ctx, s.client, s.config, pageSize, pageToken)
}

func (s *Service) Delete(ctx context.Context, id string) error {
	return cluster.DeleteCluster(ctx, s.client, s.config, id)
}

// platformCreator is the subset of ClusterService needed for platform-specific dispatching.
type platformCreator interface {
	Create(ctx context.Context, id string, cluster v1alpha1.Cluster) (*v1alpha1.Cluster, error)
}

func (s *Service) serviceForPlatform(req v1alpha1.Cluster) (v1alpha1.ACMProviderHintsPlatform, platformCreator) {
	platform := v1alpha1.Kubevirt // default per OpenAPI spec
	if req.Spec.ProviderHints != nil &&
		req.Spec.ProviderHints.Acm != nil &&
		req.Spec.ProviderHints.Acm.Platform != nil {
		platform = *req.Spec.ProviderHints.Acm.Platform
	}
	return platform, s.services[platform]
}
