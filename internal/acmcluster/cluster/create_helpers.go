package cluster

import (
	"context"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/registration"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResolveBaseDomain returns the base domain from request hints or config fallback.
func ResolveBaseDomain(req v1alpha1.Cluster, configDefault string) string {
	if req.Spec.ProviderHints != nil && req.Spec.ProviderHints.Acm != nil && req.Spec.ProviderHints.Acm.BaseDomain != nil {
		return *req.Spec.ProviderHints.Acm.BaseDomain
	}
	return configDefault
}

// ResolveReleaseImage returns the release image from hints or resolves via ClusterImageSet.
func ResolveReleaseImage(ctx context.Context, c client.Client, cfg config.ClusterConfig, req v1alpha1.Cluster) (string, error) {
	if req.Spec.ProviderHints != nil && req.Spec.ProviderHints.Acm != nil && req.Spec.ProviderHints.Acm.ReleaseImage != nil {
		return *req.Spec.ProviderHints.Acm.ReleaseImage, nil
	}
	matrix := registration.CompatibilityMatrix(cfg.VersionMatrix)
	if len(matrix) == 0 {
		matrix = registration.DefaultCompatibilityMatrix
	}
	resolver := NewVersionResolver(c, matrix)
	return resolver.Resolve(ctx, req.Spec.Version)
}
