package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DefaultServicePublishingStrategies defines the standard service publishing
// strategies for HostedCluster control plane services (REQ-ACM-180).
var DefaultServicePublishingStrategies = []hyperv1.ServicePublishingStrategyMapping{
	{Service: hyperv1.OAuthServer, ServicePublishingStrategy: hyperv1.ServicePublishingStrategy{Type: hyperv1.Route}},
	{Service: hyperv1.Konnectivity, ServicePublishingStrategy: hyperv1.ServicePublishingStrategy{Type: hyperv1.Route}},
	{Service: hyperv1.Ignition, ServicePublishingStrategy: hyperv1.ServicePublishingStrategy{Type: hyperv1.Route}},
	{Service: hyperv1.APIServer, ServicePublishingStrategy: hyperv1.ServicePublishingStrategy{Type: hyperv1.LoadBalancer}},
}

// PlatformBuilder builds platform-specific HostedCluster and NodePool resources.
type PlatformBuilder interface {
	BuildHostedCluster(req v1alpha1.Cluster, baseDomain, releaseImage string, labels map[string]string) *hyperv1.HostedCluster
	BuildNodePool(req v1alpha1.Cluster, releaseImage string, labels map[string]string) *hyperv1.NodePool
}

// CreateCluster orchestrates the shared cluster creation flow:
// resolve domain/image, check duplicates, create HC + NP via PlatformBuilder.
func CreateCluster(ctx context.Context, c client.Client, cfg config.ClusterConfig, id string, req v1alpha1.Cluster, pb PlatformBuilder) (*v1alpha1.Cluster, error) {
	baseDomain := ResolveBaseDomain(req, cfg.BaseDomain)
	if baseDomain == "" {
		return nil, service.NewInvalidArgumentError("base_domain is required")
	}

	releaseImage, err := ResolveReleaseImage(ctx, c, cfg, req)
	if err != nil {
		return nil, err
	}

	// Duplicate check: if findByInstanceID succeeds, cluster already exists.
	// NotFoundError is the expected case (no duplicate).
	if _, err := findByInstanceID(ctx, c, cfg.ClusterNamespace, id); err == nil {
		return nil, service.NewAlreadyExistsError("cluster with this ID already exists")
	} else {
		var domainErr *service.DomainError
		if errors.As(err, &domainErr) && domainErr.Type == v1alpha1.ErrorTypeNOTFOUND {
			// Expected: no duplicate found
		} else {
			return nil, err
		}
	}

	labels := DCMLabels(id)

	hc := pb.BuildHostedCluster(req, baseDomain, releaseImage, labels)
	applyControlPlaneResourceOverrides(hc, req)
	hc.Spec.PullSecret = corev1.LocalObjectReference{Name: cfg.PullSecretName} // REQ-ACM-191

	if err := c.Create(ctx, hc); err != nil {
		if k8serrors.IsAlreadyExists(err) {
			return nil, service.NewAlreadyExistsError(fmt.Sprintf("cluster with name %q already exists", req.Spec.Metadata.Name))
		}
		return nil, service.NewInternalError("failed to create cluster resources", err)
	}

	np := pb.BuildNodePool(req, releaseImage, labels)
	if err := c.Create(ctx, np); err != nil {
		if delErr := c.Delete(ctx, hc); delErr != nil {
			return nil, service.NewInternalError(
				"failed to create node pool and rollback of hosted cluster failed",
				fmt.Errorf("create: %w, rollback: %v", err, delErr),
			)
		}
		return nil, service.NewInternalError("failed to create cluster resources", err)
	}

	now := time.Now()
	result := &v1alpha1.Cluster{
		Id:         util.Ptr(id),
		Path:       util.Ptr("clusters/" + id),
		Status:     util.Ptr(v1alpha1.ClusterStatusPENDING),
		CreateTime: &now,
		UpdateTime: &now,
		Spec:       req.Spec,
	}

	return result, nil
}

// applyControlPlaneResourceOverrides sets HyperShift resource request override
// annotations for kube-apiserver and etcd (REQ-ACM-060, REQ-ACM-061).
func applyControlPlaneResourceOverrides(hc *hyperv1.HostedCluster, req v1alpha1.Cluster) {
	if req.Spec.Nodes == nil || req.Spec.Nodes.ControlPlane == nil {
		return
	}
	cp := req.Spec.Nodes.ControlPlane

	var parts []string
	if cp.Cpu != nil {
		parts = append(parts, fmt.Sprintf("cpu=%d", *cp.Cpu))
	}
	if cp.Memory != nil {
		parts = append(parts, fmt.Sprintf("memory=%s", strings.TrimSuffix(*cp.Memory, "B")))
	}
	if len(parts) == 0 {
		return
	}
	value := strings.Join(parts, ",")

	if hc.Annotations == nil {
		hc.Annotations = make(map[string]string)
	}
	hc.Annotations[hyperv1.ResourceRequestOverrideAnnotationPrefix+"/kube-apiserver.kube-apiserver"] = value
	hc.Annotations[hyperv1.ResourceRequestOverrideAnnotationPrefix+"/etcd.etcd"] = value
}
