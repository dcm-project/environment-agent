package baremetal

import (
	"context"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) Create(ctx context.Context, id string, req v1alpha1.Cluster) (*v1alpha1.Cluster, error) {
	infraEnv, err := s.resolveInfraEnv(req)
	if err != nil {
		return nil, err
	}

	b := &builder{config: s.config, infraEnv: infraEnv}
	return cluster.CreateCluster(ctx, s.client, s.config, id, req, b)
}

func (s *Service) resolveInfraEnv(req v1alpha1.Cluster) (string, error) {
	if req.Spec.ProviderHints != nil && req.Spec.ProviderHints.Acm != nil && req.Spec.ProviderHints.Acm.InfraEnv != nil {
		return *req.Spec.ProviderHints.Acm.InfraEnv, nil
	}
	if s.config.DefaultInfraEnv != "" {
		return s.config.DefaultInfraEnv, nil
	}
	return "", service.NewInvalidArgumentError("infra_env is required: set provider_hints.acm.infra_env or SP_DEFAULT_INFRA_ENV")
}

type builder struct {
	config   config.ClusterConfig
	infraEnv string
}

// BuildHostedCluster builds an Agent-platform HostedCluster for bare metal.
// control_plane.count and control_plane.storage are intentionally not mapped:
// HyperShift manages CP pod HA (ControllerAvailabilityPolicy) and etcd storage
// internally — these DCM fields describe node-level resources that don't exist
// in the hosted control plane model.
func (b *builder) BuildHostedCluster(req v1alpha1.Cluster, baseDomain, releaseImage string, labels map[string]string) *hyperv1.HostedCluster {
	return &hyperv1.HostedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Spec.Metadata.Name,
			Namespace: b.config.ClusterNamespace,
			Labels:    labels,
		},
		Spec: hyperv1.HostedClusterSpec{
			Platform: hyperv1.PlatformSpec{
				Type: hyperv1.AgentPlatform,
				Agent: &hyperv1.AgentPlatformSpec{
					AgentNamespace: b.config.AgentNamespace,
				},
			},
			Release: hyperv1.Release{
				Image: releaseImage,
			},
			DNS: hyperv1.DNSSpec{
				BaseDomain: baseDomain,
			},
			Services: cluster.DefaultServicePublishingStrategies, // REQ-ACM-180
			Etcd: hyperv1.EtcdSpec{ // REQ-ACM-210
				ManagementType: hyperv1.Managed,
				Managed: &hyperv1.ManagedEtcdSpec{
					Storage: hyperv1.ManagedEtcdStorageSpec{
						Type: hyperv1.PersistentVolumeEtcdStorage,
					},
				},
			},
		},
	}
}

func (b *builder) BuildNodePool(req v1alpha1.Cluster, releaseImage string, labels map[string]string) *hyperv1.NodePool {
	matchLabels := map[string]string{
		b.config.InfraEnvLabelKey: b.infraEnv,
	}

	if req.Spec.ProviderHints != nil && req.Spec.ProviderHints.Acm != nil && req.Spec.ProviderHints.Acm.AgentLabels != nil {
		for k, v := range *req.Spec.ProviderHints.Acm.AgentLabels {
			matchLabels[k] = v
		}
	}

	np := &hyperv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Spec.Metadata.Name,
			Namespace: b.config.ClusterNamespace,
			Labels:    labels,
		},
		Spec: hyperv1.NodePoolSpec{
			ClusterName: req.Spec.Metadata.Name,
			Release: hyperv1.Release{
				Image: releaseImage,
			},
			Management: hyperv1.NodePoolManagement{ // REQ-ACM-200
				UpgradeType: hyperv1.UpgradeTypeInPlace,
			},
			Platform: hyperv1.NodePoolPlatform{
				Type: hyperv1.AgentPlatform,
				Agent: &hyperv1.AgentNodePoolPlatform{
					AgentLabelSelector: &metav1.LabelSelector{
						MatchLabels: matchLabels,
					},
				},
			},
		},
	}

	if req.Spec.Nodes != nil && req.Spec.Nodes.Workers != nil && req.Spec.Nodes.Workers.Count != nil {
		replicas := int32(*req.Spec.Nodes.Workers.Count)
		np.Spec.Replicas = &replicas
	} else {
		np.Spec.Replicas = util.Ptr(int32(1))
	}

	return np
}
