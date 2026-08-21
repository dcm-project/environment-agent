package kubevirtprovider

import (
	"context"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) Create(ctx context.Context, id string, req v1alpha1.Cluster) (*v1alpha1.Cluster, error) {
	return cluster.CreateCluster(ctx, s.client, s.config, id, req, s)
}

// BuildHostedCluster builds a KubeVirt-platform HostedCluster.
// control_plane.count and control_plane.storage are intentionally not mapped:
// HyperShift manages CP pod HA (ControllerAvailabilityPolicy) and etcd storage
// internally — these DCM fields describe node-level resources that don't exist
// in the hosted control plane model.
func (s *Service) BuildHostedCluster(req v1alpha1.Cluster, baseDomain, releaseImage string, labels map[string]string) *hyperv1.HostedCluster {
	return &hyperv1.HostedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Spec.Metadata.Name,
			Namespace: s.config.ClusterNamespace,
			Labels:    labels,
		},
		Spec: hyperv1.HostedClusterSpec{
			Platform: hyperv1.PlatformSpec{
				Type: hyperv1.KubevirtPlatform,
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

func (s *Service) BuildNodePool(req v1alpha1.Cluster, releaseImage string, labels map[string]string) *hyperv1.NodePool {
	np := &hyperv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Spec.Metadata.Name,
			Namespace: s.config.ClusterNamespace,
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
				Type: hyperv1.KubevirtPlatform,
			},
		},
	}

	if req.Spec.Nodes != nil && req.Spec.Nodes.Workers != nil {
		w := req.Spec.Nodes.Workers
		if w.Count != nil {
			replicas := int32(*w.Count)
			np.Spec.Replicas = &replicas
		} else {
			np.Spec.Replicas = util.Ptr(int32(1))
		}

		kvPlatform := &hyperv1.KubevirtNodePoolPlatform{}
		hasPlatform := false
		if w.Memory != nil {
			memory, _ := cluster.ParseDCMMemory(*w.Memory)
			kvPlatform.Compute = &hyperv1.KubevirtCompute{Memory: &memory}
			hasPlatform = true
		}
		if w.Cpu != nil {
			if kvPlatform.Compute == nil {
				kvPlatform.Compute = &hyperv1.KubevirtCompute{}
			}
			kvPlatform.Compute.Cores = util.Ptr(uint32(*w.Cpu))
			hasPlatform = true
		}
		if w.Storage != nil {
			storage, _ := cluster.ParseDCMMemory(*w.Storage)
			kvPlatform.RootVolume = &hyperv1.KubevirtRootVolume{
				KubevirtVolume: hyperv1.KubevirtVolume{
					Type: hyperv1.KubevirtVolumeTypePersistent,
					Persistent: &hyperv1.KubevirtPersistentVolume{
						Size: &storage,
					},
				},
			}
			hasPlatform = true
		}
		if hasPlatform {
			if kvPlatform.RootVolume == nil {
				defaultSize := resource.MustParse("32Gi")
				kvPlatform.RootVolume = &hyperv1.KubevirtRootVolume{
					KubevirtVolume: hyperv1.KubevirtVolume{
						Type: hyperv1.KubevirtVolumeTypePersistent,
						Persistent: &hyperv1.KubevirtPersistentVolume{
							Size: &defaultSize,
						},
					},
				}
			}
			np.Spec.Platform.Kubevirt = kvPlatform
		}
	}

	return np
}
