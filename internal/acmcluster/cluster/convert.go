package cluster

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"strings"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/registration"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service/status"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ParseDCMMemory converts a DCM-format memory/storage string (e.g. "16GB", "512MB", "2TB")
// to a K8s resource.Quantity. DCM uses decimal units; this strips the trailing "B"
// so K8s interprets "16GB" as "16G" (decimal gigabytes).
func ParseDCMMemory(s string) (resource.Quantity, error) {
	if s == "" {
		return resource.Quantity{}, fmt.Errorf("empty memory/storage value")
	}
	k8sValue := strings.TrimSuffix(s, "B")
	if k8sValue == s {
		return resource.Quantity{}, fmt.Errorf("unsupported memory/storage format: %s", s)
	}
	q, err := resource.ParseQuantity(k8sValue)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("invalid memory/storage value %q: %w", s, err)
	}
	return q, nil
}

// HostedClusterToCluster converts a HostedCluster to a v1alpha1.Cluster.
func HostedClusterToCluster(ctx context.Context, c client.Client, cfg config.ClusterConfig, hc *hyperv1.HostedCluster) v1alpha1.Cluster {
	instanceID := hc.Labels[LabelInstanceID]
	clusterStatus := status.MapConditionsToStatus(hc.Status.Conditions, hc.DeletionTimestamp)

	var statusMessage *string
	switch clusterStatus {
	case v1alpha1.ClusterStatusFAILED:
		statusMessage = conditionMessage(hc.Status.Conditions, "Degraded")
	case v1alpha1.ClusterStatusUNAVAILABLE:
		statusMessage = conditionMessage(hc.Status.Conditions, "Available")
	}

	cl := v1alpha1.Cluster{
		Id:            util.Ptr(instanceID),
		Path:          util.Ptr("clusters/" + instanceID),
		Status:        &clusterStatus,
		StatusMessage: statusMessage,
		Spec: v1alpha1.ClusterSpec{
			Metadata: v1alpha1.ClusterMetadata{
				Name: hc.Name,
			},
			ServiceType: v1alpha1.ClusterSpecServiceTypeCluster,
			Version:     ReleaseImageToK8sVersion(hc.Spec.Release.Image, registration.CompatibilityMatrix(cfg.VersionMatrix)),
		},
	}

	if !hc.CreationTimestamp.IsZero() {
		t := hc.CreationTimestamp.Time
		cl.CreateTime = &t
	}

	if len(hc.Status.Conditions) > 0 {
		latest := hc.Status.Conditions[0].LastTransitionTime.Time
		for _, cond := range hc.Status.Conditions[1:] {
			if cond.LastTransitionTime.Time.After(latest) {
				latest = cond.LastTransitionTime.Time
			}
		}
		cl.UpdateTime = &latest
	} else if cl.CreateTime != nil {
		cl.UpdateTime = cl.CreateTime
	}

	if clusterStatus == v1alpha1.ClusterStatusREADY || clusterStatus == v1alpha1.ClusterStatusUNAVAILABLE {
		cl.ApiEndpoint = BuildAPIEndpoint(hc)
		cl.ConsoleUri = BuildConsoleURI(cfg.ConsoleURIPattern, hc)
		cl.Kubeconfig = ExtractKubeconfig(ctx, c, hc)
	}

	return cl
}

// ExtractKubeconfig reads the kubeconfig Secret referenced by the HostedCluster.
// Returns base64-encoded data or nil on graceful degradation.
func ExtractKubeconfig(ctx context.Context, c client.Client, hc *hyperv1.HostedCluster) *string {
	if hc.Status.KubeConfig == nil || hc.Status.KubeConfig.Name == "" {
		return nil
	}

	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{
		Name:      hc.Status.KubeConfig.Name,
		Namespace: hc.Namespace,
	}, secret); err != nil {
		return nil
	}

	data, ok := secret.Data["kubeconfig"]
	if !ok || len(data) == 0 {
		return nil
	}

	encoded := base64.StdEncoding.EncodeToString(data)
	return &encoded
}

// BuildConsoleURI constructs the console URI from the config pattern.
func BuildConsoleURI(pattern string, hc *hyperv1.HostedCluster) *string {
	if hc.Spec.DNS.BaseDomain == "" {
		return nil
	}
	uri := pattern
	uri = strings.ReplaceAll(uri, "{name}", hc.Name)
	uri = strings.ReplaceAll(uri, "{base_domain}", hc.Spec.DNS.BaseDomain)
	return &uri
}

// BuildAPIEndpoint constructs "https://{host}:{port}" from HC status.
func BuildAPIEndpoint(hc *hyperv1.HostedCluster) *string {
	if hc.Status.ControlPlaneEndpoint.Host == "" {
		return nil
	}
	endpoint := "https://" + net.JoinHostPort(
		hc.Status.ControlPlaneEndpoint.Host,
		strconv.FormatInt(int64(hc.Status.ControlPlaneEndpoint.Port), 10),
	)
	return &endpoint
}

// conditionMessage returns the Message from the named condition, or nil if absent/empty.
func conditionMessage(conditions []metav1.Condition, condType string) *string {
	for i := range conditions {
		if conditions[i].Type == condType && conditions[i].Message != "" {
			return &conditions[i].Message
		}
	}
	return nil
}
