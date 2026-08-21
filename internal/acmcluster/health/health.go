// Package health implements dependency health checks for the service provider.
package health

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Compile-time interface check.
var _ service.HealthChecker = (*Checker)(nil)

// GVKs for dependency health checks.
var (
	hostedClusterListGVK = util.HostedClusterListGVK
	kubevirtVMIListGVK   = util.KubevirtVMIListGVK
	agentListGVK         = util.AgentListGVK
)

// Checker implements service.HealthChecker by probing K8s API, HyperShift CRDs,
// and platform-specific dependencies.
type Checker struct {
	k8sClient client.Client
	cfg       config.HealthConfig
	version   string
	startTime time.Time
}

// NewChecker creates a Checker with the given dependencies.
func NewChecker(k8sClient client.Client, cfg config.HealthConfig, version string, startTime time.Time) *Checker {
	return &Checker{
		k8sClient: k8sClient,
		cfg:       cfg,
		version:   version,
		startTime: startTime,
	}
}

// Check performs dependency health checks and returns the health status.
// All required fields (Type, Path, Version, Uptime) are always populated.
// Status is "healthy" when all checks pass, "unhealthy" otherwise.
func (c *Checker) Check(ctx context.Context) v1alpha1.Health {
	checkCtx, cancel := context.WithTimeout(ctx, c.cfg.CheckTimeout)
	defer cancel()

	uptime := max(0, int(time.Since(c.startTime).Seconds()))
	h := v1alpha1.Health{
		Type:    util.Ptr("acm-cluster-service-provider.dcm.io/health"),
		Path:    util.Ptr("health"),
		Version: util.Ptr(c.version),
		Uptime:  &uptime,
	}

	// Critical: K8s API connectivity + HyperShift CRD (REQ-HLT-070, REQ-HLT-080)
	if err := c.checkCRDAvailable(checkCtx, hostedClusterListGVK); err != nil {
		h.Status = util.Ptr("unhealthy")
		return h
	}

	// Platform: KubeVirt infrastructure (REQ-HLT-090)
	for _, platform := range c.cfg.EnabledPlatforms {
		var gvk schema.GroupVersionKind
		switch platform {
		case "kubevirt":
			gvk = kubevirtVMIListGVK
		case "baremetal":
			gvk = agentListGVK
		default:
			continue
		}
		if err := c.checkCRDAvailable(checkCtx, gvk); err != nil {
			h.Status = util.Ptr("unhealthy")
			return h
		}
	}

	h.Status = util.Ptr("healthy")
	return h
}

// checkCRDAvailable verifies that a CRD is available by first checking if the
// GVK is recognized in the client's scheme, then attempting to list resources.
//
// Two-phase design:
//   - Tests (fake client): Scheme().Recognizes() catches unregistered GVKs
//     (the fake client auto-registers unstructured GVKs during List, masking
//     missing CRDs).
//   - Production (real client): Recognizes() passes (types registered at
//     startup), List() catches missing CRDs via API server 404.
func (c *Checker) checkCRDAvailable(ctx context.Context, listGVK schema.GroupVersionKind) error {
	if !c.k8sClient.Scheme().Recognizes(listGVK) {
		return fmt.Errorf("GVK %s not recognized in scheme", listGVK)
	}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(listGVK)
	if err := c.k8sClient.List(ctx, list, client.Limit(1)); err != nil {
		return fmt.Errorf("listing %s: %w", listGVK.Kind, err)
	}
	return nil
}
