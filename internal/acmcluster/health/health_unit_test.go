package health_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/health"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// GVKs for resources used in health checks.
var (
	hostedClusterGVK     = util.HostedClusterGVK
	hostedClusterListGVK = util.HostedClusterListGVK
	kubevirtVMIGVK       = util.KubevirtVMIGVK
	kubevirtVMIListGVK   = util.KubevirtVMIListGVK
	agentGVK             = util.AgentGVK
	agentListGVK         = util.AgentListGVK
)

// newHealthyScheme creates a scheme with ALL GVKs needed for a fully healthy check
// (critical + all platform dependencies).
func newHealthyScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(hostedClusterGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(hostedClusterListGVK, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(kubevirtVMIGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(kubevirtVMIListGVK, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(agentGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(agentListGVK, &unstructured.UnstructuredList{})
	return s
}

// newHyperShiftOnlyScheme creates a scheme with only HyperShift GVKs (critical
// checks pass, but platform-specific GVKs are absent).
func newHyperShiftOnlyScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(hostedClusterGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(hostedClusterListGVK, &unstructured.UnstructuredList{})
	return s
}

var _ = Describe("HealthChecker", func() {
	// ---------------------------------------------------------------
	// TC-HLT-UT-001: Healthy response with all checks passing
	// REQ-HLT-010, REQ-HLT-020, REQ-HLT-030, REQ-HLT-040, REQ-HLT-050, REQ-HLT-120
	// ---------------------------------------------------------------
	It("returns healthy status with all required fields when all checks pass (TC-HLT-UT-001)", func() {
		k8sClient := fake.NewClientBuilder().WithScheme(newHealthyScheme()).Build()
		cfg := config.HealthConfig{
			CheckTimeout:     5 * time.Second,
			EnabledPlatforms: []string{"kubevirt", "baremetal"},
		}
		startTime := time.Now().Add(-10 * time.Second)
		checker := health.NewChecker(k8sClient, cfg, "v1.0.0", startTime)

		result := checker.Check(context.Background())

		// REQ-HLT-050: status="healthy" when all dependency checks pass
		Expect(result.Status).NotTo(BeNil())
		Expect(*result.Status).To(Equal("healthy"))

		// REQ-HLT-040: type
		Expect(result.Type).NotTo(BeNil())
		Expect(*result.Type).To(Equal("acm-cluster-service-provider.dcm.io/health"))

		// REQ-HLT-030: path
		Expect(result.Path).NotTo(BeNil())
		Expect(*result.Path).To(Equal("health"))

		// REQ-HLT-020: version present and non-empty
		Expect(result.Version).NotTo(BeNil())
		Expect(*result.Version).To(Equal("v1.0.0"))

		// REQ-HLT-020: uptime present and >= 0
		Expect(result.Uptime).NotTo(BeNil())
		Expect(*result.Uptime).To(BeNumerically(">=", 10))
	})

	// ---------------------------------------------------------------
	// TC-HLT-UT-002: Unhealthy when K8s API unreachable (critical dependency)
	// REQ-HLT-060, REQ-HLT-070
	// ---------------------------------------------------------------
	It("returns unhealthy when K8s API is unreachable (TC-HLT-UT-002)", func() {
		funcs := interceptor.Funcs{
			List: func(
				_ context.Context,
				_ client.WithWatch,
				_ client.ObjectList,
				_ ...client.ListOption,
			) error {
				return fmt.Errorf("connection refused")
			},
		}
		k8sClient := fake.NewClientBuilder().
			WithScheme(newHealthyScheme()).
			WithInterceptorFuncs(funcs).
			Build()
		cfg := config.HealthConfig{
			CheckTimeout:     5 * time.Second,
			EnabledPlatforms: []string{"kubevirt", "baremetal"},
		}
		checker := health.NewChecker(k8sClient, cfg, "v1.0.0", time.Now())

		result := checker.Check(context.Background())

		// REQ-HLT-060: status="unhealthy" when critical dependency fails
		Expect(result.Status).NotTo(BeNil())
		Expect(*result.Status).To(Equal("unhealthy"))

		// type and path are still correctly set
		Expect(result.Type).NotTo(BeNil())
		Expect(*result.Type).To(Equal("acm-cluster-service-provider.dcm.io/health"))
		Expect(result.Path).NotTo(BeNil())
		Expect(*result.Path).To(Equal("health"))
	})

	// ---------------------------------------------------------------
	// TC-HLT-UT-003: Unhealthy when HyperShift CRD missing (critical dependency)
	// REQ-HLT-060, REQ-HLT-080
	// ---------------------------------------------------------------
	It("returns unhealthy when HyperShift CRD is missing (TC-HLT-UT-003)", func() {
		// Scheme without HostedCluster GVK — simulates CRD not installed.
		emptyScheme := runtime.NewScheme()
		k8sClient := fake.NewClientBuilder().WithScheme(emptyScheme).Build()
		cfg := config.HealthConfig{
			CheckTimeout:     5 * time.Second,
			EnabledPlatforms: []string{"kubevirt", "baremetal"},
		}
		checker := health.NewChecker(k8sClient, cfg, "v1.0.0", time.Now())

		result := checker.Check(context.Background())

		// REQ-HLT-060, REQ-HLT-080: status="unhealthy"
		Expect(result.Status).NotTo(BeNil())
		Expect(*result.Status).To(Equal("unhealthy"))
	})

	// ---------------------------------------------------------------
	// TC-HLT-UT-004: Timeout produces unhealthy
	// REQ-HLT-110
	// ---------------------------------------------------------------
	It("returns unhealthy within deadline when dependency checks timeout (TC-HLT-UT-004)", func() {
		// Interceptor that blocks until context is cancelled — simulates slow checks.
		funcs := interceptor.Funcs{
			List: func(
				ctx context.Context,
				_ client.WithWatch,
				_ client.ObjectList,
				_ ...client.ListOption,
			) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
		k8sClient := fake.NewClientBuilder().
			WithScheme(newHealthyScheme()).
			WithInterceptorFuncs(funcs).
			Build()
		cfg := config.HealthConfig{
			CheckTimeout:     200 * time.Millisecond,
			EnabledPlatforms: []string{"kubevirt", "baremetal"},
		}
		checker := health.NewChecker(k8sClient, cfg, "v1.0.0", time.Now())

		timeoutCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		start := time.Now()
		result := checker.Check(timeoutCtx)
		elapsed := time.Since(start)

		// REQ-HLT-110: response within timeout
		Expect(elapsed).To(BeNumerically("<", 1*time.Second))

		// status="unhealthy" on timeout
		Expect(result.Status).NotTo(BeNil())
		Expect(*result.Status).To(Equal("unhealthy"))
	})

	// ---------------------------------------------------------------
	// TC-HLT-UT-005: KubeVirt infrastructure check
	// REQ-HLT-090
	// ---------------------------------------------------------------
	It("returns unhealthy when KubeVirt infrastructure is not accessible (TC-HLT-UT-005)", func() {
		// Scheme has HostedCluster (critical check passes) but no KubeVirt GVKs.
		k8sClient := fake.NewClientBuilder().WithScheme(newHyperShiftOnlyScheme()).Build()
		cfg := config.HealthConfig{
			CheckTimeout:     5 * time.Second,
			EnabledPlatforms: []string{"kubevirt"},
		}
		checker := health.NewChecker(k8sClient, cfg, "v1.0.0", time.Now())

		result := checker.Check(context.Background())

		// REQ-HLT-090: status="unhealthy" when KubeVirt infra unreachable
		Expect(result.Status).NotTo(BeNil())
		Expect(*result.Status).To(Equal("unhealthy"))
	})

	// ---------------------------------------------------------------
	// TC-HLT-UT-006: BareMetal infrastructure check
	// REQ-HLT-100
	// ---------------------------------------------------------------
	It("returns unhealthy when BareMetal Agent/CIM resources are unavailable (TC-HLT-UT-006)", func() {
		// Scheme has HostedCluster (critical check passes) but no Agent/CIM GVKs.
		k8sClient := fake.NewClientBuilder().WithScheme(newHyperShiftOnlyScheme()).Build()
		cfg := config.HealthConfig{
			CheckTimeout:     5 * time.Second,
			EnabledPlatforms: []string{"baremetal"},
		}
		checker := health.NewChecker(k8sClient, cfg, "v1.0.0", time.Now())

		result := checker.Check(context.Background())

		// REQ-HLT-100: status="unhealthy" when Agent/CIM unavailable
		Expect(result.Status).NotTo(BeNil())
		Expect(*result.Status).To(Equal("unhealthy"))
	})

	// ---------------------------------------------------------------
	// TC-HLT-UT-007: Uptime increases over time
	// REQ-HLT-020
	// ---------------------------------------------------------------
	It("reports increasing uptime over successive calls (TC-HLT-UT-007)", func() {
		k8sClient := fake.NewClientBuilder().WithScheme(newHealthyScheme()).Build()
		cfg := config.HealthConfig{
			CheckTimeout:     5 * time.Second,
			EnabledPlatforms: []string{"kubevirt", "baremetal"},
		}
		startTime := time.Now().Add(-5 * time.Second)
		checker := health.NewChecker(k8sClient, cfg, "v1.0.0", startTime)

		result1 := checker.Check(context.Background())
		Expect(result1.Uptime).NotTo(BeNil())
		uptime1 := *result1.Uptime

		// Wait long enough for the integer-second uptime to increase.
		time.Sleep(1100 * time.Millisecond)

		result2 := checker.Check(context.Background())
		Expect(result2.Uptime).NotTo(BeNil())
		uptime2 := *result2.Uptime

		Expect(uptime2).To(BeNumerically(">", uptime1),
			"uptime at T2 should be greater than uptime at T1")
	})
})
