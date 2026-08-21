package status_test

import (
	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service/status"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeTable("ComputeCompositeStatus",
	func(hcStatus v1alpha1.ClusterStatus, npStatus *v1alpha1.ClusterStatus, expected v1alpha1.ClusterStatus) {
		result := status.ComputeCompositeStatus(hcStatus, npStatus)
		Expect(result).To(Equal(expected))
	},

	Entry("TC-CST-UT-001: HC READY + NP READY → READY",
		v1alpha1.ClusterStatusREADY, util.Ptr(v1alpha1.ClusterStatusREADY),
		v1alpha1.ClusterStatusREADY,
	),

	Entry("TC-CST-UT-002: HC READY + NP UNAVAILABLE → UNAVAILABLE",
		v1alpha1.ClusterStatusREADY, util.Ptr(v1alpha1.ClusterStatusUNAVAILABLE),
		v1alpha1.ClusterStatusUNAVAILABLE,
	),

	Entry("TC-CST-UT-003: HC FAILED + NP READY → FAILED",
		v1alpha1.ClusterStatusFAILED, util.Ptr(v1alpha1.ClusterStatusREADY),
		v1alpha1.ClusterStatusFAILED,
	),

	Entry("TC-CST-UT-004: HC READY + NP nil → READY",
		v1alpha1.ClusterStatusREADY, (*v1alpha1.ClusterStatus)(nil),
		v1alpha1.ClusterStatusREADY,
	),

	Entry("TC-CST-UT-005: HC READY + NP PROVISIONING → PROVISIONING",
		v1alpha1.ClusterStatusREADY, util.Ptr(v1alpha1.ClusterStatusPROVISIONING),
		v1alpha1.ClusterStatusPROVISIONING,
	),

	Entry("TC-CST-UT-006: HC DELETING + NP UNAVAILABLE → DELETING",
		v1alpha1.ClusterStatusDELETING, util.Ptr(v1alpha1.ClusterStatusUNAVAILABLE),
		v1alpha1.ClusterStatusDELETING,
	),

	Entry("TC-CST-UT-007: HC PROVISIONING + NP PROVISIONING → PROVISIONING",
		v1alpha1.ClusterStatusPROVISIONING, util.Ptr(v1alpha1.ClusterStatusPROVISIONING),
		v1alpha1.ClusterStatusPROVISIONING,
	),
)
