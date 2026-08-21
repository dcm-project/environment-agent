package status_test

import (
	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	condReady           = "Ready"
	condUpdatingVersion = "UpdatingVersion"
	condUpdatingConfig  = "UpdatingConfig"
)

var _ = DescribeTable("MapNodePoolConditionsToStatus",
	func(conditions []metav1.Condition, expectedStatus v1alpha1.ClusterStatus, expectedOk bool) {
		result, ok := status.MapNodePoolConditionsToStatus(conditions)
		Expect(ok).To(Equal(expectedOk))
		Expect(result).To(Equal(expectedStatus))
	},

	Entry("TC-NPM-UT-001: Ready=True → READY",
		[]metav1.Condition{cond(condReady, metav1.ConditionTrue)},
		v1alpha1.ClusterStatusREADY, true,
	),

	Entry("TC-NPM-UT-002: Ready=False → UNAVAILABLE",
		[]metav1.Condition{cond(condReady, metav1.ConditionFalse)},
		v1alpha1.ClusterStatusUNAVAILABLE, true,
	),

	Entry("TC-NPM-UT-003: UpdatingVersion=True → PROVISIONING",
		[]metav1.Condition{cond(condUpdatingVersion, metav1.ConditionTrue)},
		v1alpha1.ClusterStatusPROVISIONING, true,
	),

	Entry("TC-NPM-UT-004: UpdatingConfig=True → PROVISIONING",
		[]metav1.Condition{cond(condUpdatingConfig, metav1.ConditionTrue)},
		v1alpha1.ClusterStatusPROVISIONING, true,
	),

	Entry("TC-NPM-UT-005: No conditions → skip",
		[]metav1.Condition{},
		v1alpha1.ClusterStatus(""), false,
	),

	Entry("TC-NPM-UT-006: UpdatingVersion + Ready=False → PROVISIONING (precedence)",
		[]metav1.Condition{
			cond(condUpdatingVersion, metav1.ConditionTrue),
			cond(condReady, metav1.ConditionFalse),
		},
		v1alpha1.ClusterStatusPROVISIONING, true,
	),

	Entry("TC-NPM-UT-007: UpdatingVersion + Ready=True → PROVISIONING (precedence)",
		[]metav1.Condition{
			cond(condUpdatingVersion, metav1.ConditionTrue),
			cond(condReady, metav1.ConditionTrue),
		},
		v1alpha1.ClusterStatusPROVISIONING, true,
	),
)
