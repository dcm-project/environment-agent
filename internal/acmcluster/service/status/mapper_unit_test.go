package status_test

import (
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	condAvailable   = "Available"
	condProgressing = "Progressing"
	condDegraded    = "Degraded"
)

func cond(condType string, s metav1.ConditionStatus) metav1.Condition {
	return metav1.Condition{
		Type:               condType,
		Status:             s,
		LastTransitionTime: metav1.Now(),
	}
}

func deletionTS() *metav1.Time {
	t := metav1.NewTime(time.Now())
	return &t
}

var _ = DescribeTable("MapConditionsToStatus",
	func(conditions []metav1.Condition, deletionTimestamp *metav1.Time, expected v1alpha1.ClusterStatus) {
		result := status.MapConditionsToStatus(conditions, deletionTimestamp)
		Expect(result).To(Equal(expected))
	},

	Entry("TC-STS-UT-001: PENDING -- Progressing=Unknown",
		[]metav1.Condition{cond(condProgressing, metav1.ConditionUnknown)},
		nil,
		v1alpha1.ClusterStatusPENDING,
	),

	Entry("TC-STS-UT-002: PROVISIONING -- Progressing=True, Available=False",
		[]metav1.Condition{
			cond(condProgressing, metav1.ConditionTrue),
			cond(condAvailable, metav1.ConditionFalse),
		},
		nil,
		v1alpha1.ClusterStatusPROVISIONING,
	),

	Entry("TC-STS-UT-003: READY -- Available=True, Progressing=False",
		[]metav1.Condition{
			cond(condAvailable, metav1.ConditionTrue),
			cond(condProgressing, metav1.ConditionFalse),
		},
		nil,
		v1alpha1.ClusterStatusREADY,
	),

	Entry("TC-STS-UT-004: FAILED -- Degraded=True",
		[]metav1.Condition{cond(condDegraded, metav1.ConditionTrue)},
		nil,
		v1alpha1.ClusterStatusFAILED,
	),

	Entry("TC-STS-UT-005: DELETING -- deletionTimestamp set",
		[]metav1.Condition{
			cond(condAvailable, metav1.ConditionTrue),
			cond(condProgressing, metav1.ConditionFalse),
		},
		deletionTS(),
		v1alpha1.ClusterStatusDELETING,
	),

	Entry("TC-STS-UT-006: Precedence -- Degraded wins over Available",
		[]metav1.Condition{
			cond(condDegraded, metav1.ConditionTrue),
			cond(condAvailable, metav1.ConditionTrue),
		},
		nil,
		v1alpha1.ClusterStatusFAILED,
	),

	Entry("TC-STS-UT-007: Precedence -- deletionTimestamp overrides all",
		[]metav1.Condition{
			cond(condAvailable, metav1.ConditionTrue),
			cond(condProgressing, metav1.ConditionFalse),
			cond(condDegraded, metav1.ConditionTrue),
		},
		deletionTS(),
		v1alpha1.ClusterStatusDELETING,
	),

	Entry("TC-STS-UT-008: No conditions -- defaults to PENDING",
		[]metav1.Condition{},
		nil,
		v1alpha1.ClusterStatusPENDING,
	),

	Entry("TC-STS-UT-009: PROVISIONING -- Progressing=True, no Available condition",
		[]metav1.Condition{cond(condProgressing, metav1.ConditionTrue)},
		nil,
		v1alpha1.ClusterStatusPROVISIONING,
	),

	Entry("TC-STS-UT-010: READY -- Available=True, no Progressing condition",
		[]metav1.Condition{cond(condAvailable, metav1.ConditionTrue)},
		nil,
		v1alpha1.ClusterStatusREADY,
	),

	Entry("TC-STS-UT-011: UNAVAILABLE -- Available=False, Progressing=False",
		[]metav1.Condition{
			cond(condAvailable, metav1.ConditionFalse),
			cond(condProgressing, metav1.ConditionFalse),
		},
		nil,
		v1alpha1.ClusterStatusUNAVAILABLE,
	),

	Entry("TC-STS-UT-012: Precedence -- Degraded wins over UNAVAILABLE",
		[]metav1.Condition{
			cond(condDegraded, metav1.ConditionTrue),
			cond(condAvailable, metav1.ConditionFalse),
			cond(condProgressing, metav1.ConditionFalse),
		},
		nil,
		v1alpha1.ClusterStatusFAILED,
	),
)
