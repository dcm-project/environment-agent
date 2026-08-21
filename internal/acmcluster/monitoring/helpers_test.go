package monitoring_test

import (
	"context"
	"sync"
	"time"

	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster"
	"github.com/dcm-project/environment-agent/internal/acmcluster/monitoring"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const (
	testNamespace    = "clusters"
	testProviderName = "acm-cluster-sp"
)

// ── Mock StatusPublisher ─────────────────────────────────────────────

// mockStatusPublisher captures published events for assertions.
type mockStatusPublisher struct {
	mu        sync.Mutex
	events    []monitoring.StatusEvent
	err       error
	errFn     func() error
	callCount int
}

var _ monitoring.StatusPublisher = (*mockStatusPublisher)(nil)

func newMockPublisher() *mockStatusPublisher {
	return &mockStatusPublisher{}
}

func (m *mockStatusPublisher) Publish(_ context.Context, event monitoring.StatusEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	if m.errFn != nil {
		if err := m.errFn(); err != nil {
			return err
		}
	} else if m.err != nil {
		return m.err
	}
	m.events = append(m.events, event)
	return nil
}

func (m *mockStatusPublisher) Close() error { return nil }

func (m *mockStatusPublisher) Events() []monitoring.StatusEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]monitoring.StatusEvent, len(m.events))
	copy(result, m.events)
	return result
}

func (m *mockStatusPublisher) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

func (m *mockStatusPublisher) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
	m.errFn = nil
}

func (m *mockStatusPublisher) SetErrorFunc(fn func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = nil
	m.errFn = fn
}

func (m *mockStatusPublisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = nil
	m.callCount = 0
	m.err = nil
}

// ── Dynamic fake client ──────────────────────────────────────────────

func newDynamicFakeClient(objects ...*unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	objs := make([]runtime.Object, len(objects))
	for i, obj := range objects {
		objs[i] = obj
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			util.HostedClusterGVR: util.HostedClusterListGVK.Kind,
			util.NodePoolGVR:      util.NodePoolListGVK.Kind,
		},
		objs...,
	)
}

// ── Unstructured resource builders ──────────────────────────────────

type unstructuredOption func(*unstructured.Unstructured)

func buildUnstructuredResource(kind, name, instanceID string, conditions []metav1.Condition, opts ...unstructuredOption) *unstructured.Unstructured {
	labels := cluster.DCMLabels(instanceID)

	condList := make([]any, 0, len(conditions))
	for _, c := range conditions {
		condMap := map[string]any{
			"type":               c.Type,
			"status":             string(c.Status),
			"lastTransitionTime": c.LastTransitionTime.Format(time.RFC3339),
			"reason":             c.Reason,
			"message":            c.Message,
		}
		condList = append(condList, condMap)
	}

	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "hypershift.openshift.io/v1beta1",
			"kind":       kind,
			"metadata": map[string]any{
				"name":      name,
				"namespace": testNamespace,
				"labels":    toStringInterfaceMap(labels),
			},
			"status": map[string]any{
				"conditions": condList,
			},
		},
	}

	for _, opt := range opts {
		opt(obj)
	}
	return obj
}

func buildUnstructuredHostedCluster(name, instanceID string, conditions []metav1.Condition, opts ...unstructuredOption) *unstructured.Unstructured {
	return buildUnstructuredResource("HostedCluster", name, instanceID, conditions, opts...)
}

func toStringInterfaceMap(m map[string]string) map[string]any {
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func withDeletionTimestamp(t time.Time) unstructuredOption {
	return func(obj *unstructured.Unstructured) {
		ts := metav1.NewTime(t)
		obj.SetDeletionTimestamp(&ts)
	}
}

func withoutDCMLabels() unstructuredOption {
	return func(obj *unstructured.Unstructured) {
		obj.SetLabels(map[string]string{})
	}
}

func withoutInstanceIDLabel() unstructuredOption {
	return func(obj *unstructured.Unstructured) {
		labels := obj.GetLabels()
		delete(labels, cluster.LabelInstanceID)
		obj.SetLabels(labels)
	}
}

// ── Default MonitorConfig ────────────────────────────────────────────

func defaultMonitorConfig() monitoring.MonitorConfig {
	return monitoring.MonitorConfig{
		Namespace:            testNamespace,
		ProviderName:         testProviderName,
		DebounceInterval:     50 * time.Millisecond,
		ResyncInterval:       10 * time.Minute,
		PublishRetryMax:      3,
		PublishRetryInterval: 10 * time.Millisecond,
	}
}

// ── Condition presets ────────────────────────────────────────────────

func readyConditions() []metav1.Condition {
	return []metav1.Condition{
		{Type: "Available", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()},
		{Type: "Progressing", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now()},
	}
}

func provisioningConditions() []metav1.Condition {
	return []metav1.Condition{
		{Type: "Progressing", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()},
		{Type: "Available", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now()},
	}
}

func failedConditions() []metav1.Condition {
	return []metav1.Condition{
		{Type: "Degraded", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now(), Message: "etcd cluster is degraded"},
	}
}

func failedConditionsWithMessage(msg string) []metav1.Condition {
	return []metav1.Condition{
		{Type: "Degraded", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now(), Message: msg},
	}
}

func unavailableConditions() []metav1.Condition {
	return []metav1.Condition{
		{Type: "Available", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now(), Message: "Kube API server is not ready"},
		{Type: "Progressing", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now()},
	}
}

func unavailableConditionsWithMessage(msg string) []metav1.Condition {
	return []metav1.Condition{
		{Type: "Available", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now(), Message: msg},
		{Type: "Progressing", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now()},
	}
}

// ── Unstructured NodePool builder ───────────────────────────────────

func buildUnstructuredNodePool(name, instanceID string, conditions []metav1.Condition) *unstructured.Unstructured {
	return buildUnstructuredResource("NodePool", name, instanceID, conditions)
}

// ── Event lookup helper ────────────────────────────────────────────

func lastEventForInstance(events []monitoring.StatusEvent, instanceID string) monitoring.StatusEvent {
	var last monitoring.StatusEvent
	for _, e := range events {
		if e.InstanceID == instanceID {
			last = e
		}
	}
	return last
}

// ── NodePool condition presets ──────────────────────────────────────

func npReadyConditions() []metav1.Condition {
	return []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()},
	}
}

func npNotReadyConditions() []metav1.Condition {
	return npNotReadyConditionsWithMessage("0 of 3 machines are ready")
}

func npNotReadyConditionsWithMessage(msg string) []metav1.Condition {
	return []metav1.Condition{
		{
			Type: "Ready", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now(),
			Message: msg,
		},
	}
}

func npUpdatingVersionConditions() []metav1.Condition {
	return []metav1.Condition{
		{
			Type: "UpdatingVersion", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now(),
			Message: "Updating from 4.16.0 to 4.17.0",
		},
		{Type: "Ready", Status: metav1.ConditionFalse, LastTransitionTime: metav1.Now()},
	}
}
