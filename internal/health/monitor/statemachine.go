// Package monitor implements SP health monitoring: periodic checks, state machine
// transitions, and provider lifecycle management.
package monitor

import v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"

// HealthCheckResult represents the outcome of a single SP health check.
type HealthCheckResult int

const (
	CheckHealthy   HealthCheckResult = iota // SP responded 200 + status:"healthy"
	CheckUnhealthy                          // SP responded 200 + status:"unhealthy"
	CheckFailed                             // Connection refused, timeout, non-200, unparseable
)

func (r HealthCheckResult) String() string {
	switch r {
	case CheckHealthy:
		return "healthy"
	case CheckUnhealthy:
		return "unhealthy"
	case CheckFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// StateMachine tracks SP health state transitions and failure counting.
type StateMachine struct {
	state            v1alpha1.ProviderStatus
	failureCounter   int
	failureThreshold int
}

// NewStateMachine creates a state machine with the given failure threshold and initial state.
func NewStateMachine(failureThreshold int, initialState v1alpha1.ProviderStatus) *StateMachine {
	return &StateMachine{
		state:            initialState,
		failureThreshold: failureThreshold,
	}
}

// RecordResult processes a health check result and returns the state transition (from, to).
func (sm *StateMachine) RecordResult(result HealthCheckResult) (from, to v1alpha1.ProviderStatus) {
	from = sm.state
	switch result {
	case CheckHealthy:
		sm.failureCounter = 0
		sm.state = v1alpha1.Ready
	case CheckUnhealthy:
		sm.failureCounter = 0
		sm.state = v1alpha1.Unhealthy
	case CheckFailed:
		if sm.state != v1alpha1.Unavailable {
			sm.failureCounter++
		}
		if sm.failureCounter >= sm.failureThreshold {
			sm.state = v1alpha1.Unavailable
		}
	}
	to = sm.state
	return from, to
}

// State returns the current health state.
func (sm *StateMachine) State() v1alpha1.ProviderStatus {
	return sm.state
}

// FailureCounter returns the current consecutive failure count.
func (sm *StateMachine) FailureCounter() int {
	return sm.failureCounter
}
