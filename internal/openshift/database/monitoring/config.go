// Package monitoring watches Kubernetes resources managed by DCM and publishes
// status change events via CloudEvents over NATS.
package monitoring

import "time"

// MonitorConfig holds configuration for the status monitoring subsystem.
type MonitorConfig struct {
	Namespace             string
	ProviderName          string
	DebounceMs            int
	ResyncPeriod          time.Duration
	PublishMaxAttempts    int // maximum publish attempts per event; 0 defaults to 5
	PublishTimeoutSeconds int // per-publish deadline in seconds; 0 defaults to 30
}
