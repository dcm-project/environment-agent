// Package monitoring watches Kubernetes PVCs managed by DCM and publishes
// status change events via CloudEvents over NATS.
package monitoring

import "time"

// MonitorConfig holds configuration for the status monitoring subsystem.
type MonitorConfig struct {
	Namespace              string
	DebounceMs             int
	ResyncPeriod           time.Duration
	PublishMaxAttempts     int
	ShutdownPublishTimeout time.Duration // zero uses default (30s); flush-only NATS publish budget after SIGTERM
}
