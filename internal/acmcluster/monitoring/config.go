// Package monitoring watches HostedCluster and NodePool resources and publishes status CloudEvents.
package monitoring

import "time"

// MonitorConfig holds runtime configuration for the status monitor.
type MonitorConfig struct {
	Namespace            string
	ProviderName         string
	DebounceInterval     time.Duration
	ResyncInterval       time.Duration
	PublishRetryMax      int
	PublishRetryInterval time.Duration
}
