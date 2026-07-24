// Package health implements the health service domain logic.
package health

import (
	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/ptr"
)

// MessagingStatus reports messaging system connectivity.
// Implementations MUST return cached in-memory state only.
// IsConnected MUST NOT perform network I/O, disk reads, or block.
// Implementations MUST be safe for concurrent use by multiple goroutines.
type MessagingStatus interface {
	IsConnected() bool
}

// Service provides health status based on agent subsystem state.
type Service struct {
	messaging MessagingStatus
}

// NewService creates a health service with the given messaging status source.
func NewService(m MessagingStatus) *Service {
	if m == nil {
		panic("health: MessagingStatus must not be nil")
	}
	return &Service{messaging: m}
}

// Status returns the current health state. It reads only in-memory state
// and completes in constant time with no I/O (REQ-HLT-070).
func (s *Service) Status() v1alpha1.Health {
	status := "healthy"
	if !s.messaging.IsConnected() {
		status = "unhealthy"
	}
	return v1alpha1.Health{
		Status: ptr.To(status),
		Path:   ptr.To("health"),
	}
}
