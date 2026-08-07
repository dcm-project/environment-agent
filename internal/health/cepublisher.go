package health

import (
	"context"
	"log/slog"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/provider/store"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// ProviderLookup resolves a provider's stored metadata by ID.
type ProviderLookup interface {
	GetByID(ctx context.Context, id string) (*store.StoredProvider, error)
}

// CEPublisher publishes health-related CloudEvents when provider state
// transitions to degraded or unavailable (REQ-HMN-120, REQ-HMN-145).
type CEPublisher struct {
	store     ProviderLookup
	publisher routing.Publisher
	logger    *slog.Logger
	agentName string
	topicName string
}

// NewCEPublisher creates a CEPublisher. Panics if required deps are nil.
func NewCEPublisher(store ProviderLookup, publisher routing.Publisher, logger *slog.Logger, agentName, topicName string) *CEPublisher {
	if store == nil {
		panic("health: ProviderLookup must not be nil")
	}
	if publisher == nil {
		panic("health: Publisher must not be nil")
	}
	if logger == nil {
		panic("health: logger must not be nil")
	}
	return &CEPublisher{
		store:     store,
		publisher: publisher,
		logger:    logger,
		agentName: agentName,
		topicName: topicName,
	}
}

// OnTransition is a monitor.TransitionFunc-compatible callback that publishes
// health CEs on transitions to Unhealthy or Unavailable.
func (p *CEPublisher) OnTransition(ctx context.Context, providerID string, _, to v1alpha1.ProviderStatus) {
	var ceType string
	var reason string
	switch to {
	case v1alpha1.Unhealthy:
		ceType = cloudevent.TypeHealthDegraded
		reason = "service provider health check failures exceeded threshold"
	case v1alpha1.Unavailable:
		ceType = cloudevent.TypeHealthUnavailable
		reason = "service provider became unavailable"
	default:
		return
	}

	sp, err := p.store.GetByID(ctx, providerID)
	if err != nil || sp == nil {
		p.logger.Warn("failed to resolve provider for health CE", "providerID", providerID, "error", err)
		return
	}

	data := routing.HealthEventData{
		AgentID:          p.agentName, // DD-200: agentId not available pre-registration; uses agentName in v1alpha1
		AgentName:        p.agentName,
		TopicName:        p.topicName,
		ServiceType:      sp.ServiceType,
		Reason:           reason,
		AffectedProvider: sp.Name,
	}
	if err := cloudevent.PublishCE(ctx, p.publisher.Publish, cloudevent.SubjectHealth, p.agentName, ceType, data); err != nil {
		p.logger.Warn("failed to publish health CE", "type", ceType, "providerID", providerID, "error", err)
	}
}
