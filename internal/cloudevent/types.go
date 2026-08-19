package cloudevent

// CE type constants for agent-originated events.
const (
	TypeError             = "dcm.agent.error"
	TypeRequestQueued     = "dcm.agent.request-queued"
	TypeCancelAcked       = "dcm.agent.cancel-acknowledged"
	TypeCancelRejected    = "dcm.agent.cancel-rejected"
	TypeCreationAcked     = "dcm.agent.creation-acknowledged"
	TypeDeletionAcked     = "dcm.agent.deletion-acknowledged"
	TypeHealthDegraded    = "dcm.agent.health.service-type-degraded"
	TypeHealthUnavailable = "dcm.agent.health.service-type-unavailable"
)

// CE type constants for control-plane-originated inbound requests.
const (
	TypeRequestCreate = "dcm.request.create"
	TypeRequestDelete = "dcm.request.delete"
	TypeRequestCancel = "dcm.request.cancel"
)

// SourceControlPlane is the source identifier for control-plane-originated CEs.
const SourceControlPlane = "//dcm/control-plane"

// NATS subjects for agent CEs.
const (
	SubjectResponses = "dcm.agents.responses"
	SubjectHealth    = "dcm.agents.health"
)
