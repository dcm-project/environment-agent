package cloudevent

// CE type constants for agent-originated events.
const (
	TypeError          = "dcm.agent.error"
	TypeRequestQueued  = "dcm.agent.request-queued"
	TypeCancelAcked    = "dcm.agent.cancel-acknowledged"
	TypeCancelRejected = "dcm.agent.cancel-rejected"
	TypeCreationAcked  = "dcm.agent.creation-acknowledged"
	TypeDeletionAcked  = "dcm.agent.deletion-acknowledged"
)

// SubjectResponses is the NATS subject for agent response CEs.
const SubjectResponses = "dcm.agents.responses"
