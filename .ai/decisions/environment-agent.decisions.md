# Design Decisions: Environment Agent

### DD-010: Hybrid SP model (embedded + external)

**Decision:** The agent supports both embedded SPs (compiled into the binary,
enabled via configuration) and external SPs (standalone processes registering via
REST).

**Rationale:** Embedded SPs provide low-latency in-process communication for
well-known service types (container, cluster, kubevirt). External SPs provide
extensibility for third-party or custom service types without modifying the agent
binary.

**Related requirements:** REQ-SPR-010, REQ-SPR-060

### DD-020: One SP per service type

**Decision:** Only one SP may serve a given service type per agent instance.

**Rationale:** Simplifies routing logic — no SP selection strategy needed. The
first SP to register claims the slot. Future iterations may support multiple SPs
per service type with selection strategies.

**Related requirements:** REQ-SPR-200

### DD-030: Messaging system for creation requests (pull model)

**Decision:** DCM publishes creation requests to a messaging system topic; the
agent pulls work from the topic rather than receiving direct REST calls.

**Rationale:** Removes the need for DCM-to-environment inbound connectivity for
creation requests. The agent initiates all connections outbound. Aligns with
Kubernetes-style pull-based reconciliation. Also provides inherent durability
and buffering during agent restarts.

**Related requirements:** REQ-MSG-010, REQ-MSG-060

### DD-040: Three-state health model for SPs

**Decision:** SP health uses Ready / Unhealthy / Unavailable states with
different routing behaviors for each.

**Rationale:** Differentiating Unhealthy from Unavailable avoids registration
flapping. An Unhealthy SP may recover quickly; removing and re-adding the
service type for transient issues would cause unnecessary load on DCM and
policies. Unavailable means the SP is gone and the service type should be
de-advertised.

**Related requirements:** REQ-HMN-050, REQ-HMN-060, REQ-HMN-070

### DD-050: Retry topic for unhealthy SP requests

**Decision:** When an SP is Unhealthy, requests are held in a dedicated retry
topic rather than rejected immediately.

**Rationale:** Gives the SP time to recover without losing requests. Requests
are processed event-driven (on SP recovery or unavailability transition), not
polled periodically. This avoids busy-waiting while ensuring prompt processing
when the SP recovers.

**Related requirements:** REQ-RTE-090, REQ-RCM-020

### DD-060: Cancel topic and deny list

**Decision:** DCM can cancel creation requests that have been re-routed to a
different agent, using a cancel topic and an in-memory deny list.

**Rationale:** Prevents stale creation requests from being processed after DCM
has re-evaluated and routed to a different agent. The deny list is rebuilt from
the cancel topic on startup. The double-crash risk (agent acknowledges cancel
then crashes before filtering the creation) is accepted — SP idempotent creation
is the final safety net.

**Related requirements:** REQ-RTE-140, REQ-RCM-120, REQ-RCM-130

### DD-070: Deterministic topic name

**Decision:** The main topic name is deterministic — either derived from the
agent's name or provided via configuration — ensuring reuse across restarts.

**Rationale:** Guarantees that unconsumed messages are not lost on restart. The
agent reconnects to the same topic and resumes processing. Also ensures DCM's
reference to the topic (from registration) remains valid.

**Related requirements:** REQ-MSG-010, REQ-MSG-040

### DD-080: Local persistence for SP registrations

**Decision:** SP registrations are persisted to local storage so that slot
ownership survives restarts.

**Rationale:** Without persistence, an agent restart would lose knowledge of
external SP registrations. External SPs that re-register would eventually
recover, but there would be a window where the agent incorrectly allows
embedded SPs to claim slots that belong to external SPs. Local persistence
closes this gap.

**Related requirements:** REQ-SPR-170, REQ-SPR-180

### DD-090: Pod conditions as non-fatal feature

**Decision:** Pod condition updates are best-effort. If the agent cannot update
pod conditions (running outside K8s, missing RBAC), it logs a warning and
continues.

**Rationale:** The agent must operate in multiple deployment modes (standalone,
Docker, Kubernetes). Pod conditions are a convenience feature for K8s
environments and should not block agent operation in other environments.

**Related requirements:** REQ-HMN-270

### DD-100: Heartbeat-based agent liveness (REST, not messaging)

**Decision:** The agent reports liveness to DCM via REST heartbeats rather than
through the messaging system.

**Rationale:** The messaging system is used for resource operations. Using a
separate channel (REST) for liveness provides independent failure detection —
if the messaging system is down, DCM can still determine whether the agent is
alive. The agent already has outbound REST connectivity to DCM for registration.

**Related requirements:** REQ-DCM-140

### DD-110: Deny list consume-on-use and LRU eviction

**Decision:** Deny list entries are removed once consumed (used to filter a
matching creation request). If the deny list exceeds a configurable maximum size
(`AGENT_DENY_LIST_MAX_SIZE`), the oldest entries are evicted using LRU.

**Rationale:** The enhancement states entries remain for the process lifetime.
The spec refines this with two additions: (1) consume-on-use — once a
cancellation filters its matching creation request, the transaction is complete
and the entry serves no further purpose; keeping it wastes memory and could
interfere with future legitimate requests for the same resourceId. (2) LRU
eviction — an unbounded in-memory structure that grows until process exit is not
production-safe; size-based eviction caps memory usage. On restart, the deny
list is rebuilt from the cancel topic's durable consumer, so no entries are
permanently lost. A future refinement may use time-based (TTL) eviction instead
of or in addition to size-based LRU.

**Related requirements:** REQ-RTE-190

### DD-120: SP registration lease expiry deferred (v1alpha1)

**Decision:** No consequences are defined for SP registration non-renewal in
v1alpha1. External SPs that stop re-registering retain their slot indefinitely.

**Rationale:** Designing automatic slot reclamation requires defining timeout
semantics, grace periods, and notification mechanisms. This is deferred to a
future version to limit initial scope. Manual intervention (clearing local
persistence) is the v1alpha1 escape hatch. The agent accepts periodic
re-registration idempotently but does not enforce lease renewal.

**Related requirements:** REQ-SPR-170, REQ-SPR-190

### DD-130: Immediate cancel processing for retry-held requests

**Decision:** When a cancel CloudEvent arrives for a resourceId that is already
queued in the retry topic, the agent immediately consumes the retry topic,
removes the matching message, re-publishes non-matching messages, and publishes
a `cancel-acknowledged` CloudEvent.

**Rationale:** The enhancement specifies immediate removal of cancelled requests
from the retry topic. The original spec deferred this to the next health state
transition (Ready), which could leave cancelled requests sitting in the retry
topic indefinitely if the SP remains Unhealthy. Immediate processing ensures
DCM receives the cancellation acknowledgment promptly, allowing it to proceed
with re-evaluation without waiting for an SP health transition. The cost of
consuming and re-publishing the retry topic is acceptable given that cancels are
an exceptional path and the retry topic is expected to be small.

**Related requirements:** REQ-RTE-170

### DD-140: Enhancement doc v1 vs v1alpha1 API version

**Decision:** The enhancement document references `/api/v1/` endpoints as the
target stable API. The current implementation uses `/api/v1alpha1/` as we are in
the alpha phase. This is intentional — the enhancement describes the GA target;
the implementation reflects current maturity.

**Rationale:** v1alpha1 signals to consumers that the API contract may change.
The enhancement is a forward-looking design document, not a snapshot of the
current implementation. When the API stabilizes, routes will migrate to v1 and
the enhancement will reflect the implementation.

**Related requirements:** REQ-HTTP-020

**Ref:** MF-R3 from multi-model review panel (Codex, 2026-07-20)

### DD-150: Non-strict handler pattern (v1alpha1)

**Decision:** Use `HandlerWithOptions` with `server.Unimplemented{}` for Topic 1
instead of the peer-standard `NewStrictHandlerWithOptions` pattern. Migrate to
strict handlers when Topic 2 (Health Service) or Topic 3 (SP Registration)
introduces real handler implementations.

**Rationale:** Strict handlers require typed request/response structs that don't
yet exist for stub endpoints. The non-strict pattern is simpler for Topic 1's
placeholder handlers. All 4 peer repos use strict handlers — alignment will
happen when real handlers are implemented.

**Related requirements:** REQ-HTTP-020

### DD-160: Constructor lifecycle alignment to peer pattern

**Decision:** Align the HTTP server constructor to the dcm-project peer pattern:
`New(cfg, logger, handler)` + `Run(ctx, ln)` where the listener is passed at
runtime, not at construction time. This matches all 4 active peer repos.

**Rationale:** Keeping constructor pure (no I/O resources) and passing the
listener at runtime boundary improves testability and cross-repo consistency.
`Addr()` reads from a field set during `Run()`. Tests that previously passed the
listener to `New()` are updated to pass it to `Run()` instead.

**Related requirements:** REQ-HTTP-010

### DD-170: Timeout middleware wall-clock limitation (v1alpha1)

**Decision:** Accept that the sync timeout middleware does not bound wall-clock
response time for handlers that ignore `ctx.Done()`. Document as known v1alpha1
limitation. When Topic 8 (SP Forwarding) adds real external HTTP calls, evaluate
whether goroutine+select timeout is needed.

**Rationale:** The sync approach calls `next.ServeHTTP()` and only checks
deadline after the handler returns. If a handler ignores context cancellation,
the client is held until the handler finishes. For v1alpha1, all handlers are
stub/placeholder and the risk is theoretical. The per-request timeout AC
(AC-HTTP-095) is satisfied for context-aware handlers.

**Related requirements:** REQ-HTTP-110
