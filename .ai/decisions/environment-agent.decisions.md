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

**Resolved (2026-08-07):** Strict handler wired in production and in
`internal/health/health_integration_test.go` (IT-HLT-010/020/030/040 now run
through the real handler chain, not a stub). Gate satisfied.

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

### DD-180: Health state keyed by StoredProvider.ID

**Decision:** The HealthTracker interface is keyed by `StoredProvider.ID` (UUID),
not by provider name. The routing subsystem uses `sp.ID` from the store record
when querying health state.

**Rationale:** Provider names are human-readable registration identifiers; IDs
are system-generated UUIDs that survive re-registration. The health monitor and
provider service both use `sp.ID` as the authoritative key when calling
`SetState`. The router must use the same key for lookups. The trust boundary is:
after `store.GetByName()` returns a `StoredProvider`, its `.ID` field matches
the key used by the health subsystem (guaranteed by the single-writer
registration path in `provider.Service`).

**Related requirements:** REQ-HMN-050, REQ-RTE-030, REQ-RTE-025, REQ-RTE-026

**Amendment (Topic 9 hardening):** `ResolveProvider` treats a store record with
an empty `.ID` (registry/store data corruption — should be unreachable given
the single-writer registration path) as Unavailable rather than querying
health state with an empty key or routing to it. See REQ-RTE-026.

**Amendment (PR #19 review):** A review pass initially recommended removing
this branch outright as dead code per `quality.mdc`'s single-point-of-defense
rule, since it had zero test coverage. On reconsideration, the branch is kept
and tested (`IT-RTE-150`) rather than deleted, but not for the reason
originally given here: `store.FileStore` already rejects empty-`.ID` records
on both `Save` and every read (`validateStoredProvider`), so under the
production `store.NewFileStore` wiring this branch is unreachable — on-disk
corruption surfaces as a `GetByName` **error** (line 30-32's branch), not as
a `StoredProvider` with an empty `.ID`. The actual justification is narrower:
`ResolveProvider` is written against the `store.Store` *interface*, which
carries no such non-empty-`.ID` guarantee in its contract. The defense is for
that interface gap — a future or alternate `Store` implementation that
doesn't validate as strictly as `FileStore` — not for file I/O corruption
specifically. `routingtest.FakeStore` (used by `IT-RTE-150`) is exactly such
a non-validating implementation, which is why the test can reach this branch
at all.

### DD-190: SP-side idempotency for JetStream redelivery protection

**Decision:** The router does NOT implement message-level deduplication for
JetStream redeliveries. Protection against duplicate side effects is the
service provider's responsibility (idempotent create/delete operations).

**Rationale:** The `claimedResourcesSet` is a cancel-rejection ledger that
persists across the resource lifecycle (create → delete). It cannot double as a
message-level dedup mechanism without distinguishing operation type. A naive
`AddIfAbsent` short-circuit would block legitimate delete-after-create
operations. Proper message dedup would require CE event ID tracking or a
TTL-based idempotency key store, which is deferred to Topic 9 (retry/lifecycle
redesign). The ack-error logging in messaging handlers provides operational
visibility into redelivery occurrences.

**Related requirements:** REQ-RTE-180, REQ-RTE-200, REQ-RTE-210, REQ-MSG-116

**Amendment (Topic 8 hardening):** The agent sets explicit JetStream `AckWait`
(default 120s for main, 10s for cancel) to prevent spurious redelivery during
in-line SP retries. This is a static stopgap; Topic 9 will introduce
`InProgress()` heartbeats, `MaxDeliver` with terminal error CE emission, handler
context deadlines, and CE event ID forwarding as `Idempotency-Key` to enable
proper SP-side event-level dedup. SP idempotency for create/delete by resourceId
is a MUST requirement (not merely an assumption).

**Amendment (Topic 9, REQ-RTE-210 — concurrent double-dispatch):** The above rationale
covers *sequential* redeliveries (message-level dedup, deliberately not
implemented). It does not cover two *concurrent* forward attempts for the same
`resource_id` racing each other into the SP simultaneously — e.g. a JetStream
redelivery firing while the first attempt is still in flight, or the main-topic
and retry-topic consumers processing the same resource at once. Unlike
sequential redelivery, concurrent racing serves no purpose (the redelivery
would resolve on its own once the first attempt acks) and is cheap to prevent
without the trade-offs a permanent dedup ledger would introduce. The agent adds
a transient in-flight lock (`Router.inFlight` / shared `InFlightSet`), keyed by
`resource_id`, held only for the duration of a single forward attempt and
released unconditionally when it completes. This is intentionally a separate
structure from `claimedResourcesSet`: that ledger lives for the
whole create→delete lifecycle and must keep allowing delete-after-create
(REQ-RTE-200); the in-flight lock lives only for one forward call and blocks
nothing but true concurrency (REQ-RTE-210).

### DD-200: CloudEvent source uses agentName (v1alpha1)

**Decision:** CloudEvent source is `dcm/agents/{agentName}`, not
`dcm/agents/{agent_id}`, in v1alpha1.

**Rationale:** The DCM-assigned `agent_id` is only available after successful
registration (`POST /api/v1alpha1/agents` → 201). CloudEvents are published before
registration completes (e.g., health degraded CEs during startup health checks,
error CEs for unsupported service types). Using `agentName` — which is a required
config value available from startup — provides a stable, always-available source
identifier. Switching to `agent_id` post-registration would create a split-brain
where CEs from the same agent session carry different source values, complicating
control plane correlation. A future version may introduce a dynamic source that
switches to `agent_id` after registration, but v1alpha1 accepts this trade-off.

**Related requirements:** REQ-XC-CE-030

### DD-210: CloudEvent data payload snake_case (AEP convention)

**Decision:** All CloudEvent `data` JSON field names exchanged with the control
plane use snake_case (`resource_id`, `service_type`, `agent_name`, `topic_name`,
etc.), matching the control-plane's AEP-style structs.

**Rationale:** Go's `encoding/json` does not fold underscores — camelCase tags
cannot bind to snake_case wire payloads, causing silent message drops in both
directions when casing diverges. Internal Go identifiers and config env vars
(e.g. `AGENT_TOPIC_NAME`, struct field `TopicName`) remain unchanged; only
marshaled JSON field names follow snake_case.

**Related requirements:** REQ-MSG-130, REQ-RCM-140, REQ-XC-CE-010

### DD-220: Control-plane topic prefix (`dcm.agent.`)

**Decision:** Control-plane-facing request subjects are prefixed with
`dcm.agent.`. The agent derives subjects from an unprefixed **base name**
(`AGENT_TOPIC_NAME` or `AGENT_NAME`):

- Main: `dcm.agent.{base}` (advertised to DCM as `topic_name`)
- Cancel: `dcm.agent.{base}.cancel`
- Retry (agent-internal): `{base}.retry` — **no prefix**

**Rationale:** The control-plane owns a wildcard JetStream stream
(`dcm-agent-requests`, subject `dcm.agent.>`). Registration requires
`topic_name` to match `^dcm\.agent\..+`. The retry subject is never published to
by the control plane, so it stays unprefixed and agent-owned.

**Related requirements:** REQ-MSG-010, REQ-MSG-030, REQ-MSG-050

### DD-230: JetStream stream ownership split (CP vs agent)

**Decision:** The control plane owns JetStream streams for CP-facing subjects.
The agent MUST NOT create streams on those subjects. Specifically:

| Stream | Owner | Subject binding | Agent action |
|--------|-------|-----------------|--------------|
| `dcm-agent-requests` | Control plane | `dcm.agent.>` | Create durable consumers filtered to its main and cancel subjects |
| `dcm-agent-responses` | Control plane | `dcm.agents.responses` | Publish directly (no stream creation) |
| `{base}-retry` | Agent | `{base}.retry` | CreateOrUpdateStream |
| `dcm-health` | Agent | `dcm.agents.health` | CreateOrUpdateStream |

On startup, if `dcm-agent-requests` does not exist yet, the agent retries
durable consumer creation every 2s for up to 30s (phase 1, synchronous with
startup). If it's still missing after that, the agent does NOT give up — it
retries the full setup again every 30s in the background, indefinitely
(phase 2), until it succeeds or the agent shuts down. A hard give-up would
leave the agent silently consuming nothing if the CP takes longer than 30s to
start (e.g. a rolling restart), with no further chance to recover short of a
manual agent restart.

**Rationale:** NATS rejects overlapping stream subject bindings (error 10065).
Startup order between control plane and agent is not guaranteed. The agent only
administers resources it owns; CP-facing traffic uses CP-provisioned streams.

**Related requirements:** REQ-MSG-048, REQ-MSG-049, REQ-MSG-051, REQ-MSG-140

### DD-240: JetStream publish dedup via CloudEvent id (Nats-Msg-Id)

**Decision:** All agent-originated response and health CloudEvents are published
to JetStream with the `Nats-Msg-Id` header set to the CloudEvent's own `id`.

**Rationale:** Enables server-side deduplication if publish retry logic is added
later. Without a stable message ID, a retried publish could duplicate delivery
to the control-plane's response consumer.

**Related requirements:** REQ-MSG-135, REQ-XC-CE-050

### DD-250: Embedded SP operation handlers deferred (v1alpha1)

**Decision:** Embedded SPs register correctly (REQ-SPR-010–050) and are health-monitored, but no embedded SP ever actually serves a create/delete operation in v1alpha1 — `routing.NewForwarder` is constructed with no `Embedded` field (`cmd/environment-agent/main.go`), so `Forwarder.embedded` is always empty and any operation routed to an embedded SP fails with a 503. No concrete `routing.EmbeddedHandler` implementation exists anywhere except a test fake.

**Rationale:** A full requirements-coverage audit confirmed this is not a wiring bug but a genuinely unbuilt feature — the interface and `EMBEDDED_SPS` config exist as scaffolding for a future capability. The agent is fully functional today via external SPs, which is the only registration path actually exercised in practice. Building real embedded-SP operational logic (e.g. an in-process container/cluster/kubevirt handler) is a substantial feature, not a bug fix, and is out of scope for this hardening pass.

**Related requirements:** REQ-RTE-030, REQ-SPR-040, REQ-SPR-050, REQ-RCM-230

### DD-260: DCM resource capacity reporting deferred (v1alpha1)

**Decision:** `REQ-DCM-030`'s `resources_available` field is permanently omitted from DCM registration/heartbeat payloads — `main.go` passes a literal `nil` `resourceProvider` to `dcm.NewRegistrar`. The registrar's conditional logic correctly omits the field when the provider is nil; there is simply no implementation anywhere that computes real resource capacity to plug into that provider interface.

**Rationale:** Confirmed by the 2026-08-07 audit. No embedded SP or subsystem in v1alpha1 currently has a well-defined notion of "resource capacity" to report (this only becomes meaningful once real embedded SP operational logic exists — see DD-250). Defining that data source now, without a concrete consumer, would be speculative. Deferred until a real capacity source exists.

**Related requirements:** REQ-DCM-030

### DD-270: Kubernetes pod conditions unimplemented (v1alpha1)

**Decision:** REQ-HMN-190 through REQ-HMN-270 (9 requirements covering surfacing SP health as Kubernetes pod conditions) are entirely unimplemented. Only a dead config flag (`Health.PodConditionsEnabled`, never read past `config.go`) exists; there is no Kubernetes client dependency anywhere in the module. `IT-HMN-140/150/160` assert generic health-check/liveness facts and never touch pod conditions.

**Rationale:** Confirmed by the 2026-08-07 audit as the single largest, most-corroborated gap in the entire audit (4 independent agents across 2 rounds, zero dissent). This is a real backlog item, not an oversight to silently drop — DD-090 already anticipated pod conditions as a best-effort, non-fatal feature, but the feature itself was never built. Tracked here so the dead config flag doesn't continue to imply a capability that doesn't exist. Implementation (client-go dependency, RBAC, condition-update loop) is deferred to a future version.

**Related requirements:** REQ-HMN-190, REQ-HMN-200, REQ-HMN-210, REQ-HMN-220, REQ-HMN-230, REQ-HMN-240, REQ-HMN-250, REQ-HMN-260, REQ-HMN-270

### DD-280: "Unbounded consumer slice" audit finding is a false positive

**Decision:** No code change was made for the audit's "reconnect
consumer-tracking slice grows unbounded" finding. Code inspection of
`messaging.Client.attemptSetup` shows `c.consumers` is only ever appended to
once per client lifetime: `attemptSetup`'s `setupDone` guard short-circuits
before `setupStreamsAndConsume` (and therefore `beginConsuming`) can run
again, and `beginConsuming` has its own independent `consuming`-flag guard
as a second line of defense. A NATS reconnect re-fires
`ConnectHandler`/`ReconnectHandler` → `doSetup`, but by that point
`setupDone` is already `true`, so the append path is unreachable.

**Rationale:** The original finding predates the `DeferConsume` refactor
and assumed each reconnect re-ran full consumer setup. Verified false via
code inspection plus a regression test
(`TestAttemptSetup_SetupDoneShortCircuits`,
`TestBeginConsuming_ConsumingFlagShortCircuits` in
`internal/messaging/client_setup_test.go`) that fails if either guard is
removed. Documented here rather than silently dropped so a future
refactor that touches these guards is aware of the invariant they protect.

**Related requirements:** REQ-MSG-080, REQ-MSG-100

### DD-290: Composition-root ordering constraint — SetOnTransition before RegisterEmbedded

**Decision:** In `cmd/environment-agent/main.go`, `healthMonitor.SetOnTransition` (and
`providerSvc.SetOnChange`) MUST be wired before `providerSvc.RegisterEmbedded` is called.

**Rationale:** `RegisterEmbedded` → `registerEmbeddedType` calls `monitor.Monitor.RegisterProvider`
with `initialCheck=true`, which runs the embedded SP's health check synchronously and invokes the
monitor's `onTransition` callback in-line — before `RegisterProvider` even returns — if the check
result differs from the assumed initial status. If that callback isn't set yet (`main.go`
previously called `RegisterEmbedded` immediately after `LoadPersisted`, long before
`registrar`/`retryProcessor`/`healthCEPub` exist to be wired into the callback), that transition is
silently dropped: no retry-topic reprocessing, no health CloudEvent, and DCM re-registration only
happens to be compensated for by the unconditional `NotifyServiceTypeChange()` kick later in
`run()`. This is a general property of `Monitor.RegisterProvider`'s `initialCheck` path, not
specific to embedded SPs — any future caller of `RegisterProvider(..., initialCheck: true)` must
wire `SetOnTransition` first for the same reason. Verified via
`internal/provider/service/service_test.go`'s "RegisterEmbedded initialCheck transition ordering"
tests, which demonstrate the callback fires when wired first and is silently dropped when
wired after — proving the hazard is real, not merely theoretical.

**Related requirements:** REQ-SPR-030, REQ-HMN-100, REQ-HMN-120

### DD-300: Registry slot mutations verify current ownership before releasing

**Decision:** `provider.Registry.Move` now returns an error if `oldType` is currently held by a
provider other than the caller, instead of unconditionally deleting whatever occupies that slot.
Similarly, `service.ProviderService.removeStaleEmbedded` now checks `registry.Lookup(serviceType)`
against the stale embedded provider's own name before calling `Release`, instead of releasing
unconditionally.

**Rationale:** Both are single-slot-invariant (REQ-SPR-200)
violations reachable if the registry and persisted store ever desync: (a) `Move`'s unconditional
`delete(r.slots, oldType)` could delete a *different* provider's active slot; (b)
`removeStaleEmbedded`'s unconditional `Release` could free an external provider's slot out from
under it, since `LoadPersisted` (which claims external providers' registry slots) runs before
`RegisterEmbedded` at startup — so the store can legitimately contain both a stale embedded record
and a newer external registration for the same service type. Both fixes are defensive ownership
checks, not behavior changes for the expected (non-desynced) path — verified by
`TestProvider`'s ownership-check cases and `TestService`'s slot-release cases, which cover both
the desynced and normal-path outcomes.

**Related requirements:** REQ-SPR-200

### DD-310: `FileStore` fsyncs before and after rename

**Decision:** `FileStore.writeFile` now opens the temp file explicitly, writes, calls `Sync()` on
it, closes it, renames it into place, then opens the parent directory and calls `Sync()` on that
too — rather than `os.WriteFile` + `os.Rename` with no explicit fsync at all. A `syncFn` field
(defaulting to `(*os.File).Sync`, overridable in package-internal tests) makes the fsync call
order/count directly observable in unit tests without needing to simulate a real crash.

**Rationale:** Considered moving to an embedded SQLite-backed store
for stronger durability guarantees. Decided against that for now — fsync-before-rename is the
standard, dependency-free pattern for durable atomic file writes on POSIX filesystems, and this
store's write volume/complexity (single small JSON array, no concurrent-writer contention beyond
the existing in-process mutex) doesn't yet justify a database dependency. Revisit if/when this
store needs concurrent multi-process access, partial updates, or query patterns beyond
list/get-by-name/get-by-id.

**Related requirements:** REQ-SPR-170

**Update:** `writeFile` no longer returns an error when
only the post-rename directory fsync fails — it logs a warning (via a `*slog.Logger` now threaded
through `NewFileStore`) and returns `nil`. Once `os.Rename` succeeds, the new data is already
committed and visible to any reader of `f.path`; the directory fsync is defense-in-depth against a
narrow crash window immediately after the rename, not the point at which the write "happens". The
original implementation returned this error like any other write failure, and `service.go`'s
`Register`/`Update`/`RegisterEmbedded` treat any `Save`/`Delete` error as "not persisted", rolling
back in-memory registry/health state to the pre-write state — which would desync the in-memory
registry from the store, since the disk had already moved on to the post-write state. `NewFileStore`
now takes a `*slog.Logger` (matches the `monitor.New`/`health.NewCEPublisher` convention of a
required, non-nil logger dependency) purely to make this warning observable; see `UT-SPR-112`, RED
confirmed via a temporary revert to the old "propagate as error" behavior.

### DD-320: `name`/`service_type` are trimmed once, at the HTTP handler boundary

**Decision:** `Handler.CreateProvider` trims `body.Name`/`body.ServiceType` with `strings.TrimSpace`
immediately after decoding, before validation, and uses the trimmed values both for validation and
for the `RegistrationInput` passed to `ProviderService.Register`. No trimming was added inside
`ProviderService` itself.

**Rationale:** Per the single-point-of-defense principle, the HTTP handler is the trust boundary
for this input — trimming there means every downstream consumer (idempotency lookup by name,
registry slot claims by service type, persisted records) sees an already-normalized value, so no
redundant trimming is needed deeper in the call chain. This fixes a bug where `ValidateName`/
`ValidateServiceType` rejected purely-empty (post-trim) values but never trimmed the value actually
used as a natural key, so `"provider1"` and `"provider1 "` (or a whitespace-padded `service_type`)
registered as distinct entries, bypassing REQ-SPR-080's idempotency and REQ-SPR-200's
single-slot-per-service-type invariants.

**Related requirements:** REQ-SPR-081

### DD-330: File-based config uses env-var-named KEY=VALUE format, no os.Setenv

**Decision:** `config.Load` now supports `AGENT_CONFIG_FILE`, pointing at a minimal `.env`-style
file (`KEY=VALUE` per line, `#` comments, blank lines ignored). Keys in the file are the SAME
environment variable names used by `Config`'s struct tags (e.g. `AGENT_SERVER_ADDRESS`), not the
dotted `config.key` names from the spec's Consolidated Configuration Reference table. Merging is
done via a local `map[string]string` passed to `env.ParseWithOptions(cfg, env.Options{Environment:
...})` — `Load` never calls `os.Setenv`.

**Rationale:** Two deliberate simplifications for a MAY-level requirement (REQ-XC-CFG-010):
(1) reusing the exact env-var names as file keys avoids building and maintaining a separate
dotted-key-to-env-var mapping table that could silently drift out of sync with the struct tags as
new config fields are added; (2) `caarlos0/env`'s `Options.Environment` lets the merged
file+env view be passed in as a plain map rather than mutating the real process environment via
`os.Setenv`, which would leak file-sourced values across repeated `Load()` calls (e.g. into
unrelated test cases that never touch `AGENT_CONFIG_FILE` themselves) — verified by
`UT-XC-CFG-063`, which asserts `os.LookupEnv` does NOT see file-sourced keys after `Load()`
returns. This closes a gap where file-based config was previously "entirely unimplemented — env vars only."

**Related requirements:** REQ-XC-CFG-010

### DD-340: Wire-level Nats-Msg-Id assertion added against a real NATS server

**Decision:** Added `internal/messaging/client_msgid_wire_test.go` (`IT-MSG-160`, `IT-MSG-161`),
which calls `Client.PublishWithMsgID` against the suite's real embedded NATS/JetStream server and
then (a) fetches the published message via a raw JetStream consumer to assert the `Nats-Msg-Id`
header equals the caller's msg ID, and (b) publishes twice with the same msg ID/subject and asserts
the stream's message count stays at 1, proving JetStream's server-side dedup actually triggers. No
production code changed — `PublishWithMsgID` already called `js.Publish(ctx, subject, data,
jetstream.WithMsgID(msgID))` correctly.

**Rationale:** DD-240 documents the `Nats-Msg-Id` design decision, but every existing test exercised
it through `messaging.Client`'s own abstractions (fakes/mocks of the JetStream publish path), never
observing the actual bytes/headers a NATS consumer receives. A refactor that accidentally dropped
the `jetstream.WithMsgID(msgID)` option, or passed the wrong ID, would still pass those tests.
Fetching the message back out via an independent `jetstream.JetStream` connection created directly
from the test (not reusing `Client`'s internals) closes that blind spot. RED confirmed by temporarily
reverting `PublishWithMsgID` to `js.Publish(ctx, subject, data)` (dropping the msg-ID option) —
both tests failed as expected — then reverting back to GREEN.

**Related requirements:** REQ-MSG-135, REQ-XC-CE-050

### DD-350: Restart-drain sequencing moved to a JetStream-readiness callback, `beginConsuming` hardened

**Decision:** Added `messaging.Client.SetOnSetupReady(fn func())`, a callback fired exactly once —
synchronously from within `setupStreamsAndConsume`, right after `c.js`/`c.mainCons`/`c.cancelCons`
are populated for the first time, and before any live consumption begins. `main.go` now constructs
`retry.Processor` before calling `msgClient.Start`, wires `SetOnSetupReady` to run
`retryProcessor.ProcessOnRestart` followed by `msgClient.StartConsuming()`, and removed the old
direct calls to both that previously ran synchronously right after `Start` returned. Separately,
`beginConsuming` was changed to hold `c.mu` for its entire check-then-act sequence (including the
`Consume()` calls) instead of releasing it between the `consuming` guard check and setting the flag.

**Rationale:** `messaging.Client.Start` is explicitly non-blocking (AC-MSG-050: NATS may still be
unreachable when it returns). The composition root previously called
`retryProcessor.ProcessOnRestart` synchronously right after `Start`, assuming JetStream was already
set up — but `Processor.fetchAllFromConsumer` silently returns `(nil, nil)` when `JSProvider()` is
nil, and `ProcessOnRestart` is invoked exactly once at startup with no retry of its own. If NATS
happened to not be connected yet at that exact instant (a scenario AC-MSG-050 explicitly requires
the agent to tolerate, not a rare edge case), restart-drain of the retry/cancel backlog would be
silently and permanently skipped for that startup — undermining the very restart-drain fix
(`ProcessOnRestart` must finish draining before live `Consume()` begins) meant to prevent message
loss/stealing. `SetOnSetupReady` ties the drain-then-consume sequence to the one place JetStream
readiness is actually known, whether that's Start's own synchronous connect attempt or a later
`ReconnectHandler`-driven `doSetup`; `attemptSetup`'s existing `setupDone` single-flight guard
ensures it fires at most once. This also incidentally closes a related concurrency gap: with
`StartConsuming` now called from exactly one place (inside the once-only callback), the previous
theoretical race where two overlapping `StartConsuming`/`doSetup` callers could each start a
duplicate live consume loop on the same durable consumer (`beginConsuming`'s check-then-act
without holding the lock throughout) is unreachable via the production call path — but
`beginConsuming` was hardened directly anyway (rather than relying on "only one caller in
practice") since it remains a public method any future caller could invoke concurrently.
RED confirmed for the concurrency fix via a barrier-synchronized multi-trial test against the
reverted (lock-released) implementation before restoring the fix.

**Related requirements:** REQ-RCM-080, REQ-MSG-100
### DD-360: Registrar panic recovery must not let the goroutine exit permanently

**Decision:** `Registrar.Start` now spawns `runSupervised`, a supervisor loop that calls `run()`
via `runRecovering` (panic-recovering wrapper). If `run()` panics, the panic is recovered and
logged as before, but instead of the goroutine returning (closing `Done()`), the supervisor waits
`registrarPanicRestartDelay` (1s fixed constant) and calls `run()` again — restarting the whole
prerequisite-wait/registration/heartbeat state machine from scratch. The loop only exits for real
when `run()` returns normally, which — given `run()`'s own control flow — only happens on context
cancellation. `IT-DCM-180`'s fake (`panicNTimesLister`) was changed from panicking unconditionally
to panicking only on its first 3 calls, then succeeding, so the test can assert actual forward
progress (a successful registration) rather than merely "the panic didn't crash the process."

**Rationale:** The original panic-recovery fix and its regression test both stopped at "recover
the panic, log it, let the goroutine exit" — `IT-DCM-180` asserted
`Registrar.Done()` closing as its success condition, i.e. it asserted exactly the behavior that's
actually broken. Recovering the panic prevents a process crash, which is real value, but a
permanently-exited registrar goroutine means DCM registration and heartbeating silently stop
forever after a single dependency panic, with no user-visible symptom besides "DCM stops hearing
from this agent" — arguably worse than a crash, since a crash is at least visible (process exit,
supervisor restart) whereas this failure mode is silent. Restarting `run()` after a panic is safe
because DCM registration is idempotent (REQ-DCM-080): even if the agent was already registered and
mid-heartbeat when a panic occurred, restarting from the prerequisite-wait phase just re-registers,
which the control plane treats as an update to the existing agent entry, not a duplicate. RED
confirmed via `IT-DCM-180` against the pre-fix `Start` (single `recover()`, no restart loop): the
new assertion (registration eventually succeeds after 3 panics) timed out as expected, since the
old code's goroutine exited for good after the very first panic.

**Related requirements:** REQ-DCM-070

### DD-370: `main.go`'s wiring order gets its own composition-root-level regression test

**Decision:** Added `cmd/environment-agent/main_m9_test.go`, an integration test that calls the
real `run(ctx)` entry point (not a hand-built partial wiring) against a real embedded
NATS/JetStream test server, with an embedded "widget" SP configured (via
`AGENT_EMBEDDED_SP_WIDGET_HEALTH=unhealthy` and `AGENT_HEALTH_FAILURE_THRESHOLD=1`) to force a
synchronous Ready->Unhealthy transition during `RegisterEmbedded`'s `initialCheck`. A raw NATS
subscriber independently observes `dcm.agents.health` for the resulting health CE — the only
externally-observable side effect of that transition reaching `healthMonitor`'s `onTransition`
callback, since the other two effects wired into the same callback either don't fire for a plain
Unhealthy transition (`registrar.NotifyServiceTypeChange` only reacts to Unavailable-involving
transitions) or aren't independently observable from outside `run()`
(`retryProcessor.RunTransition`).

**Rationale:** `UT-SPR-100`/`UT-SPR-101` in `internal/provider/service` — the existing
regression tests — construct `ProviderService`/
`monitor.Monitor` directly and prove the general ordering property in isolation; they never touch
`main.go`'s `run()` at all. If `main.go`'s actual construction order were reverted to the
pre-fix state (`RegisterEmbedded` before `SetOnTransition`/`SetOnChange`), those unit tests
would keep passing regardless, since they don't exercise the composition root's wiring. RED
confirmed by temporarily swapping the two blocks back to the pre-fix order in `main.go`: the new
test timed out waiting for the health CE (10s), as expected, then passed again once the order was
restored.

**Related requirements:** REQ-SPR-030, REQ-HMN-100, REQ-HMN-120
### DD-380: `RequestErrorHandlerFunc` wired to RFC 7807 output

**Decision:** `main.go`'s `StrictHTTPServerOptions` now sets `RequestErrorHandlerFunc` to
`httperror.WriteInvalidArgument`, alongside the already-present `ResponseErrorHandlerFunc`. Both
test harnesses that independently replicate `main.go`'s strict-handler construction
(`health_integration_test.go`'s and `provider_integration_test.go`'s `startRealServer` helpers)
were updated in the same change so they can't silently drift from production wiring.

**Rationale:** `oapigen.NewStrictHandlerWithOptions` defaults `RequestErrorHandlerFunc` to a
bare `http.Error(w, err.Error(), http.StatusBadRequest)` when unset — plain text,
`Content-Type: text/plain`, not RFC 7807 — for any request whose body fails Go's own
`json.Decode` (as opposed to failing the earlier, more lenient `openapi3filter` schema
validation). This is concretely reachable, not just theoretical: the OpenAPI schema's
`total_node` field is declared as JSON Schema `type: integer`, which accepts `100.0`/`1e2` (both
are mathematically integers), but Go's `encoding/json` rejects either for the generated `*int`
field. A client sending `"total_node": 100.0` would pass `openapi3filter` and then hit the
strict-handler's raw JSON decode, falling into the SDK's non-RFC-7807 default — violating
REQ-HTTP-091 (framework-layer errors MUST be RFC 7807) for a case entirely outside application
code's control. `IT-HTTP-110b` uses `{"name":123}` as a simpler, equivalent proxy for the same
decode-failure code path (`server.gen.go`'s strict-handler body decode), rather than replicating
the exact `total_node` payload — the field triggering the decode failure doesn't change which
code path or wiring is under test.

**Related requirements:** REQ-HTTP-091, REQ-XC-ERR-010, REQ-XC-ERR-020, REQ-SPR-130

### DD-390: Dead code removed — `PanicToErrorBody`/`StatusForType`

**Decision:** Deleted `internal/httperror/panic.go` entirely and removed `StatusForType` from
`internal/httperror/problem.go`, along with their unit tests
(`problem_unit_test.go`'s `StatusForType`/`PanicToErrorBody` `Describe` blocks) and the now-stale
`UT-XC-ERR-030`/`UT-XC-ERR-040` test-plan entries.

**Rationale:** Both were unused: panic recovery in production goes through chi's/the router's own
recovery middleware into `IT-HTTP-080`'s tested path, not through `PanicToErrorBody`, and
`StatusForType` had no callers once that dead path was identified. Confirmed zero remaining
references anywhere in the repo (code, tests, specs, test plans) except a gitignored audit
exploration note. Kept for consistency with this project's practice of giving every audit-finding
disposition — including a plain dead-code removal — a recorded decision, the same way DD-280
records the false-positive disposition.

**Related requirements:** REQ-XC-ERR-010, REQ-XC-ERR-020
### DD-400: Declined — `AGENT_CONFIG_FILE` size/line-count bound (LOW)

**Decision:** No size or line-count limit was added to the `AGENT_CONFIG_FILE` loader
(`config.loadConfigFile` in `internal/config/config.go`).

**Rationale:** `AGENT_CONFIG_FILE`'s path is itself supplied via an environment variable — set by
whoever controls the agent's process environment/container image (the same operator trust
boundary as every other env-var-driven path in this config, e.g. `AGENT_SP_PERSISTENCE_PATH`).
It is not derived from any remote/network-facing input (HTTP request, NATS message, DCM
response), so it sits outside the "single point of defense" trust boundary that validates
untrusted input at this codebase's actual attack surface. An operator who can set
`AGENT_CONFIG_FILE` to point at an enormous file already controls the process's entire
environment and could cause equivalent or worse resource exhaustion through many other means
(e.g. `AGENT_EMBEDDED_SPS` with a huge comma-separated list). Adding a bound here would be
speculative hardening against a threat model this component doesn't have, not a fix for a
reachable issue — declined per this project's "no redundant validation outside actual trust
boundaries" principle. `bufio.Scanner`'s existing 64KiB per-line limit already fails cleanly
(not a crash) on the one dimension (single-line length) that could otherwise interact badly with
downstream parsing.

**Related requirements:** REQ-XC-CFG-010 — this is a disposition of an audit finding against the
existing file-config-loading requirement, not a new requirement.

### DD-410: Retry-subject consumer has no `MaxDeliver` limit (MEDIUM)

**Decision:** The retry-subject JetStream consumer does not set `MaxDeliver`, mirroring the
existing cancel-consumer exemption. `retry.Processor` has no `MaxDeliver`-exceeded termination
guard for the retry topic. `REQ-RCM-150` scopes `MaxDeliver` to the main-subject consumer only.

**Rationale:** `purgeFromRetryTopic` (cancel handling) Naks every non-matching retry-topic
message in place on every cancel, which increments JetStream's delivery count for messages
unrelated to the cancelled resource. A `MaxDeliver` limit on this consumer would let a burst of
cancels for *other* resources push an otherwise-healthy retry-topic message toward premature
termination — a false-positive poison-message classification driven by unrelated traffic, not by
actual SP failures. Not setting `MaxDeliver` on this consumer avoids that risk entirely, since
delivery count no longer gates anything.

**Accepted trade-off:** Retry-topic residency is bounded only by SP health-state transitions
(`REQ-HMN-150`/`REQ-RCM-040`: drained and rejected once the SP reaches `Unavailable`), not by a
delivery-count ceiling. `REQ-HMN-090` means an SP that is reachable and consistently reports
itself unhealthy (`200 OK`, `status: "unhealthy"`) never increments the failure counter that
leads to `Unavailable` — only genuine connectivity failures do (`REQ-HMN-070`). Such an SP can
therefore remain `Unhealthy` indefinitely, and its queued resource requests can sit in the retry
topic indefinitely with no error CloudEvent ever published for them. No time-based
Unhealthy-to-Unavailable escalation was added to close this gap; it is accepted as a known
limitation for v1alpha1.

**Related requirements:** REQ-RCM-150, REQ-HMN-070, REQ-HMN-090, REQ-HMN-150, REQ-RCM-040

### DD-420: `finishSetup` no longer latches `setupDone` on a failed post-callback `beginConsuming` (HIGH)

**Decision:** Extracted the "what happens once streams/consumers exist" branch of
`setupStreamsAndConsume` into a new `messaging.Client.finishSetup` method. In the `DeferConsume`/
`onSetupReady` branch, after the callback runs, `finishSetup` now checks whether `StartConsuming`
(called by the callback) actually succeeded in starting live consumption. If `consumeRequested` is
true but `consuming` is still false — i.e. `beginConsuming`'s `Consume()` call failed — it returns
`false` instead of unconditionally `true`. Added `Client.isConsuming()` and two regression tests in
`client_onsetupready_test.go`: the existing `onSetupReady` test now calls the real `finishSetup`
instead of hand-reimplementing its logic, and a new test
(`TestFinishSetup_DoesNotReportSuccessWhenBeginConsumingFailsAfterCallback`) drives a `Consume()`
failure through the callback and asserts `finishSetup` reports failure, then succeeds on retry
without re-invoking the callback. Also added `IT-MSG-131`, a real-NATS integration test that wires
`SetOnSetupReady` the way `main.go` actually does (drain-equivalent work then `StartConsuming`,
both synchronously inside the callback) — the prior `onSetupReady` coverage never exercised the
real `setupStreamsAndConsume`/`finishSetup` code path at all, only a fake-consumer unit test that
manually replicated the branch.

**Rationale:** `attemptSetup` latches `setupDone = true` whenever `setupStreamsAndConsume` (now
`finishSetup`) returns `true`, and once `setupDone` is true, `attemptSetup` short-circuits on every
future call — no reconnect or background retry will ever call `setupStreamsAndConsume`/
`finishSetup` again. Before this fix, the `onSetupReady` branch always returned `true` right after
invoking the callback, regardless of whether the callback's `StartConsuming()` call actually
started consuming. `StartConsuming` itself discards `beginConsuming`'s error (`_ =
c.beginConsuming()`), by design, since it's meant to be a fire-and-forget "start whenever setup is
ready" latch. If `beginConsuming`'s `Consume()` call failed transiently — plausible right at a
reconnect boundary, since `beginConsuming` now runs synchronously inside `onSetupReady`, itself
called from `setupStreamsAndConsume`, itself invoked from a `ConnectHandler`/`ReconnectHandler` —
the client would silently and permanently strand itself "connected but not consuming" until process
restart: exactly the class of failure the restart-drain fix was introduced to prevent, just one step further down the
same call chain. Confirmed RED (a temporarily-reverted single line: `return true` unconditionally)
against the new regression test before restoring the fix.

**Related requirements:** REQ-RCM-080, REQ-MSG-100

### DD-430: Declined — `registrarPanicRestartDelay` backoff growth/cap (LOW)

**Decision:** No backoff growth or restart-count cap was added to `Registrar.runSupervised`'s
panic-restart loop; `registrarPanicRestartDelay` remains a fixed 1-second delay.

**Rationale:** Two independent reviewers raised the same observation: a
deterministically-panicking `ServiceTypeLister` dependency would produce roughly one
full stack-trace ERROR log per second indefinitely. Both reviewers characterized this as
"resilience/log-flooding polish, not a defect" — `ServiceTypeLister` is `provider.Service`, an
in-process dependency with no remote/attacker-controlled input path that could realistically induce
a *sustained, deterministic* panic (a genuine bug there would need its own fix regardless of restart
cadence). `time.After`-based sleeping has no CPU-spin cost, so the only real downside is log volume
under a "should never happen" condition — the same category of trade-off already declined in
DD-400. Adding exponential backoff-with-reset here would require deciding when to reset the
attempt counter (e.g. "run() survived N seconds without panicking"), which is speculative complexity
for a scenario with no known trigger. Declined per this project's "no redundant complexity against
inputs/conditions outside the actual trust boundary" principle; revisit if a real panicking
dependency is ever found in production.

**Related requirements:** REQ-DCM-070 (per `AC-DCM-055`, which this finding was raised against) —
disposition of an audit finding, not a new requirement.

### DD-440: Test-hygiene fixes — GinkgoT().Setenv and IT-SPR-172 goroutine leak (LOW/MEDIUM)

**Decision:** Two independent test-only fixes:

1. `cmd/environment-agent/main_test.go`'s two `It` blocks (`AGENT_SERVER_ADDRESS`) switched from
   `os.Setenv` + `DeferCleanup(os.Unsetenv, ...)` to `GinkgoT().Setenv`.
2. `cmd/environment-agent/main_m9_test.go`'s `IT-SPR-172` now defers `cancel()` +
   `Eventually(runDone, ...).Should(Receive())` immediately after spawning `run(ctx)`, instead of
   only calling them unconditionally at the end of a passing `It`.

**Rationale:** (1) `os.Setenv`/`os.Unsetenv` mutate real process environment variables directly;
under Ginkgo's `--randomize-all` (which this suite explicitly runs with), a panicking assertion
between `os.Setenv` and its `DeferCleanup` could in principle still leave the variable set
depending on exact panic/recover timing, and `GinkgoT().Setenv` is the standard Ginkgo idiom that
guarantees restoration via the test's own cleanup stack regardless — LOW severity, no observed
actual failure, but a real latent risk under randomized parallel execution. (2) Ginkgo's
`Fail`/`Eventually` timeout panics before reaching code after it
in the same `It` — the previous unconditional `cancel()` call sat at the bottom of the `It`, after
the primary `Eventually(healthCh, ...)` assertion, so a failure of that assertion would skip
`cancel()` entirely and leave the full agent (HTTP listener, health monitor, registrar hammering
`localhost:8080`, messaging reconnect loop) running for the remainder of the test binary's
lifetime.

**Related requirements:** none (test-infrastructure hygiene only, no behavioral requirement)..
