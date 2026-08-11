# Specification: Environment Agent

## 1. Overview

The Environment Agent is a lightweight process that runs in a target environment,
acting as the intermediary between the DCM Control Plane and Service Providers
deployed in that environment. It registers the environment to DCM, consumes
resource operation requests from a messaging system, and routes them to the
appropriate Service Provider.

The agent supports a hybrid SP model: it ships with embedded SP code (enabled via
configuration) and also accepts external ("bring your own") SPs that register via
REST API. Only one SP — embedded or external — may serve a given service type per
agent; duplicate registrations are rejected.

**Initial scope (v1alpha1):**

- Single agent instance per environment (no HA / competing consumers)
- One SP per service type (no multi-SP selection strategies)
- Creation and deletion operations only (no update/day-2 operations)
- External SP authentication deferred (network isolation as interim mitigation)
- No hot-reload of agent configuration (restart required for config changes)
- SP un-registration process not yet designed (periodic re-registration is accepted but no consequences defined for non-renewal in v1alpha1)

**Reference documents:**

- [Environment Agent Enhancement](https://github.com/dcm-project/enhancements/blob/main/enhancements/environment-agent/environment-agent.md)
- [SP Registration Flow](https://github.com/dcm-project/enhancements/blob/main/enhancements/sp-registration-flow/sp-registration-flow.md)
- [SP Health Check](https://github.com/dcm-project/enhancements/blob/main/enhancements/service-provider-health-check/service-provider-health-check.md)
- [SP Status Reporting](https://github.com/dcm-project/enhancements/blob/main/enhancements/state-management/service-provider-status-reporting.md)
- [SP Resource Manager](https://github.com/dcm-project/enhancements/blob/main/enhancements/sp-resource-manager/sp-resource-manager.md)
- OpenAPI Spec: `api/v1alpha1/openapi.yaml` (source of truth for API contract)

---

## 2. Architecture

```
                         +-------------------+
                         |  DCM Control Plane|
                         +--------+----------+
                                  |
                 +----------------+----------------+
                 ^                |                ^
                 |                |                |
          Registration           Heartbeat              Messaging System
   POST /api/v1alpha1/agents  PUT .../heartbeat  (NATS JetStream; CP-owned streams)
                 |                |                |
                 |                |                |
+----------------+----------------+----------------+-----------+
|                     Environment Agent                         |
|                                                               |
|  +-------------+  +------------------+  +-----------------+  |
|  | HTTP Server |--| SP Registration  |--| SP Health       |  |
|  | (REST API)  |  | & Management     |  | Monitor         |  |
|  +------+------+  +--------+---------+  +--------+--------+  |
|         |                  |                      |           |
|  +------+------+  +-------+--------+   +---------+---------+ |
|  | Health Svc  |  | SP Registry    |   | DCM Registration  | |
|  +-------------+  | (persistence)  |   | & Heartbeat       | |
|                   +----------------+   +-------------------+ |
|                                                               |
|  +-----------------------------------------------------------+|
|  | Messaging Integration                                      |
|  |  CP stream: dcm-agent-requests (dcm.agent.>)               |
|  | +-------------+ +-------------+ +---------------+          |
|  | | Main Subject| | Retry Stream| | Cancel Subject|          |
|  | | Consumer    | | (agent-owned)| | Consumer      |          |
|  | +------+------+ +------+------+ +-------+-------+          |
|  |        |                |                |                  |
|  | +------+----------------+----------------+-------+          |
|  | |         Resource Operation Router              |          |
|  | +------------------------------------------------+          |
|  +-----------------------------------------------------------+|
|         |                              |                      |
|  +------+------+              +--------+--------+            |
|  | Embedded SP |              | External SP     |            |
|  | (in-process)|              | (REST client)   |            |
|  +-------------+              +-----------------+            |
+---------------------------------------------------------------+
```

---

## 3. Topic Dependency Graph

| # | Topic                                | Prefix | Depends On |
|---|--------------------------------------|--------|------------|
| 1 | HTTP Server                          | HTTP   | -          |
| 2 | Health Service                       | HLT    | 1          |
| 3 | SP Registration & Management         | SPR    | 1          |
| 4 | Provider Query Endpoints             | STS    | 1, 3       |
| 5 | SP Health Monitoring                 | HMN    | 3          |
| 6 | DCM Registration & Heartbeat         | DCM    | 3, 5       |
| 7 | Messaging System Integration         | MSG    | -          |
| 8 | Resource Operation Routing           | RTE    | 3, 5, 7    |
| 9 | Retry & Cancel Mechanisms            | RCM    | 5, 7, 8    |

```
Topic 1: HTTP Server               (independent)
  |
  +---> Topic 2: Health Service          (depends on 1)
  +---> Topic 3: SP Registration         (depends on 1)
          |
          +---> Topic 4: Provider Query  (depends on 1, 3)
          +---> Topic 5: SP Health Mon.  (depends on 3)
                  |
                  +---> Topic 6: DCM Reg & Heartbeat (depends on 3, 5)

Topic 7: Messaging System           (independent)

Topics 8: Resource Routing           (depends on 3, 5, 7)
  |
  +---> Topic 9: Retry & Cancel          (depends on 5, 7, 8)
```

Topics 1 and 7 can be delivered in parallel. Topics 2, 3 depend on 1.
Topics 4, 5 depend on 3. Topic 6 depends on 3 and 5.
Topic 8 depends on 3, 5, 7. Topic 9 depends on 5, 7, 8.

---

## 4. Topic Specifications

### 4.1 HTTP Server

#### Overview

Foundation layer: HTTP server with graceful shutdown, signal handling,
configuration loading from environment variables, and route registration for all
OpenAPI-defined endpoints. All endpoints are under `/api/v1alpha1`.

Out of scope: TLS termination (handled by infrastructure/ingress),
authentication/authorization middleware, rate limiting.

#### Requirements

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-HTTP-010 | The agent MUST start an HTTP server on the configured address | MUST | |
| REQ-HTTP-020 | The agent MUST register all OpenAPI-defined routes under `/api/v1alpha1`: `/health` (GET), `/providers` (GET, POST), `/providers/{provider_id}` (GET) | MUST | |
| REQ-HTTP-030 | The agent MUST initiate graceful shutdown on SIGTERM: stop new connections, drain in-flight requests within configured timeout, exit with code 0 after in-flight requests complete or drain timeout elapses. The server MUST close connections that remain in-flight after the drain timeout. Clients connected at timeout MAY receive HTTP 503 or a connection reset | MUST | Amended: Go stdlib limitation, see commit b797493 |
| REQ-HTTP-040 | The agent MUST initiate graceful shutdown on SIGINT, behaving identically to REQ-HTTP-030 | MUST | |
| REQ-HTTP-050 | The agent MUST load configuration values from environment variables (see REQ-XC-CFG-010 for file support and precedence rules) | MUST | |
| REQ-HTTP-060 | The agent MUST log each HTTP request at INFO level including method, path, response status code, and duration | MUST | |
| REQ-HTTP-070 | The agent MUST catch panics in HTTP handlers and return an RFC 7807 INTERNAL error response | MUST | |
| REQ-HTTP-071 | Panic recovery middleware SHOULD be applied as the outermost middleware layer | SHOULD | Implementation guidance — observable behavior covered by REQ-HTTP-070 |
| REQ-HTTP-080 | The agent MUST log server lifecycle events: listen address on startup, shutdown initiation, and shutdown completion | MUST | |
| REQ-HTTP-090 | The agent MUST return 400 Bad Request with RFC 7807 error body for malformed requests | MUST | |
| REQ-HTTP-091 | The API framework layer MUST return RFC 7807 error responses for request parsing and response serialization failures | MUST | |
| REQ-HTTP-110 | The agent SHOULD enforce a configurable per-request timeout | SHOULD | |

#### Configuration Introduced

| Config Key | Env Var | Default | Min | Max | Unit | Description |
|------------|---------|---------|-----|-----|------|-------------|
| server.address | AGENT_SERVER_ADDRESS | :8080 | - | - | - | Listen address (host:port) |
| server.shutdownTimeout | AGENT_SERVER_SHUTDOWN_TIMEOUT | 15s | 1s | 5m | duration | Graceful shutdown drain timeout |
| server.requestTimeout | AGENT_SERVER_REQUEST_TIMEOUT | 30s | 1s | 10m | duration | Per-request context timeout |

#### Acceptance Criteria

##### AC-HTTP-010: Server starts on configured address

- **Validates:** REQ-HTTP-010
- **Given** valid configuration is provided
- **When** the agent starts
- **Then** the HTTP server MUST begin listening on the configured address

##### AC-HTTP-020: Route registration

- **Validates:** REQ-HTTP-020
- **Given** the HTTP server has started
- **When** a request is made to any defined endpoint (e.g., `/api/v1alpha1/health`, `/api/v1alpha1/providers`, `/api/v1alpha1/providers/{provider_id}`)
- **Then** the request MUST be routed to the corresponding handler

##### AC-HTTP-030: Graceful shutdown on SIGTERM

- **Validates:** REQ-HTTP-030
- **Given** the HTTP server is running
- **When** SIGTERM is received
- **Then** the server MUST stop accepting new connections
- **And** the server MUST drain in-flight requests within the configured shutdown timeout
- **And** connections still in-flight after the timeout elapses MUST be closed (clients MAY receive HTTP 503 or a connection reset)
- **And** the process MUST exit with code 0

##### AC-HTTP-040: Graceful shutdown on SIGINT

- **Validates:** REQ-HTTP-040
- **Given** the HTTP server is running
- **When** SIGINT is received
- **Then** the server MUST behave identically to REQ-HTTP-030

##### AC-HTTP-050: Configuration from environment variables

- **Validates:** REQ-HTTP-050
- **Given** environment variables are set (e.g., AGENT_SERVER_ADDRESS=:9090)
- **When** the agent starts
- **Then** the agent MUST use the values from the environment variables

##### AC-HTTP-060: Request logging

- **Validates:** REQ-HTTP-060
- **Given** any HTTP request is processed
- **When** the response is sent
- **Then** the agent MUST log at INFO level with method, path, status code, and duration

##### AC-HTTP-070: Panic recovery

- **Validates:** REQ-HTTP-070, REQ-HTTP-071
- **Given** a handler panics during request processing
- **When** the panic is caught
- **Then** the response MUST be HTTP 500 with RFC 7807 body (type=INTERNAL)
- **And** the panic and stack trace MUST be logged at ERROR level

##### AC-HTTP-080: Lifecycle logging

- **Validates:** REQ-HTTP-080
- **Given** the agent starts or stops
- **When** the server begins listening or initiates shutdown
- **Then** the agent MUST log the event including the listen address on startup

##### AC-HTTP-090: Malformed request handling

- **Validates:** REQ-HTTP-090
- **Given** a request with invalid parameters
- **When** the request reaches the router
- **Then** the agent MUST return a 400 Bad Request with an RFC 7807 error body

##### AC-HTTP-091: Framework-layer error responses

- **Validates:** REQ-HTTP-091
- **Given** the API framework layer encounters a request parsing or response serialization failure
- **When** an error response is generated
- **Then** the error response MUST be RFC 7807 with `Content-Type: application/problem+json`

##### AC-HTTP-095: Per-request timeout enforcement

- **Validates:** REQ-HTTP-110
- **Given** a per-request timeout of 1s is configured
- **When** a handler takes longer than 1s
- **Then** the request context MUST be cancelled
- **And** the response MUST be HTTP 503 with RFC 7807 body (type=UNAVAILABLE)

#### Dependencies

None - independently deliverable.

---

### 4.2 Health Service

#### Overview

Implementation of the `/api/v1alpha1/health` endpoint as defined in the OpenAPI
spec. This endpoint reports whether the agent process is alive. DCM does not poll
this endpoint directly (it uses heartbeats for agent liveness); this endpoint
serves infrastructure health checks (e.g., Kubernetes liveness probes).

> **Note:** This endpoint originates from the OpenAPI spec (`api/v1alpha1/openapi.yaml`)
> rather than the enhancement document. The enhancement defines only `/providers`;
> the health endpoint was added for infrastructure compatibility.

Out of scope: Dependency health aggregation (SP health is reported via
`GET /api/v1alpha1/providers`).

#### Requirements

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-HLT-010 | The agent MUST expose `GET /api/v1alpha1/health` and return HTTP 200 OK when the agent process is running | MUST | |
| REQ-HLT-020 | The health response MUST return a JSON body conforming to the Health schema with `status` and `path` fields | MUST | |
| REQ-HLT-030 | The `status` field MUST be `"healthy"` when the agent is operational: the HTTP server is accepting connections AND the messaging system is connected | MUST | |
| REQ-HLT-040 | The `status` field MUST be `"unhealthy"` when the messaging system is disconnected | MUST | |
| REQ-HLT-050 | The `path` field MUST be `"health"` | MUST | |
| REQ-HLT-060 | The response MUST set `Content-Type: application/json` | MUST | |
| REQ-HLT-070 | The health endpoint MUST return within 5ms from in-memory state with no network I/O or disk reads | MUST | |

#### Acceptance Criteria

##### AC-HLT-010: Health endpoint availability

- **Validates:** REQ-HLT-010
- **Given** the HTTP server is running
- **When** a GET request is made to `/api/v1alpha1/health`
- **Then** the agent MUST return HTTP 200 OK

##### AC-HLT-020: Health response body — healthy

- **Validates:** REQ-HLT-020, REQ-HLT-030, REQ-HLT-050
- **Given** the agent is operational and the messaging system is connected
- **When** GET `/api/v1alpha1/health` is called
- **Then** the response body MUST contain:
  - `status`: `"healthy"`
  - `path`: `"health"`

##### AC-HLT-030: Health response body — unhealthy

- **Validates:** REQ-HLT-020, REQ-HLT-040
- **Given** the agent is running but the messaging system is disconnected
- **When** GET `/api/v1alpha1/health` is called
- **Then** the response MUST be HTTP 200 OK
- **And** the response body MUST contain `status`: `"unhealthy"`

##### AC-HLT-040: Content type

- **Validates:** REQ-HLT-060
- **Given** any call to the health endpoint
- **When** the response is returned
- **Then** the `Content-Type` header MUST be `application/json`

##### AC-HLT-050: Health endpoint performs no blocking I/O

- **Validates:** REQ-HLT-070
- **Given** the agent is running but external dependencies (messaging system) are unreachable
- **When** `GET /api/v1alpha1/health` is called
- **Then** the endpoint MUST return a response without blocking or timing out
- **And** the response MUST be derived solely from in-memory state

#### Dependencies

Depends on Topic 1 (HTTP Server).

---

### 4.3 SP Registration & Management

#### Overview

The agent manages a registry of Service Providers. Embedded SPs are registered
at startup via configuration. External SPs register via REST API
(`POST /api/v1alpha1/providers`). Only one SP may serve a given service type.
The registry is persisted to local storage to survive restarts.

Out of scope: Authentication of external SP registrations, multiple SPs per
service type with selection strategies.

#### Requirements — Embedded SP Registration

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-SPR-010 | The agent MUST support configuring embedded SPs via environment variables or configuration file | MUST | |
| REQ-SPR-020 | Each embedded SP MUST be explicitly enabled in configuration before it becomes active | MUST | |
| REQ-SPR-030 | At startup, the agent MUST register only the embedded SPs that are explicitly enabled in configuration | MUST | |
| REQ-SPR-040 | Embedded SP registration MUST be performed in-process without a REST call | MUST | |
| REQ-SPR-050 | If an embedded SP's service type is already occupied by a persisted external SP registration (from a prior session), the embedded SP registration for that service type MUST be skipped | MUST | |
| REQ-SPR-051 | When an embedded SP registration is skipped due to slot conflict, the agent MUST log a warning and continue startup without failing; the skipped SP MUST NOT prevent other SPs from registering or the agent from becoming operational (HTTP server listening, messaging system connection initiated, health checks running) | MUST | |

#### Requirements — External SP Registration

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-SPR-060 | The agent MUST expose `POST /api/v1alpha1/providers` accepting a JSON body conforming to the Provider schema | MUST | |
| REQ-SPR-070 | The Provider schema MUST require: `name`, `endpoint`, `service_type`, `schema_version`. The `display_name` field is OPTIONAL | MUST | |
| REQ-SPR-075 | On re-registration (same `name`), if the `service_type` has changed, the agent MUST validate the new service type is not already occupied by another SP | MUST | |
| REQ-SPR-076 | On re-registration with changed service type, if the new service type is occupied by another SP, the agent MUST reject with 409 Conflict | MUST | |
| REQ-SPR-077 | On re-registration with changed service type, if the new service type is available, the agent MUST update the registration and free the old service type slot | MUST | |
| REQ-SPR-080 | Registration MUST be idempotent using `name` as the natural key (create-or-update behavior) | MUST | |
| REQ-SPR-081 | `name` and `service_type` MUST be trimmed of leading/trailing whitespace before use as idempotency/collision keys, so whitespace-only differences cannot bypass REQ-SPR-080 or REQ-SPR-200 | MUST | |
| REQ-SPR-090 | When a `?id=` query parameter is provided on provider registration, the agent MUST use it as the provider ID | MUST | |
| REQ-SPR-091 | When the `?id=` query parameter is absent on provider registration, the agent MUST generate a provider ID using UUID v4 (lowercase hyphenated format). User-supplied `?id=` values MUST be validated against the AEP-122 pattern: `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$` (1–63 characters) | MUST | |
| REQ-SPR-100 | Successful new registration MUST return 201 Created with the full Provider resource including server-set fields (`id`, `path`, `create_time`, `update_time`) | MUST | |
| REQ-SPR-110 | Successful re-registration MUST return 200 OK with the updated Provider resource | MUST | |
| REQ-SPR-120 | If the requested service type is already served by another SP (embedded or external, identified by a different name), the agent MUST reject the registration with 409 Conflict | MUST | |
| REQ-SPR-121 | The 409 Conflict response MUST include an error message identifying the conflicting provider name and service type | MUST | |
| REQ-SPR-130 | If the request body is malformed or fails validation, the agent MUST return 400 Bad Request with RFC 7807 error body | MUST | |
| REQ-SPR-131 | If the request body passes structural parsing but fails semantic validation (`schema_version` pattern, `endpoint` not a valid URI, `?id=` pattern violation), the agent MUST return 422 Unprocessable Entity with RFC 7807 error body (type=UNPROCESSABLE_ENTITY) | MUST | |

#### Requirements — Persistence

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-SPR-170 | The agent MUST persist SP registrations to local storage so that slot ownership survives restarts | MUST | |
| REQ-SPR-180 | On startup, the agent MUST load persisted registrations before registering embedded SPs or accepting external ones | MUST | |
| REQ-SPR-190 | An external SP registered during a prior session MUST retain its service type slot across agent restarts | MUST | |
| REQ-SPR-181 | If the persistence layer fails to load on startup (corruption, I/O error, schema mismatch), the agent MUST log the error and exit immediately (fail fast) | MUST | |

> **Known limitation (v1alpha1):** When an external SP becomes Unavailable
> (via health monitoring), its registration is retained in local persistent
> storage and continues to block the service-type slot. No automatic slot
> reclamation exists in v1alpha1. Manual intervention (clearing the
> persistence store or restarting with a clean state) is required to reclaim
> the slot. A future version will define SP un-registration or automatic
> slot reclamation after prolonged Unavailability.

#### Requirements — Service Type Uniqueness

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-SPR-200 | Only one SP — embedded or external — may serve a given service type per agent | MUST | |
| REQ-SPR-210 | The first SP to register for a service type claims the slot | MUST | |

> **Note:** Because embedded SPs register at startup before external SPs can
> connect, they effectively take priority on a clean agent state. This is a
> consequence of REQ-SPR-030 and REQ-SPR-210, not a separate requirement.

#### Configuration Introduced

| Config Key | Env Var | Default | Min | Max | Unit | Description |
|------------|---------|---------|-----|-----|------|-------------|
| sp.embedded | AGENT_EMBEDDED_SPS | (empty) | - | - | - | Comma-separated list of enabled embedded SP identifiers |
| sp.persistencePath | AGENT_SP_PERSISTENCE_PATH | /var/lib/environment-agent/registrations | - | - | - | Path for persisting SP registrations |

#### Acceptance Criteria

##### AC-SPR-010: Embedded SP registration at startup

- **Validates:** REQ-SPR-010, REQ-SPR-030, REQ-SPR-040
- **Given** the agent is configured with `AGENT_EMBEDDED_SPS=container,cluster`
- **When** the agent starts
- **Then** the embedded SPs for "container" and "cluster" service types MUST be registered internally
- **And** embedded registration MUST NOT make outbound REST calls

##### AC-SPR-015: Embedded SP initial health transition is not lost

- **Validates:** REQ-SPR-030, REQ-HMN-100, REQ-HMN-120
- **Given** an embedded SP's initial health check (performed synchronously during registration)
  yields a status different from its initial assumed status (e.g. immediately Unhealthy)
- **When** the agent completes startup composition
- **Then** that transition MUST reach the same downstream handling as any later transition —
  retry-topic reprocessing and health CloudEvent publication — not be silently dropped because
  the transition callback was wired after embedded registration ran

##### AC-SPR-020: Embedded SPs not active by default

- **Validates:** REQ-SPR-020
- **Given** `AGENT_EMBEDDED_SPS` is empty or not set
- **When** the agent starts
- **Then** no embedded SPs MUST be registered

##### AC-SPR-030: Embedded SP skipped when slot occupied

- **Validates:** REQ-SPR-050, REQ-SPR-051
- **Given** a persisted external SP registration exists for service type "container"
- **And** the agent is configured with `AGENT_EMBEDDED_SPS=container`
- **When** the agent starts
- **Then** the embedded SP registration for "container" MUST be skipped
- **And** a warning MUST be logged
- **And** the agent MUST continue starting normally

##### AC-SPR-040: External SP registration — success (new)

- **Validates:** REQ-SPR-060, REQ-SPR-100
- **Given** no SP is currently serving service type "database"
- **When** `POST /api/v1alpha1/providers` is called with `{name: "db-provider", endpoint: "https://sp.example.com:8080", service_type: "database", schema_version: "v1alpha1"}`
- **Then** the response MUST be 201 Created
- **And** the response body MUST include server-set fields: `id`, `path`, `create_time`, `update_time`

##### AC-SPR-050: External SP re-registration (idempotent update)

- **Validates:** REQ-SPR-080, REQ-SPR-110
- **Given** a provider named "db-provider" is already registered for service type "database"
- **When** `POST /api/v1alpha1/providers` is called with the same `name` and `service_type`
- **Then** the response MUST be 200 OK
- **And** the `update_time` MUST be refreshed

##### AC-SPR-060: Service type conflict

- **Validates:** REQ-SPR-120, REQ-SPR-121, REQ-SPR-210
- **Given** an embedded SP is serving service type "container"
- **When** an external SP attempts to register for service type "container" with a different name
- **Then** the response MUST be 409 Conflict
- **And** the error message MUST identify the conflicting provider

##### AC-SPR-070: Same SP re-registers for same service type (idempotent)

- **Validates:** REQ-SPR-080
- **Given** "vm-provider" is registered for service type "vm"
- **When** "vm-provider" re-registers for service type "vm"
- **Then** the response MUST be 200 OK (idempotent update, not conflict)

##### AC-SPR-071: Whitespace-padded name/service_type does not bypass idempotency or slot collision

- **Validates:** REQ-SPR-081
- **Given** "ws-provider" is registered for service type "ws-svc"
- **When** a registration is submitted with `name="  ws-provider  "` (leading/trailing whitespace)
  and the same `service_type`
- **Then** the response MUST be 200 OK, treated as an idempotent update of the SAME provider — not
  a new registration
- **And** symmetrically, a whitespace-padded `service_type` matching an already-claimed slot MUST
  still be rejected with 409 Conflict

##### AC-SPR-090: Persistence across restart

- **Validates:** REQ-SPR-170, REQ-SPR-180, REQ-SPR-190
- **Given** an external SP "db-provider" is registered for service type "database"
- **When** the agent restarts
- **Then** "db-provider" MUST still hold the "database" service type slot
- **And** the agent MUST load the persisted registration before processing new registrations

##### AC-SPR-092: Persisted writes are fsync'd before and after rename

- **Validates:** REQ-SPR-170
- **Given** a provider registration is being persisted (`FileStore.Save`/`Delete`)
- **When** the write completes
- **Then** the temp file's contents MUST be fsync'd before the atomic rename into place
- **And** the parent directory MUST be fsync'd after the rename, so the directory-entry update
  itself survives a crash immediately following the write
- **And** a failure to fsync the temp file MUST abort before the rename (the previous persisted
  state remains intact, no partial/torn write is exposed)
- **And** a failure to fsync the parent directory AFTER the rename has already succeeded MUST NOT
  be reported as a `Save`/`Delete` failure — the rename already made the new data durable and
  visible to readers; treating this as a failure would make `service.ProviderService` roll back
  in-memory registry/health state to the pre-write state while the disk still reflects the
  post-write state, desyncing the two (see AC-SPR-111's slot-ownership invariant)

##### AC-SPR-095: Re-registration with changed service type (available)

- **Validates:** REQ-SPR-075, REQ-SPR-077
- **Given** "db-provider" is registered for service type "database"
- **And** no SP is serving service type "analytics"
- **When** "db-provider" re-registers with `service_type="analytics"`
- **Then** the response MUST be 200 OK
- **And** "db-provider" MUST now serve "analytics"
- **And** the "database" slot MUST be freed

##### AC-SPR-096: Re-registration with changed service type (conflict)

- **Validates:** REQ-SPR-075, REQ-SPR-076
- **Given** "db-provider" is registered for service type "database"
- **And** another SP "other-provider" is serving service type "analytics"
- **When** "db-provider" re-registers with `service_type="analytics"`
- **Then** the response MUST be 409 Conflict

##### AC-SPR-100: Invalid registration body

- **Validates:** REQ-SPR-130
- **Given** a POST request with missing required fields (e.g., no `service_type`)
- **When** `POST /api/v1alpha1/providers` is called
- **Then** the response MUST be 400 Bad Request with RFC 7807 error body

##### AC-SPR-105: Provider ID from query parameter

- **Validates:** REQ-SPR-090
- **Given** `POST /api/v1alpha1/providers?id=custom-001`
- **When** the registration succeeds
- **Then** the response `id` MUST equal `"custom-001"`

##### AC-SPR-106: Provider ID auto-generated

- **Validates:** REQ-SPR-091
- **Given** `POST /api/v1alpha1/providers` with no `?id=` parameter
- **When** the registration succeeds
- **Then** the response `id` MUST be a non-empty UUID v4 string

##### AC-SPR-106b: Provider ID AEP-122 pattern validation

- **Validates:** REQ-SPR-091
- **Given** `POST /api/v1alpha1/providers?id=INVALID_ID!` (contains uppercase and special characters)
- **When** the request is processed
- **Then** the response MUST be 422 Unprocessable Entity with RFC 7807 error body (type=UNPROCESSABLE_ENTITY)
- **And** the error MUST identify the `?id=` pattern violation

##### AC-SPR-107: Provider schema_version required

- **Validates:** REQ-SPR-070
- **Given** `POST /api/v1alpha1/providers` with body missing `schema_version`
- **When** the request is processed
- **Then** the response MUST be 400 Bad Request

##### AC-SPR-108: Semantic validation returns 422

- **Validates:** REQ-SPR-131
- **Given** `POST /api/v1alpha1/providers` with a valid JSON body where `schema_version` does not match the pattern `^v[0-9]+(alpha|beta)?[0-9]*$`
- **When** the request is processed
- **Then** the response MUST be 422 Unprocessable Entity with RFC 7807 error body (type=UNPROCESSABLE_ENTITY)

##### AC-SPR-108b: Endpoint URI semantic validation returns 422

- **Validates:** REQ-SPR-131
- **Given** `POST /api/v1alpha1/providers` with a valid JSON body where `endpoint` is not a valid URI (e.g., `"not-a-url"`)
- **When** the request is processed
- **Then** the response MUST be 422 Unprocessable Entity with RFC 7807 error body (type=UNPROCESSABLE_ENTITY)

##### AC-SPR-106c: Cross-provider ID collision rejected with the colliding holder's name

- **Validates:** REQ-SPR-091
- **Given** provider "collision-holder" is already registered with `?id=shared-id`
- **When** a DIFFERENT, new provider name registers with `?id=shared-id`
- **Then** the response MUST be 409 Conflict with RFC 7807 error body (type=CONFLICT)
- **And** the error detail MUST name the requested ID and the existing holder ("collision-holder")

##### AC-SPR-109: Persistence load failure causes fail-fast

- **Validates:** REQ-SPR-181
- **Given** the persistence store is corrupted or unreadable
- **When** the agent starts
- **Then** the agent MUST log the error and exit immediately without starting the HTTP server

##### AC-SPR-110: One SP per service type enforced

- **Validates:** REQ-SPR-200
- **Given** an SP is registered for service type "database"
- **When** a different SP attempts to register for service type "database"
- **Then** the registration MUST be rejected with 409 Conflict

##### AC-SPR-111: Slot mutations verify current ownership before releasing a service type

- **Validates:** REQ-SPR-200
- **Given** the registry's in-memory slot map and the persisted store have desynced (e.g. a stale
  embedded record survives after an external provider has since claimed that service type)
- **When** any operation that releases or moves a service-type slot runs (a provider changing its
  own service type, or stale-embedded cleanup during `RegisterEmbedded`)
- **Then** the slot MUST only be released/reassigned if the current registry holder for that service
  type matches the provider performing the operation
- **And** a different provider's active slot MUST NOT be silently deleted or reassigned as a side
  effect

#### Dependencies

Depends on Topic 1 (HTTP Server).

---

### 4.4 Provider Query Endpoints

#### Overview

The agent exposes two endpoints for querying Service Provider state:

- `GET /api/v1alpha1/providers` — lists all registered SPs (both embedded and
  external) with their current health status.
- `GET /api/v1alpha1/providers/{provider_id}` — returns a single SP by ID with
  its current health status.

Health state fields (`type`, `status`, `last_check_time`) are read-only properties on
the Provider resource itself, eliminating the need for a separate status schema.

These endpoints are always available regardless of deployment mode (Kubernetes,
Docker, standalone).

Out of scope: Historical status data, metrics.

#### Requirements

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-STS-010 | The agent MUST expose `GET /api/v1alpha1/providers` returning all registered SPs with their current health state | MUST | |
| REQ-STS-015 | The list response MUST return a JSON object with a `results` array containing Provider resources | MUST | |
| REQ-STS-020 | The agent MUST expose `GET /api/v1alpha1/providers/{provider_id}` returning a single SP by ID | MUST | |
| REQ-STS-025 | If the requested `provider_id` does not match any registered SP, the agent MUST return 404 Not Found with RFC 7807 error body | MUST | |
| REQ-STS-030 | Each Provider resource MUST include read-only health fields: `type` (embedded/external), `status` (Ready/Unhealthy/Unavailable), `last_check_time` timestamp | MUST | |
| REQ-STS-040 | The list endpoint MUST include all registered SPs regardless of their health state | MUST | |
| REQ-STS-050 | The responses MUST set `Content-Type: application/json` | MUST | |

#### Acceptance Criteria

##### AC-STS-010: List providers with multiple providers

- **Validates:** REQ-STS-010, REQ-STS-015, REQ-STS-030
- **Given** an embedded SP "k8s-container" (container, Ready) and an external SP "db-provider" (database, Unhealthy) are registered
- **When** GET `/api/v1alpha1/providers` is called
- **Then** the response MUST be 200 OK with a `results` array containing both providers
- **And** each entry MUST include `type`, `status`, `last_check_time`

##### AC-STS-020: List providers with no providers

- **Validates:** REQ-STS-010, REQ-STS-040
- **Given** no SPs are registered
- **When** GET `/api/v1alpha1/providers` is called
- **Then** the response MUST be 200 OK with an empty `results` array

##### AC-STS-025: Get provider by ID

- **Validates:** REQ-STS-020, REQ-STS-030
- **Given** an SP "db-provider" is registered with ID "sp-db-001"
- **When** GET `/api/v1alpha1/providers/sp-db-001` is called
- **Then** the response MUST be 200 OK with the full Provider resource
- **And** the response MUST include `type`, `status`, `last_check_time`

##### AC-STS-026: Get provider — not found

- **Validates:** REQ-STS-025
- **Given** no SP is registered with ID "nonexistent"
- **When** GET `/api/v1alpha1/providers/nonexistent` is called
- **Then** the response MUST be 404 Not Found with RFC 7807 error body

##### AC-STS-030: Provider list reflects real-time health

- **Validates:** REQ-STS-030
- **Given** an external SP transitions from Ready to Unhealthy
- **When** GET `/api/v1alpha1/providers` is called after the transition
- **Then** the provider's `status` MUST reflect `"Unhealthy"`

##### AC-STS-035: Content-Type on provider endpoints

- **Validates:** REQ-STS-050
- **Given** any GET request to `/api/v1alpha1/providers` or `/api/v1alpha1/providers/{id}`
- **When** the response is returned
- **Then** `Content-Type` MUST be `application/json`

##### AC-STS-022: List includes all health states

- **Validates:** REQ-STS-040
- **Given** SPs in Ready, Unhealthy, and Unavailable states
- **When** `GET /api/v1alpha1/providers` is called
- **Then** all SPs MUST appear in the `results` array regardless of health state

#### Dependencies

Depends on Topic 1 (HTTP Server) and Topic 3 (SP Registration).

---

### 4.5 SP Health Monitoring

#### Terminology

Three distinct health-related concepts are used throughout this spec:

- **Agent process health**: Whether the agent itself is operational (HTTP server
  accepting connections AND messaging system connected). Used by `GET /health`
  and Kubernetes liveness probes. See §4.2.
- **SP runtime state**: The three-state model (Ready / Unhealthy / Unavailable)
  per registered SP. See requirements below.
- **DCM-advertisable**: A service type is advertisable when backed by an SP in
  Ready or Unhealthy state (i.e., not Unavailable). This is what "healthy" means
  in the DCM registration context (§4.6).

#### Overview

The agent monitors the health of its registered Service Providers using the
three-state health model (Ready, Unhealthy, Unavailable). The monitoring
mechanism differs by SP type: in-process for embedded SPs, endpoint polling for
external SPs.

The agent differentiates behavior based on health state: Unhealthy SPs keep their
service type advertised but stop receiving requests; Unavailable SPs cause service
type removal from DCM.

On Kubernetes/OpenShift deployments, the agent additionally surfaces SP health as
custom pod conditions.

Out of scope: Metrics/observability integration.

#### Requirements — Health Check Mechanism

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-HMN-010 | The agent MUST periodically check the health of each registered external SP by polling its `GET /health` endpoint | MUST | |
| REQ-HMN-020 | The agent MUST check the health of each registered embedded SP in-process without a network call | MUST | |
| REQ-HMN-030 | The health check interval MUST be configurable | MUST | |
| REQ-HMN-040 | The health check MUST have a configurable timeout per SP call | MUST | |

#### Requirements — Three-State Model

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-HMN-050 | SP responding with `200 OK` and `status: "healthy"` (external) or internal check passing (embedded) MUST set state to Ready | MUST | |
| REQ-HMN-051 | Newly registered external SPs MUST receive an immediate health check. The resulting state MUST reflect the check outcome: Ready if healthy, Unhealthy otherwise. On agent restart, previously persisted external SPs MAY defer their first check to the next periodic tick | MUST | |
| REQ-HMN-052 | Newly registered embedded SPs MUST have their in-process health check executed immediately. The initial state is set based on the result: Ready if passing, Unhealthy if reporting unhealthy | MUST | |
| REQ-HMN-054 | When an external SP re-registers with a changed endpoint, its health state MUST reset to Unhealthy and an immediate health check MUST be performed. Non-endpoint field changes MUST NOT affect health state | MUST | |
| REQ-HMN-060 | SP responding with `200 OK` and `status: "unhealthy"` (external) or internal check reporting unhealthy (embedded) MUST set state to Unhealthy | MUST | |
| REQ-HMN-070 | SP not responding (connection refused, DNS failure, TCP timeout, HTTP status other than 200, or response body unparseable) after exceeding a configurable failure threshold MUST set state to Unavailable | MUST | |
| REQ-HMN-080 | A healthy response MUST reset the failure counter for the SP | MUST | |
| REQ-HMN-090 | An unhealthy response MUST NOT increment the failure counter (the SP is reachable but not fully operational) | MUST | |

#### Requirements — Behavior on State Transitions

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-HMN-100 | When an SP becomes Unhealthy, the agent MUST keep the service type in its advertised list to DCM | MUST | |
| REQ-HMN-110 | When an SP becomes Unhealthy, the agent MUST stop routing new requests to that SP | MUST | Requests are queued to the retry topic; see REQ-RTE-090 |
| REQ-HMN-120 | When an SP becomes Unhealthy, the agent MUST publish a `dcm.agent.health.service-type-degraded` CloudEvent to `dcm.agents.health` | MUST | |
| REQ-HMN-130 | When an SP becomes Unavailable, the agent MUST remove the service type from its advertised list | MUST | |
| REQ-HMN-140 | When an SP becomes Unavailable, the agent MUST send a `POST /api/v1alpha1/agents` to DCM with the updated registration (service types without the affected type) | MUST | |
| REQ-HMN-145 | When an SP becomes Unavailable, the agent MUST publish a `dcm.agent.health.service-type-unavailable` CloudEvent to `dcm.agents.health` | MUST | |
| REQ-HMN-150 | When an SP becomes Unavailable, the agent MUST process the retry topic and reject all held requests for that service type with error CloudEvents | MUST | |
| REQ-HMN-170 | When a previously unhealthy or unavailable SP recovers to Ready, the agent MUST process held requests from the retry topic for that service type | MUST | |
| REQ-HMN-180 | When an SP recovers from Unavailable to Ready, the agent MUST re-add the service type to its list and send `POST /api/v1alpha1/agents` to DCM with the updated registration | MUST | |
| REQ-HMN-185 | When an SP transitions from Unavailable to Unhealthy, the agent MUST re-add the service type to its advertised list and send `POST /api/v1alpha1/agents` to DCM with the updated registration. The retry topic MUST NOT be processed on this transition (only on transition to Ready per REQ-HMN-170) | MUST | |

#### Requirements — Pod Conditions (Kubernetes/OpenShift)

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-HMN-190 | On Kubernetes/OpenShift deployments, the agent SHOULD surface SP health as custom pod conditions on its own pod | SHOULD | |
| REQ-HMN-200 | Each registered SP SHOULD be represented as a separate pod condition using the SP's provider ID and service type as the condition type | SHOULD | |
| REQ-HMN-210 | The condition `status` SHOULD be `True` for Ready SPs and `False` for Unhealthy/Unavailable SPs | SHOULD | |
| REQ-HMN-220 | The condition `reason` SHOULD reflect the health state (Ready, Unhealthy, Unavailable) | SHOULD | |
| REQ-HMN-230 | The condition `message` SHOULD include the SP name, service type, and current health state string (e.g., 'SP db-provider (database): Unhealthy') | SHOULD | |
| REQ-HMN-240 | The agent SHOULD use Pod Readiness Gates to surface per-SP health as custom pod conditions | SHOULD | |
| REQ-HMN-250 | Pod condition updates SHOULD use in-cluster authentication to patch the pod's `status.conditions` | SHOULD | |
| REQ-HMN-260 | Pod conditions SHOULD be updated only when a health state changes, not on every health check | SHOULD | |
| REQ-HMN-270 | If the agent cannot update pod conditions (e.g., running outside K8s, missing RBAC), it MUST log a warning and continue all other operations (SP health monitoring, DCM heartbeats, request routing) without interruption | MUST | |

#### Configuration Introduced

| Config Key | Env Var | Default | Min | Max | Unit | Description |
|------------|---------|---------|-----|-----|------|-------------|
| health.checkInterval | AGENT_HEALTH_CHECK_INTERVAL | 10s | 1s | 5m | duration | Interval between health checks for external SPs |
| health.checkTimeout | AGENT_HEALTH_CHECK_TIMEOUT | 5s | 500ms | health.checkInterval | duration | Timeout per health check call |
| health.failureThreshold | AGENT_HEALTH_FAILURE_THRESHOLD | 3 | 1 | 100 | integer | Consecutive failures before marking SP as Unavailable |
| health.podConditionsEnabled | AGENT_POD_CONDITIONS_ENABLED | auto | - | - | - | Enable/disable K8s pod condition updates. `auto` enables when running inside Kubernetes (detected via `KUBERNETES_SERVICE_HOST`), `true` forces on, `false` forces off |

#### Acceptance Criteria

##### AC-HMN-010: External SP health check — Ready

- **Validates:** REQ-HMN-010, REQ-HMN-050
- **Given** an external SP is registered with endpoint "https://sp.example.com:8080"
- **When** the health check polls `GET https://sp.example.com:8080/health`
- **And** the SP responds with `200 OK` and `{status: "healthy"}`
- **Then** the SP MUST be marked as Ready

##### AC-HMN-020: External SP health check — Unhealthy

- **Validates:** REQ-HMN-010, REQ-HMN-060, REQ-HMN-090
- **Given** an external SP is registered
- **When** the health check receives `200 OK` with `{status: "unhealthy"}`
- **Then** the SP MUST be marked as Unhealthy
- **And** the failure counter MUST NOT be incremented

##### AC-HMN-030: External SP health check — Unavailable after threshold

- **Validates:** REQ-HMN-070
- **Given** an external SP is registered and `AGENT_HEALTH_FAILURE_THRESHOLD=3`
- **When** the health check fails 3 consecutive times (timeout or error)
- **Then** the SP MUST be marked as Unavailable

##### AC-HMN-040: Healthy response resets failure counter

- **Validates:** REQ-HMN-080
- **Given** an external SP has 2 consecutive failures
- **When** the next health check receives a healthy response
- **Then** the failure counter MUST be reset to 0

##### AC-HMN-050: Unhealthy SP keeps service type advertised

- **Validates:** REQ-HMN-100, REQ-HMN-110
- **Given** the SP for service type "database" becomes Unhealthy
- **When** the agent evaluates its advertised service types
- **Then** "database" MUST remain in the advertised list
- **And** new requests for "database" MUST NOT be routed to the SP

##### AC-HMN-060: Unavailable SP removes service type

- **Validates:** REQ-HMN-130, REQ-HMN-140
- **Given** the SP for service type "database" becomes Unavailable
- **When** the agent updates DCM
- **Then** the registration payload to DCM MUST NOT include "database"

##### AC-HMN-070: SP recovery from Unavailable

- **Validates:** REQ-HMN-170, REQ-HMN-180
- **Given** the SP for service type "database" was Unavailable and the service type was removed
- **When** the SP recovers to Ready
- **Then** the agent MUST re-add "database" to its list
- **And** MUST send an updated registration to DCM including "database"
- **And** MUST process held requests from the retry topic

##### AC-HMN-080: Health degraded CloudEvent

- **Validates:** REQ-HMN-120, REQ-MSG-150
- **Given** the SP for service type "database" transitions to Unhealthy
- **When** the state change is detected
- **Then** the agent MUST publish a CloudEvent to `dcm.agents.health` with type `dcm.agent.health.service-type-degraded`

##### AC-HMN-085: Health unavailable CloudEvent

- **Validates:** REQ-HMN-145
- **Given** the SP for service type "database" transitions to Unavailable
- **When** the state change is detected
- **Then** the agent MUST publish a CloudEvent to `dcm.agents.health` with type `dcm.agent.health.service-type-unavailable`
- **And** the CE data MUST include `agent_id`, `agent_name`, `topic_name`, `service_type`, `reason`, and `affected_provider`

##### AC-HMN-100: Pod conditions updated on state change

- **Validates:** REQ-HMN-190, REQ-HMN-210, REQ-HMN-260, REQ-HMN-220, REQ-HMN-200, REQ-HMN-230
- **Given** the agent runs on Kubernetes with `AGENT_POD_CONDITIONS_ENABLED=true`
- **And** SP "db-provider" transitions from Ready to Unhealthy
- **When** the state change is detected
- **Then** the pod condition for "db-provider" SHOULD be updated: `status=False`, `reason=Unhealthy`
- **And** the condition type MUST incorporate the provider ID and service type
- **And** the `message` MUST include the SP name, service type, and health state

##### AC-HMN-110: Pod conditions non-fatal when unavailable

- **Validates:** REQ-HMN-270
- **Given** the agent runs outside Kubernetes or lacks RBAC permissions
- **When** a pod condition update fails
- **Then** the agent MUST log a warning
- **And** MUST continue operating normally

##### AC-HMN-005: Configurable health check interval and timeout

- **Validates:** REQ-HMN-030, REQ-HMN-040
- **Given** `AGENT_HEALTH_CHECK_INTERVAL=20s` and `AGENT_HEALTH_CHECK_TIMEOUT=2s`
- **When** health checks run
- **Then** checks MUST occur at the configured interval
- **And** a slow SP exceeding the timeout MUST be treated as a failed check

##### AC-HMN-015: Embedded SP health check in-process

- **Validates:** REQ-HMN-020
- **Given** an embedded SP is registered
- **When** the health check runs
- **Then** it MUST execute in-process without any network call

##### AC-HMN-051: External SP becomes Ready when healthy on registration

- **Validates:** REQ-HMN-051
- **Given** an external SP registers with a reachable, healthy endpoint
- **When** the registration completes
- **Then** the SP state MUST be Ready

##### AC-HMN-051b: External SP stays Unhealthy when unreachable on registration

- **Validates:** REQ-HMN-051
- **Given** an external SP registers with an unreachable endpoint
- **When** the registration completes
- **Then** the SP state MUST be Unhealthy

##### AC-HMN-052: Embedded SP immediate health check — passes

- **Validates:** REQ-HMN-052
- **Given** an embedded SP is registered at startup
- **When** the in-process health check passes
- **Then** the SP state MUST be Ready

##### AC-HMN-053: Embedded SP immediate health check — reports unhealthy

- **Validates:** REQ-HMN-052
- **Given** an embedded SP is registered at startup
- **When** the in-process health check reports unhealthy
- **Then** the SP state MUST be Unhealthy

##### AC-HMN-054: Endpoint change with healthy new endpoint

- **Validates:** REQ-HMN-054
- **Given** an external SP is registered and has reached Ready state
- **When** the SP re-registers with a different, healthy endpoint
- **Then** the SP state MUST be Ready after the re-registration completes
- **And** subsequent health checks MUST poll the new endpoint

##### AC-HMN-054b: Endpoint change with unreachable new endpoint

- **Validates:** REQ-HMN-054
- **Given** an external SP is registered and has reached Ready state
- **When** the SP re-registers with an unreachable endpoint
- **Then** the SP state MUST be Unhealthy after the re-registration completes

##### AC-HMN-055: Non-endpoint update preserves health

- **Validates:** REQ-HMN-054
- **Given** an external SP is registered and has reached Ready state
- **When** the SP re-registers with the same endpoint but different display_name
- **Then** its health state MUST remain Ready

##### AC-HMN-185: Unavailable to Unhealthy re-advertises

- **Validates:** REQ-HMN-185
- **Given** an SP was Unavailable and its service type was removed from DCM
- **When** the SP responds with `status: "unhealthy"`
- **Then** the agent MUST re-add the service type to its advertised list
- **And** MUST send updated registration to DCM

##### AC-HMN-120: Pod Readiness Gates used for conditions

- **Validates:** REQ-HMN-240, REQ-HMN-250
- **Given** the agent runs on Kubernetes with `AGENT_POD_CONDITIONS_ENABLED=true`
- **When** an SP health state changes
- **Then** the agent SHOULD use Pod Readiness Gates to surface the condition
- **And** SHOULD use in-cluster authentication to patch the pod's `status.conditions`

#### Dependencies

Depends on Topic 3 (SP Registration & Management).

---

### 4.6 DCM Registration & Heartbeat

#### Overview

The agent self-registers with the DCM Control Plane once at least one SP is
registered and not Unavailable (see Terminology in §4.5). Registration is
idempotent (name as natural key). After registration, the agent sends periodic
heartbeats including consumer lag.

Out of scope: Agent de-registration on shutdown, HA coordination.

#### Requirements — Agent Registration

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-DCM-010 | The agent MUST register with DCM via `POST /api/v1alpha1/agents` once at least one SP (embedded or external) is registered and not Unavailable | MUST | |
| REQ-DCM-020 | The registration payload MUST include: `name`, `environment`, `service_types`, `cost`, `topic_name`. The `topic_name` value MUST be the prefixed main subject (`dcm.agent.{base}` per REQ-MSG-010). The initial registration MUST include a non-empty `service_types` list. Subsequent updates MAY include an empty list (see REQ-DCM-115) | MUST | |
| REQ-DCM-030 | The registration payload SHOULD include `resources_available` when available. Structure: `{total_cpu: string, total_memory: string (e.g., "1TB"), total_storage: string (e.g., "2TB"), total_node: integer}` — sourced from K8s node info or manual configuration | SHOULD | Aligned with OpenAPI `ResourceCapacity` schema (snake_case) |
| REQ-DCM-040 | Registration MUST execute asynchronously without blocking HTTP server startup | MUST | |
| REQ-DCM-050 | Registration MUST retry with exponential backoff using formula: min(initialInterval × 2^attempt, maxInterval) with full jitter (uniform random in [0, calculated]) on failure | MUST | |
| REQ-DCM-060 | Non-retryable errors (4xx client errors except 429 Too Many Requests) MUST stop retries immediately. On 429, the agent MUST respect the `Retry-After` header if present, or apply standard backoff | MUST | |
| REQ-DCM-070 | Registration failures MUST be logged without causing the agent to exit | MUST | |
| REQ-DCM-080 | Re-registration on restart MUST update the existing agent entry in DCM using `name` as the natural key (idempotent behavior) | MUST | |
| REQ-DCM-090 | After successful registration, the agent MUST store the returned `agent_id` exclusively in memory for use in heartbeats and updates. The `agent_id` is assigned by DCM in the registration response; on restart the agent relies on DCM's idempotent registration to recover it | MUST | |
| REQ-DCM-100 | The agent MUST wait until at least one SP is registered and not Unavailable before registering to DCM (prerequisite gate) | MUST | |
| REQ-DCM-110 | Each service type advertised to DCM MUST be backed by an SP in Ready or Unhealthy state (not Unavailable) | MUST | |
| REQ-DCM-115 | When all SPs become Unavailable after initial registration, the agent MUST send `POST /api/v1alpha1/agents` with an empty `service_types` list | MUST | |

#### Requirements — Service Type Change Notification

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-DCM-120 | When the list of supported service types changes (SP registration or health-driven removal) and the agent is already registered to DCM, the agent MUST send `POST /api/v1alpha1/agents` with the full updated registration payload | MUST | |
| REQ-DCM-130 | If the agent is not yet registered to DCM when the service type list changes, the agent MUST defer — the SP registration satisfies the prerequisite for initial DCM registration | MUST | |

#### Requirements — Heartbeat

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-DCM-140 | After successful registration, the agent MUST send periodic heartbeats to DCM via `PUT /api/v1alpha1/agents/{agentId}/heartbeat` | MUST | |
| REQ-DCM-150 | The heartbeat payload MUST include `timestamp` (ISO 8601) and `consumer_lag` (number of unacknowledged messages on the main topic's durable consumer, excluding retry and cancel topics). `timestamp` MUST be strictly greater than the previous heartbeat's `timestamp` | MUST | Generated fresh via `time.Now()` on every send; DCM rejects a heartbeat whose timestamp isn't strictly increasing (control-plane review item #8) |
| REQ-DCM-160 | The heartbeat interval MUST be configurable | MUST | |
| REQ-DCM-170 | Heartbeat failures MUST be logged and retried on the next interval without causing the agent to exit | MUST | |

#### Configuration Introduced

| Config Key | Env Var | Default | Min | Max | Unit | Description |
|------------|---------|---------|-----|-----|------|-------------|
| agent.name | AGENT_NAME | (required) | - | - | - | Unique agent name |
| agent.environment | AGENT_ENVIRONMENT | (required) | - | - | - | Freeform environment identifier |
| agent.cost | AGENT_COST | (required) | - | - | - | Cost tier: low, medium-low, medium, medium-high, high |
| dcm.registrationUrl | DCM_REGISTRATION_URL | (required) | - | - | - | Base URL of DCM Control Plane API |
| dcm.initialBackoff | DCM_REGISTRATION_INITIAL_BACKOFF | 1s | 100ms | dcm.maxBackoff | duration | Initial retry backoff |
| dcm.maxBackoff | DCM_REGISTRATION_MAX_BACKOFF | 5m | dcm.initialBackoff | 1h | duration | Maximum backoff interval |
| heartbeat.interval | AGENT_HEARTBEAT_INTERVAL | 30s | 5s | 10m | duration | Heartbeat interval |

#### Acceptance Criteria

##### AC-DCM-010: Initial registration after first non-Unavailable SP

- **Validates:** REQ-DCM-010, REQ-DCM-100, REQ-DCM-090
- **Given** the agent starts with one embedded SP configured and not Unavailable
- **When** the SP becomes Ready
- **Then** the agent MUST send `POST /api/v1alpha1/agents` to DCM with the correct payload
- **And** the agent MUST store the returned `agent_id` in memory

##### AC-DCM-020: Registration waits for non-Unavailable SP

- **Validates:** REQ-DCM-100
- **Given** the agent starts with no SPs configured
- **When** no SP is registered and not Unavailable
- **Then** the agent MUST NOT send a registration request to DCM

##### AC-DCM-030: Registration payload correctness

- **Validates:** REQ-DCM-020
- **Given** the agent is configured with `name="agent-prod-1"`, `environment="production"`, `cost="medium"`, `AGENT_TOPIC_NAME="agent-prod-1"`
- **And** service types ["container", "database"] are available
- **When** the agent registers to DCM
- **Then** the payload MUST include all required fields with correct values (including `topic_name="dcm.agent.agent-prod-1"` and `service_types=["container","database"]`)

##### AC-DCM-040: Idempotent re-registration on restart

- **Validates:** REQ-DCM-080
- **Given** the agent was previously registered to DCM with name "agent-prod-1"
- **When** the agent restarts and re-registers with the same name
- **Then** DCM MUST update the existing entry (not create a duplicate)
- **And** the agent MUST receive the same `agent_id`

##### AC-DCM-050: Registration retry with exponential backoff

- **Validates:** REQ-DCM-050, REQ-DCM-070
- **Given** the DCM Control Plane is unreachable
- **When** a registration attempt fails
- **Then** the agent MUST retry with exponential backoff
- **And** MUST continue serving HTTP requests

##### AC-DCM-060: Non-retryable error stops retries

- **Validates:** REQ-DCM-060
- **Given** DCM returns a 4xx status code other than 429
- **When** the agent receives this response
- **Then** retries MUST stop immediately
- **And** the error MUST be logged at ERROR level

##### AC-DCM-061: 429 Too Many Requests respects Retry-After

- **Validates:** REQ-DCM-060
- **Given** DCM returns HTTP 429 with a `Retry-After` header
- **When** the agent receives this response
- **Then** the agent MUST wait at least the duration specified by `Retry-After` before the next attempt
- **And** if no `Retry-After` header is present, the agent MUST apply standard exponential backoff

##### AC-DCM-055: Registrar goroutine panic does not crash the agent, and registration keeps making forward progress

- **Validates:** REQ-DCM-070
- **Given** a dependency injected into the registrar (e.g. `ServiceTypeLister`) panics, whether once
  or repeatedly
- **When** the registrar's background goroutine invokes that dependency
- **Then** each panic MUST be recovered and logged
- **And** the agent process MUST continue running (HTTP server, routing, other background
  goroutines unaffected)
- **And** the registrar goroutine itself MUST NOT permanently exit after a recovered panic —
  registration/heartbeating MUST keep being retried afterward, so DCM registration does not
  silently stall forever for the remainder of the process lifetime just because one dependency
  call panicked once

##### AC-DCM-070: Service type change triggers DCM update

- **Validates:** REQ-DCM-120
- **Given** the agent is registered to DCM with service types ["container"]
- **And** an external SP registers for service type "database"
- **When** the service type list changes to ["container", "database"]
- **Then** the agent MUST send `POST /api/v1alpha1/agents` with the updated list

##### AC-DCM-080: Periodic heartbeat

- **Validates:** REQ-DCM-140, REQ-DCM-150
- **Given** the agent is registered and its DCM-assigned agent ID is `"agent-123"`
- **When** the heartbeat interval elapses
- **Then** the agent MUST send `PUT /api/v1alpha1/agents/agent-123/heartbeat` with `timestamp` and `consumer_lag`

##### AC-DCM-090: Heartbeat includes consumer lag

- **Validates:** REQ-DCM-150
- **Given** the agent's main topic durable consumer has 5 unacknowledged messages
- **When** a heartbeat is sent
- **Then** `consumer_lag` MUST be 5

##### AC-DCM-095: Heartbeat timestamps strictly increase

- **Validates:** REQ-DCM-150
- **Given** the registrar sends multiple heartbeats over time
- **When** the payload `timestamp` of each is compared to the previous one
- **Then** each MUST be strictly greater than the previous (`IT-DCM-135`)

##### AC-DCM-100: All SPs unavailable — empty service_types sent to DCM

- **Validates:** REQ-DCM-115
- **Given** the agent is registered to DCM with service types ["container", "database"]
- **And** both SPs become Unavailable
- **When** the last SP transitions to Unavailable
- **Then** the agent MUST send `POST /api/v1alpha1/agents` with `service_types=[]`
- **And** the agent MUST remain registered (not de-register)

##### AC-DCM-015: Registration non-blocking to HTTP

- **Validates:** REQ-DCM-040
- **Given** DCM is unreachable
- **When** the agent starts
- **Then** the HTTP server MUST be listening before DCM registration completes

##### AC-DCM-025: Pre-registration defers changes

- **Validates:** REQ-DCM-130
- **Given** the agent is not yet registered to DCM
- **When** an SP registers and changes the service type list
- **Then** the agent MUST NOT send a registration update to DCM
- **And** MUST include the change in the initial registration

##### AC-DCM-035: resources_available in registration

- **Validates:** REQ-DCM-030
- **Given** resource availability information is available
- **When** the agent registers to DCM
- **Then** the payload SHOULD include `resources_available`

##### AC-DCM-085: Configurable heartbeat interval

- **Validates:** REQ-DCM-160
- **Given** `AGENT_HEARTBEAT_INTERVAL=60s`
- **When** the agent is registered
- **Then** heartbeats MUST be sent at 60s intervals

##### AC-DCM-095: Heartbeat failure resilience

- **Validates:** REQ-DCM-170
- **Given** a heartbeat request to DCM fails
- **When** the next interval elapses
- **Then** the agent MUST retry the heartbeat
- **And** MUST NOT exit due to heartbeat failure

##### AC-DCM-105: Advertised service types backed by non-Unavailable SPs

- **Validates:** REQ-DCM-110
- **Given** the agent has SPs in Ready, Unhealthy, and Unavailable states
- **When** the agent sends a registration or update to DCM
- **Then** `service_types` MUST include only types backed by SPs in Ready or Unhealthy state
- **And** MUST NOT include types whose SP is Unavailable

#### Dependencies

Depends on Topic 3 (SP Registration) and Topic 5 (SP Health Monitoring).

---

### 4.7 Messaging System Integration

#### Overview

The agent uses **NATS with JetStream** as its messaging system for communication
with DCM. This is consistent with other DCM components (e.g., the Control Plane).

At startup the agent derives three **subjects** from a deterministic **base name**
(`AGENT_TOPIC_NAME` if set, otherwise `AGENT_NAME`):

| Subject role | NATS subject | Stream ownership |
|--------------|--------------|------------------|
| Main (create/delete requests from CP) | `dcm.agent.{base}` | Control-plane stream `dcm-agent-requests` (wildcard `dcm.agent.>`) |
| Cancel (cancel requests from CP) | `dcm.agent.{base}.cancel` | Same CP stream |
| Retry (agent-internal hold queue) | `{base}.retry` | Agent-owned stream `{base}-retry` |

The control plane owns JetStream streams for all CP-facing subjects (`dcm-agent-requests`
binding `dcm.agent.>`, and `dcm-agent-responses` binding `dcm.agents.responses`).
The agent MUST NOT create competing streams on those subjects. It creates **durable
consumers** filtered to its own main and cancel subjects on `dcm-agent-requests`,
and **publishes** response CloudEvents directly to `dcm.agents.responses` (no
response stream creation). The agent still creates and owns its internal retry
stream and a health stream for `dcm.agents.health`.

The prefixed main subject (`dcm.agent.{base}`) is advertised to DCM as `topic_name`
during registration. The unprefixed retry subject is never CP-facing.

> **Terminology mapping:** This spec uses "topic" as a logical abstraction.
> In NATS JetStream terms: a *topic* maps to a JetStream **subject**. CP-facing
> subjects share the control-plane-owned `dcm-agent-requests` stream; the agent
> consumes via durable **consumers** with `FilterSubject`. Agent-internal retry
> uses a dedicated agent-owned stream.

Out of scope: Creating or administering control-plane JetStream streams. The agent
MUST tolerate the CP not having created `dcm-agent-requests` yet at startup (see
REQ-MSG-051).

#### Requirements — Topic Management

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-MSG-010 | The agent MUST derive a deterministic **base name** at startup: the value of `AGENT_TOPIC_NAME` if set, otherwise `AGENT_NAME`. The base name MUST conform to NATS subject token rules (alphanumeric, hyphens, dots, underscores; no spaces or wildcards; max 255 characters) and MUST NOT already start with the reserved `dcm.agent.` prefix. The control-plane-facing **main subject** MUST be `dcm.agent.{base}` | MUST | Prefix `dcm.agent.` is a static constant, not user input; rejecting a pre-prefixed base avoids double-prefixing and prevents the agent-owned retry subject from landing inside the CP's `dcm.agent.>` wildcard |
| REQ-MSG-020 | The agent MUST create an agent-owned retry stream at startup backing subject `{base}.retry` (stream name `{base}-retry`) | MUST | Never published to by the control plane |
| REQ-MSG-030 | The agent MUST derive the control-plane-facing **cancel subject** as `dcm.agent.{base}.cancel` | MUST | Part of the `dcm.agent.>` wildcard; consumed via durable consumer on `dcm-agent-requests`, not an agent-owned stream |
| REQ-MSG-040 | If agent-owned streams or CP-stream durable consumers already exist, the agent MUST reuse them (`CreateOrUpdateStream` / `CreateOrUpdateConsumer`) | MUST | |
| REQ-MSG-045 | The agent MUST create durable JetStream consumers on first startup | MUST | Main and cancel on `dcm-agent-requests`; retry on agent-owned retry stream |
| REQ-MSG-046 | The agent MUST reuse (bind to) existing JetStream consumers on subsequent startups | MUST | |
| REQ-MSG-047 | JetStream consumer names MUST be deterministic (derived from the base name: `{base}-consumer`, `{base}-cancel-consumer`, `{base}-retry-consumer`) to ensure resumption from the last acknowledged message position | MUST | |
| REQ-MSG-048 | The agent MUST NOT create JetStream streams for control-plane-owned subjects (`dcm.agent.>` or `dcm.agents.responses`) | MUST | CP owns `dcm-agent-requests` and `dcm-agent-responses`; overlapping bindings are rejected by NATS |
| REQ-MSG-049 | The agent MUST create durable consumers on the control-plane stream `dcm-agent-requests` with `FilterSubject` set to its main subject (`dcm.agent.{base}`) and cancel subject (`dcm.agent.{base}.cancel`) respectively | MUST | One consumer per subject; both bound to the same CP stream |
| REQ-MSG-050 | Only the prefixed main subject (`dcm.agent.{base}`) is advertised to DCM as `topic_name` during registration. The retry subject is agent-internal. DCM publishes cancel requests to `dcm.agent.{base}.cancel` | MUST | |
| REQ-MSG-051 | When creating durable consumers on `dcm-agent-requests`, if the stream does not exist yet (control plane not started), the agent MUST retry indefinitely in two phases: (1) an initial bounded attempt retrying every 2s for up to 30s, synchronous with agent startup; (2) if phase 1 fails, a background retry of the full setup attempt every 30s, indefinitely, until it succeeds or the agent shuts down. The agent MUST NOT permanently give up while running | MUST | The CP may take longer than 30s to create its stream (e.g. rolling restarts); giving up permanently would leave the agent silently consuming nothing. Logs WARN for the expected "stream not found" case, ERROR for any other consumer-creation failure (both still retried) |

#### Requirements — Message Consumption

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-MSG-060 | The agent MUST maintain an active JetStream consumer subscription on the main topic, processing messages as they arrive | MUST | |
| REQ-MSG-070 | The agent MUST maintain an active JetStream consumer subscription on the cancel topic to keep the deny list current | MUST | |
| REQ-MSG-080 | The agent MUST use durable JetStream consumers for the main, retry, and cancel topics to ensure no messages are lost across agent restarts | MUST | |
| REQ-MSG-090 | On startup, the agent MUST drain the cancel topic (consuming messages until the consumer reports no pending messages or a configurable drain timeout of 5s elapses) to populate the deny list before processing main and retry topics | MUST | |
| REQ-MSG-100 | The agent MUST handle messaging system connection failures: log at WARN level, reconnect with exponential backoff (same formula as REQ-DCM-050), and continue operating. Messaging unavailability MUST NOT crash the agent | MUST | |
| REQ-MSG-110 | Messaging system availability MUST NOT block agent HTTP server startup | MUST | |
| REQ-MSG-115 | The agent MUST acknowledge a message only after the routing outcome is finalized — the request has been forwarded to the SP and the SP's HTTP response received, the message has been published to the retry topic, or the message has been resolved without forwarding (error CloudEvent published or deny list drop). For the main-topic consume path, `MaxDeliver`-exceeded handling and per-message handler-timeout enforcement MUST occur in the messaging client before invoking the router handler (see REQ-RCM-160, REQ-RCM-180) | MUST | |
| REQ-MSG-116 | Unacknowledged messages MUST be redelivered by JetStream on reconnection | MUST | |

#### Requirements — CloudEvent Format

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-MSG-120 | See REQ-XC-CE-010. All messages exchanged through the messaging system MUST conform to CloudEvents v1.0 | MUST | |
| REQ-MSG-130 | All agent-originated CloudEvents MUST include `agent_name` and `topic_name` in the data payload for correlation (`topic_name` is the prefixed main subject, e.g. `dcm.agent.{base}`) | MUST | AEP snake_case wire format |
| REQ-MSG-135 | Agent response and health CloudEvents MUST be published via JetStream with a `Nats-Msg-Id` header set to the CloudEvent's own `id`, enabling server-side deduplication if publish is retried | MUST | See REQ-XC-CE-050 |
| REQ-MSG-140 | Agent response CloudEvents MUST be published to the `dcm.agents.responses` subject without creating a response stream (JetStream routes to the CP-owned `dcm-agent-responses` stream) | MUST | |
| REQ-MSG-150 | Agent health warning CloudEvents MUST be published to the `dcm.agents.health` subject | MUST | Agent creates `dcm-health` stream for this subject |

#### Configuration Introduced

| Config Key | Env Var | Default | Min | Max | Unit | Description |
|------------|---------|---------|-----|-----|------|-------------|
| messaging.url | AGENT_MESSAGING_URL | (required) | - | - | - | Messaging system URL |
| messaging.topicName | AGENT_TOPIC_NAME | (derived from AGENT_NAME) | - | - | - | Unprefixed base name; main subject is `dcm.agent.{base}` |
| messaging.ackWait | AGENT_MESSAGING_ACK_WAIT | 120s | 10s | 5m | duration | Main-topic durable consumer AckWait |
| messaging.cancelAckWait | AGENT_MESSAGING_CANCEL_ACK_WAIT | 10s | 1s | 1m | duration | Cancel-topic durable consumer AckWait |
| messaging.reconnectInitialBackoff | AGENT_MESSAGING_RECONNECT_INITIAL_BACKOFF | 1s | 100ms | messaging.reconnectMaxBackoff | duration | Initial NATS reconnect backoff (REQ-MSG-100) |
| messaging.reconnectMaxBackoff | AGENT_MESSAGING_RECONNECT_MAX_BACKOFF | 30s | messaging.reconnectInitialBackoff | 5m | duration | Maximum NATS reconnect backoff (REQ-MSG-100) |

#### Acceptance Criteria

##### AC-MSG-010: Subject and stream setup at startup

- **Validates:** REQ-MSG-010, REQ-MSG-020, REQ-MSG-030, REQ-MSG-048, REQ-MSG-049
- **Given** the agent is configured with `AGENT_TOPIC_NAME="agent-prod-1"`
- **And** the control plane has created stream `dcm-agent-requests` binding `dcm.agent.>`
- **When** the agent starts
- **Then** it MUST NOT create a stream on `dcm.agent.agent-prod-1` or `dcm.agents.responses`
- **And** it MUST create durable consumers on `dcm-agent-requests` filtered to `dcm.agent.agent-prod-1` and `dcm.agent.agent-prod-1.cancel`
- **And** it MUST create (or reuse) agent-owned retry stream `agent-prod-1-retry` backing subject `agent-prod-1.retry`

##### AC-MSG-020: Stream and consumer reuse on restart

- **Validates:** REQ-MSG-040, REQ-MSG-046
- **Given** durable consumers and the retry stream already exist from a prior agent session
- **When** the agent restarts
- **Then** the agent MUST bind to the existing consumers and retry stream without error

##### AC-MSG-030: Main subject consumption

- **Validates:** REQ-MSG-060
- **Given** DCM publishes a CloudEvent to subject `dcm.agent.agent-prod-1`
- **When** the agent is running
- **Then** the agent MUST consume the message

##### AC-MSG-040: Cancel topic drained first on startup

- **Validates:** REQ-MSG-090
- **Given** the cancel topic has pending cancel messages and the main topic has creation requests
- **When** the agent starts
- **Then** cancel messages MUST be processed first (populating the deny list)
- **And** main topic messages MUST be processed after the deny list is populated
- **And** a transient error from an individual drain `Fetch` call MUST NOT abort the drain early —
  the drain MUST continue retrying until either it observes zero pending messages or the
  configured drain timeout (5s) elapses, whichever comes first

##### AC-MSG-050: Messaging system failure handling

- **Validates:** REQ-MSG-100, REQ-MSG-110
- **Given** the messaging system is unreachable at startup
- **When** the agent starts
- **Then** the HTTP server MUST start normally
- **And** the agent MUST retry messaging connection in the background

##### AC-MSG-052: NATS reconnect uses exponential backoff with jitter

- **Validates:** REQ-MSG-100
- **Given** the NATS connection is lost after a successful initial connection
- **When** the client computes successive reconnect delays
- **Then** each delay MUST follow `min(initialInterval × 2^attempt, maxInterval)` with full jitter
  (uniform random in `[0, calculated]`) — the same formula as REQ-DCM-050 — rather than a fixed
  interval
- **And** `initialInterval`/`maxInterval` MUST be configurable (`AGENT_MESSAGING_RECONNECT_INITIAL_BACKOFF`,
  `AGENT_MESSAGING_RECONNECT_MAX_BACKOFF`)

##### AC-MSG-060: CloudEvent format compliance

- **Validates:** REQ-MSG-120, REQ-MSG-130, REQ-MSG-140
- **Given** the agent publishes a response CloudEvent
- **When** the event is constructed
- **Then** it MUST conform to CloudEvents v1.0
- **And** the data payload MUST include `agent_name` and `topic_name`
- **And** the CE MUST be published to subject `dcm.agents.responses`

##### AC-MSG-065: Nats-Msg-Id header set on the wire for dedup

- **Validates:** REQ-MSG-135, REQ-XC-CE-050
- **Given** the agent publishes an agent-originated CloudEvent (response or health) via `PublishWithMsgID`
- **When** a raw NATS/JetStream consumer inspects the message as it appears on the wire
- **Then** the message MUST carry a `Nats-Msg-Id` header equal to the CloudEvent's own `id`
- **And** a second publish reusing the same `Nats-Msg-Id` within the stream's duplicate window MUST be
  dropped by JetStream's server-side dedup (observable as no increase in the stream's message count)

##### AC-MSG-012: Request-stream consumer creation retries when CP stream absent

- **Validates:** REQ-MSG-051
- **Given** the agent starts before the control plane has created `dcm-agent-requests`
- **When** the agent attempts to create its main durable consumer
- **Then** it MUST retry every 2s with WARN logs for up to 30s (phase 1)
- **And**, if the stream still doesn't exist after 30s, it MUST keep retrying the full setup every 30s in the background indefinitely (phase 2), rather than giving up
- **And** once the stream appears (in either phase) it MUST successfully create the consumer and begin consuming, with no further manual intervention required

##### AC-MSG-015: Durable consumer creation on first start

- **Validates:** REQ-MSG-045, REQ-MSG-047
- **Given** the agent starts for the first time (no existing consumers)
- **When** the messaging system connection is established
- **Then** the agent MUST create durable JetStream consumers named `agent-prod-1-consumer`, `agent-prod-1-cancel-consumer`, and `agent-prod-1-retry-consumer` (for base `agent-prod-1`)

##### AC-MSG-016: Consumer reuse on restart

- **Validates:** REQ-MSG-046
- **Given** durable consumers exist from a prior session
- **When** the agent restarts
- **Then** the agent MUST bind to the existing consumers without creating new ones

##### AC-MSG-018: Durable consumers survive crash

- **Validates:** REQ-MSG-080, REQ-MSG-116, REQ-RCM-070
- **Given** the agent crashes after consuming but before acknowledging a message
- **When** the agent restarts
- **Then** the unacknowledged message MUST be redelivered

##### AC-MSG-025: Only prefixed main subject advertised to DCM

- **Validates:** REQ-MSG-050
- **Given** the agent registers to DCM with base name `agent-prod-1`
- **When** the registration payload is sent
- **Then** `topic_name` MUST be `dcm.agent.agent-prod-1` (not the unprefixed base, retry subject, or cancel subject)

##### AC-MSG-035: Continuous cancel consumption

- **Validates:** REQ-MSG-070, REQ-RCM-120
- **Given** the agent is running and the cancel topic receives a new cancel CE
- **When** the CE is consumed
- **Then** the deny list MUST be updated with the cancelled resource ID

##### AC-MSG-055: Ack only after routing finalized

- **Validates:** REQ-MSG-115
- **Given** a message is consumed from the main topic
- **When** the routing outcome is finalized (SP response received, retry topic published, or deny list drop)
- **Then** and only then MUST the message be acknowledged to JetStream

#### Dependencies

None - independently deliverable (topic management is an infrastructure concern).

---

### 4.8 Resource Operation Routing

#### Overview

The agent consumes resource operation requests (creation, deletion) from the
messaging system, validates the requested service type, checks SP health, and
routes the request to the appropriate SP. For embedded SPs, routing is an
in-process call. For external SPs, routing is a REST call to the SP's endpoint.

Out of scope: Update/day-2 operations, multi-SP selection strategies.

#### Requirements — Request Validation

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RTE-010 | The agent MUST validate that the requested service type in a creation/deletion CloudEvent is supported by a registered SP | MUST | |
| REQ-RTE-020 | If the service type is not supported, the agent MUST publish an error CloudEvent (`dcm.agent.error`) to `dcm.agents.responses` | MUST | |

#### Requirements — Request Routing (SP Ready)

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RTE-030 | When the SP for the requested service type is Ready and embedded, the agent MUST forward the request via an in-process call | MUST | |
| REQ-RTE-040 | When the SP for the requested service type is Ready and external, the agent MUST forward creation requests via `POST {endpoint}` where `{endpoint}` is the URL provided during SP registration | MUST | |
| REQ-RTE-050 | When the SP for the requested service type is Ready and external, the agent MUST forward deletion requests via `DELETE {endpoint}/{resource_id}` | MUST | |
| REQ-RTE-060 | On successful SP response (creation accepted), the agent MUST publish a `dcm.agent.creation-acknowledged` CloudEvent to `dcm.agents.responses` with `{resource_id, agent_name, topic_name, status: "PROVISIONING"}` | MUST | |
| REQ-RTE-070 | On successful SP response (deletion accepted), the agent MUST publish a `dcm.agent.deletion-acknowledged` CloudEvent to `dcm.agents.responses` with `{resource_id, agent_name, topic_name, status: "DELETING"}` | MUST | |
| REQ-RTE-080 | On SP error response, the agent MUST publish a `dcm.agent.error` CloudEvent to `dcm.agents.responses` with `{resource_id, agent_name, topic_name, error, details}` | MUST | |

#### Requirements — Request Routing (SP Unhealthy)

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RTE-090 | When the SP for the requested service type is Unhealthy, the agent MUST publish the original request CloudEvent to the retry subject (`{base}.retry`) | MUST | |
| REQ-RTE-100 | When a request is held in the retry topic, the agent MUST publish a `dcm.agent.request-queued` CloudEvent to `dcm.agents.responses` with `{resource_id, agent_name, topic_name, service_type, status: "QUEUED"}` | MUST | |

#### Requirements — Request Routing (SP Unavailable)

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RTE-105 | When the SP for the requested service type is Unavailable, the agent MUST immediately reject the request by publishing a `dcm.agent.error` CloudEvent to `dcm.agents.responses` indicating the SP is unavailable | MUST | |

#### Requirements — Retry Policy

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RTE-110 | When the agent forwards a request to an SP and receives a retryable error (5XX status codes, HTTP 429 Too Many Requests, connection failures, or timeouts), the agent MUST apply a configurable retry policy | MUST | |
| REQ-RTE-111 | When the agent forwards a request to an SP and receives a non-retryable error (4XX status codes other than 429), the agent MUST report it immediately as an error CloudEvent without retry | MUST | |
| REQ-RTE-120 | When retries are exhausted, the agent MUST publish an error CloudEvent with the resource ID | MUST | |
| REQ-RTE-130 | The retry policy (max attempts via `routing.retryMaxAttempts`, backoff using formula: min(`routing.retryBackoff` × 2^attempt, `routing.retryMaxBackoff`) with full jitter) MUST be configurable | MUST | |
| REQ-RTE-131 | The retry policy in REQ-RTE-110/130 applies to immediate in-line retries for transient SP errors. This is distinct from the retry topic mechanism (Topic 9), which holds requests when an SP's health state is Unhealthy. If all immediate retries are exhausted, the agent MUST publish an error CloudEvent (REQ-RTE-120); the request MUST NOT fall back to the retry topic | MUST | |

#### Requirements — Deny List (Cancel Filtering)

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RTE-140 | The agent MUST maintain an in-memory deny list of `resource_id` values from consumed cancel CloudEvents | MUST | |
| REQ-RTE-150 | When processing creation requests from the main topic, the agent MUST check each `resource_id` against the deny list | MUST | |
| REQ-RTE-160 | If a `resource_id` matches the deny list, the agent MUST drop the creation request without forwarding it to the SP | MUST | |
| REQ-RTE-170 | If a cancel CloudEvent arrives for a `resource_id` that is already queued in the retry topic, the agent MUST immediately consume the retry topic, remove the matching message, re-publish any non-matching messages, and publish a `dcm.agent.cancel-acknowledged` CloudEvent to `dcm.agents.responses`. The cancelled resource ID MUST also be added to the deny list | MUST | |
| REQ-RTE-180 | If a cancel CloudEvent arrives for a `resource_id` that has already been dispatched to the SP (request in-flight or SP response received), the agent MUST reject the cancellation by publishing a `dcm.agent.cancel-rejected` CloudEvent to `dcm.agents.responses` with `{resource_id, agent_name, topic_name, reason}`. The rejected cancel MUST NOT be added to the deny list | MUST | |
| REQ-RTE-190 | Deny list entries MUST be removed once consumed (used to filter a creation request from the main topic). If the deny list exceeds a configurable maximum size, the oldest entries MUST be evicted (LRU). On restart, the deny list is rebuilt from the cancel topic | MUST | |

#### Configuration Introduced

| Config Key | Env Var | Default | Min | Max | Unit | Description |
|------------|---------|---------|-----|-----|------|-------------|
| routing.retryMaxAttempts | AGENT_ROUTING_RETRY_MAX | 3 | 0 | 20 | integer | Max retry attempts when SP returns error |
| routing.retryBackoff | AGENT_ROUTING_RETRY_BACKOFF | 2s | 100ms | routing.retryMaxBackoff | duration | Initial backoff between retries |
| routing.retryMaxBackoff | AGENT_ROUTING_RETRY_MAX_BACKOFF | 30s | routing.retryBackoff | 5m | duration | Maximum backoff interval (caps exponential growth) |
| routing.denyListMaxSize | AGENT_DENY_LIST_MAX_SIZE | 100000 | 1000 | 10000000 | integer | Maximum deny list entries before LRU eviction |

#### Acceptance Criteria

##### AC-RTE-010: Route creation request to Ready embedded SP

- **Validates:** REQ-RTE-010, REQ-RTE-030, REQ-RTE-060
- **Given** an embedded SP is registered and Ready for service type "container"
- **When** the agent consumes a `dcm.request.create` CloudEvent with `service_type="container"`
- **Then** the agent MUST forward the request via in-process call
- **And** on success, publish a `dcm.agent.creation-acknowledged` CloudEvent

##### AC-RTE-020: Route creation request to Ready external SP

- **Validates:** REQ-RTE-010, REQ-RTE-040, REQ-RTE-060
- **Given** an external SP at "https://sp.example.com:8080" is registered and Ready for service type "database"
- **When** the agent consumes a `dcm.request.create` CloudEvent with `service_type="database"`
- **Then** the agent MUST send `POST https://sp.example.com:8080` with the spec
- **And** on success, publish a `dcm.agent.creation-acknowledged` CloudEvent

##### AC-RTE-030: Route deletion request to external SP

- **Validates:** REQ-RTE-050, REQ-RTE-070
- **Given** an external SP is registered and Ready for service type "database"
- **When** the agent consumes a `dcm.request.delete` CloudEvent with `resource_id="res-123"` and `service_type="database"`
- **Then** the agent MUST send `DELETE https://sp.example.com:8080/res-123`
- **And** on success, publish a `dcm.agent.deletion-acknowledged` CloudEvent

##### AC-RTE-040: Unsupported service type

- **Validates:** REQ-RTE-020
- **Given** no SP is registered for service type "storage"
- **When** the agent consumes a creation request with `service_type="storage"`
- **Then** the agent MUST publish a `dcm.agent.error` CloudEvent with `{resource_id, agent_name, topic_name, error: "UNSUPPORTED_SERVICE_TYPE", details}` to `dcm.agents.responses`

##### AC-RTE-045: SP is Unavailable — request rejected immediately

- **Validates:** REQ-RTE-105
- **Given** the SP for "database" is Unavailable (registered but not reachable)
- **When** the agent consumes a creation request with `service_type="database"`
- **Then** the agent MUST publish a `dcm.agent.error` CloudEvent indicating the SP is unavailable
- **And** the request MUST NOT be queued in the retry topic

##### AC-RTE-050: SP is Unhealthy — request queued

- **Validates:** REQ-RTE-090, REQ-RTE-100
- **Given** the SP for "database" is Unhealthy
- **When** the agent consumes a creation request with `service_type="database"`
- **Then** the original CloudEvent MUST be published to the retry topic
- **And** a `dcm.agent.request-queued` CloudEvent MUST be published to `dcm.agents.responses`

##### AC-RTE-060: SP returns retryable error — retry policy applied

- **Validates:** REQ-RTE-110, REQ-RTE-120
- **Given** the SP for "container" is Ready but returns a 503 Service Unavailable on the creation call
- **When** retries are exhausted (e.g., 3 attempts)
- **Then** the agent MUST publish a `dcm.agent.error` CloudEvent with the resource ID

##### AC-RTE-065: SP returns non-retryable error — immediate failure

- **Validates:** REQ-RTE-111, REQ-RTE-080
- **Given** the SP for "container" is Ready but returns a 400 Bad Request on the creation call
- **When** the error response is received
- **Then** the agent MUST immediately publish a `dcm.agent.error` CloudEvent with the resource ID
- **And** the agent MUST NOT retry the request

##### AC-RTE-070: Deny list filters cancelled request

- **Validates:** REQ-RTE-140, REQ-RTE-150, REQ-RTE-160
- **Given** the deny list contains `resource_id="res-456"`
- **When** the agent processes a creation request for `resource_id="res-456"` from the main topic
- **Then** the request MUST be dropped without forwarding to the SP

##### AC-RTE-080: Cancel for request in retry topic

- **Validates:** REQ-RTE-170
- **Given** a creation request for `resource_id="res-789"` is held in the retry topic
- **And** a cancel CloudEvent arrives for `resource_id="res-789"`
- **When** the cancel is consumed from the cancel topic
- **Then** the agent MUST immediately consume the retry topic
- **And** the message for `resource_id="res-789"` MUST be removed and acknowledged without forwarding to the SP
- **And** any non-matching messages MUST be re-published to the retry topic
- **And** a `dcm.agent.cancel-acknowledged` CloudEvent MUST be published to `dcm.agents.responses`
- **And** `resource_id="res-789"` MUST be added to the deny list

##### AC-RTE-090: Cancel rejected for in-flight provisioning

- **Validates:** REQ-RTE-180
- **Given** the agent already forwarded a creation request for `resource_id="res-101"` and received a success response from the SP
- **When** a cancel CloudEvent arrives for `resource_id="res-101"`
- **Then** the agent MUST publish a `dcm.agent.cancel-rejected` CloudEvent

##### AC-RTE-075: Deny list LRU eviction

- **Validates:** REQ-RTE-190
- **Given** the deny list is at its configured maximum size (`AGENT_DENY_LIST_MAX_SIZE=1000`)
- **And** a new cancel CloudEvent arrives for a previously unseen `resource_id`
- **When** the deny list entry is added
- **Then** the oldest entry MUST be evicted to make room

##### AC-RTE-076: Deny list consume-on-use

- **Validates:** REQ-RTE-190
- **Given** the deny list contains `resource_id="res-456"`
- **When** a creation request for `resource_id="res-456"` is consumed from the main topic and filtered
- **Then** the deny list entry for `"res-456"` MUST be removed
- **And** a subsequent creation request for `resource_id="res-456"` MUST NOT be filtered
- **And** the new entry MUST be present in the deny list

##### AC-RTE-055: Configurable retry policy

- **Validates:** REQ-RTE-130, REQ-RTE-131
- **Given** `AGENT_ROUTING_RETRY_MAX=5` and `AGENT_ROUTING_RETRY_BACKOFF=1s`
- **When** an SP returns a retryable error
- **Then** the agent MUST retry up to 5 times with backoff starting at 1s
- **And** if all retries fail, MUST publish an error CE (not fall back to retry topic)

#### Dependencies

Depends on Topic 3 (SP Registration), Topic 5 (SP Health Monitoring), and
Topic 7 (Messaging System Integration).

---

### 4.9 Retry & Cancel Mechanisms

#### Overview

The retry subject (`{base}.retry`) holds requests when SPs are unhealthy.
Consumption is event-driven (triggered by SP health state transitions to Ready or
Unavailable, or by agent restart — not periodic). The cancel subject
(`dcm.agent.{base}.cancel`) provides a mechanism for DCM to signal that a request
has been re-routed.

The `retry.Processor` handles retry-subject reprocessing (`ProcessOnTransition`
and `ProcessOnRestart`), including its own `MaxDeliver`-exceeded termination
guard mirroring the main-subject one (REQ-RCM-165). Main-subject consumption,
cancel-subject consumption, main-path `MaxDeliver`-exceeded termination, and
main-path handler timeouts are owned by `messaging.Client` (see REQ-MSG-115,
REQ-RCM-160, REQ-RCM-180).

Out of scope: Message TTL/expiry (handled by messaging system configuration).
`MaxDeliver` (REQ-RCM-150) is a distinct mechanism from the out-of-scope
TTL/expiry item above: it bounds the number of delivery *attempts* for a
message, not elapsed time.

#### Requirements — Retry Topic

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RCM-010 | The retry topic MUST hold original CloudEvent bytes published without wrapping, transformation, or additional envelope | MUST | |
| REQ-RCM-020 | Consumption of the retry topic MUST be event-driven — triggered by SP health state transitions (Ready per REQ-RCM-030, Unavailable per REQ-RCM-040) or agent restart (per REQ-RCM-080), not by periodic timers. The health check interval (REQ-HMN-030) serves as the natural rate-limiter for retry topic processing | MUST | |
| REQ-RCM-030 | When an SP transitions to Ready, the agent MUST consume the retry topic and process requests whose service type has a Ready SP | MUST | |
| REQ-RCM-040 | When an SP transitions to Unavailable, the agent MUST consume the retry topic and reject requests whose service type's SP is Unavailable with error CloudEvents to DCM | MUST | |
| REQ-RCM-050 | Messages for service types whose SP is still Unhealthy MUST be re-published to the retry topic without triggering additional `dcm.agent.request-queued` CloudEvents (the initial CloudEvent is sent only on first routing per REQ-RTE-100) | MUST | |
| REQ-RCM-060 | Requests MUST be processed in arrival order per service type. Requests for different service types are independent | MUST | |
| REQ-RCM-070 | The retry topic MUST use the same durable consumer pattern as other topics (per REQ-MSG-080), ensuring messages survive agent crashes | MUST | See REQ-MSG-080 |
| REQ-RCM-080 | On restart, the agent MUST re-read both the main topic and the retry topic, treating messages from the retry topic that remain queued (SP still Unhealthy) as re-publications per REQ-RCM-050 without triggering additional `request-queued` CloudEvents | MUST | |

#### Requirements — Creation/Deletion Dedup

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RCM-090 | If both a creation request and a deletion request for the same resource ID are present in the retry topic, both messages MUST be removed (they cancel out) | MUST | |
| REQ-RCM-100 | The agent MUST log the cancellation when dedup occurs | MUST | |
| REQ-RCM-110 | The agent MUST acknowledge the deletion to DCM when dedup occurs. The creation request is silently dropped since it was never started | MUST | |

#### Requirements — Cancel Topic

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RCM-120 | See REQ-MSG-070. The cancel topic MUST have an active JetStream consumer subscription | MUST | |
| REQ-RCM-130 | See REQ-MSG-090. The cancel topic MUST be drained on startup (until no pending messages or drain timeout elapses) before main/retry processing | MUST | |
| REQ-RCM-140 | Cancel CloudEvents MUST contain `resource_id` and `service_type` in their data payload | MUST | |

#### Requirements — Delivery Safety Net (Poison Messages)

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RCM-150 | JetStream consumers for the main and retry subjects MUST configure a `MaxDeliver` limit, bounding the number of delivery attempts for a message before it is treated as a poison message. The cancel consumer MUST NOT set `MaxDeliver` (cancels must not be dropped by delivery-count exhaustion) | MUST | `MaxDeliver` counts delivery attempts, not elapsed time — distinct from the out-of-scope TTL/expiry mechanism |
| REQ-RCM-160 | When a main-subject message's delivery attempts reach the configured `MaxDeliver` without the routing outcome being finalized (per REQ-MSG-115), the messaging client MUST publish a `dcm.agent.error` CloudEvent to `dcm.agents.responses` with `error: "MAX_DELIVERY_EXCEEDED"`, then terminate the message so it is not redelivered again | MUST | Enforced in `messaging.Client.handleMainMessage` before router invocation |
| REQ-RCM-165 | When a retry-subject message's delivery attempts reach the configured `MaxDeliver` without the routing outcome being finalized, `retry.Processor` MUST publish a `dcm.agent.error` CloudEvent to `dcm.agents.responses` with `error: "MAX_DELIVERY_EXCEEDED"`, then terminate the message so it is not redelivered again — mirroring REQ-RCM-160 for the retry topic, since a retry-subject message can be redelivered just as indefinitely as a main-subject one if its target SP never recovers | MUST | Enforced in `retry.Processor.terminalOnMaxDeliver`, called from `parseMessages` before item processing; see DD-410 |
| REQ-RCM-170 | The `MaxDeliver` limit MUST be configurable | MUST | |

#### Requirements — Handler Processing Deadline

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RCM-180 | The agent MUST bound the processing time of each consumed message with a configurable per-message handler deadline, covering forwarding to the SP and awaiting its response. For main-subject messages, enforcement MUST occur in the messaging client before invoking the router; for retry-subject messages, enforcement MUST occur in the retry processor | MUST | Prevents a hung SP call from blocking the consumer indefinitely |
| REQ-RCM-190 | If the handler deadline elapses before the routing outcome is finalized, the agent MUST cancel the in-flight SP call, treat the outcome as a retryable error (per REQ-RTE-110), and MUST NOT acknowledge the message | MUST | Main path: messaging client NAK; retry path: retry processor NAK/re-publish |
| REQ-RCM-200 | The handler deadline MUST be configurable | MUST | |

#### Requirements — SP-Side Idempotency (Event ID Forwarding)

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-RCM-210 | When forwarding a creation or deletion request to an external SP (per REQ-RTE-040, REQ-RTE-050), the agent MUST set an `Idempotency-Key` HTTP header on the request equal to the inbound CloudEvent's `id` attribute | MUST | Enables SP-side event-level dedup; per DD-190, SPs remain responsible for enforcing idempotency — the agent's obligation is limited to forwarding the key |
| REQ-RCM-220 | The `Idempotency-Key` value forwarded for a given inbound CloudEvent MUST be identical across every delivery attempt of that event — in-line retries (REQ-RTE-110), retry-topic reprocessing (REQ-RCM-030), and JetStream redelivery (REQ-MSG-116) | MUST | Without a stable key, an SP cannot distinguish "same event, retried" from "new event", defeating the purpose of event-level dedup |
| REQ-RCM-230 | For embedded SPs, the agent MUST make the inbound CloudEvent's `id` available to the in-process call (e.g., via request context or a typed request field) | MUST | Keeps idempotency semantics consistent between embedded and external SPs; embedded SPs are not exempt from REQ-RCM-220 |

#### Configuration — Topic 9

| Config Key | Env Var | Default | Min | Max | Unit | Description |
|------------|---------|---------|-----|-----|------|-------------|
| messaging.maxDeliver | AGENT_MESSAGING_MAX_DELIVER | 10 | 1 | 100 | integer | Maximum JetStream delivery attempts for a message before terminal error CE and termination (REQ-RCM-150) |
| routing.handlerTimeout | AGENT_ROUTING_HANDLER_TIMEOUT | 60s | 1s | 10m | duration | Maximum time allowed to process a single consumed message, including the SP call (REQ-RCM-180) |

#### Acceptance Criteria

##### AC-RCM-010: SP recovers — retry topic processed

- **Validates:** REQ-RCM-020, REQ-RCM-030, REQ-RCM-010
- **Given** the retry topic contains 3 requests for service type "database"
- **And** the SP for "database" transitions from Unhealthy to Ready
- **When** the health state change is detected
- **Then** the agent MUST consume the retry topic
- **And** MUST forward the 3 requests to the SP
- **And** the original CE bytes MUST be published without wrapping

##### AC-RCM-020: SP becomes Unavailable — retry topic drained

- **Validates:** REQ-RCM-040, REQ-HMN-150
- **Given** the retry topic contains 2 requests for service type "database"
- **And** the SP for "database" transitions to Unavailable
- **When** the health state change is detected
- **Then** the agent MUST reject both requests with error CloudEvents to DCM

##### AC-RCM-030: Mixed service types in retry topic

- **Validates:** REQ-RCM-050
- **Given** the retry topic contains requests for "database" (Unhealthy) and "container" (Unavailable)
- **When** the agent processes the retry topic
- **Then** "container" requests MUST be rejected (Unavailable)
- **And** "database" requests MUST be re-published to the retry topic (still Unhealthy)
- **And** re-publication MUST NOT trigger additional `dcm.agent.request-queued` CloudEvents

##### AC-RCM-040: FIFO ordering per service type

- **Validates:** REQ-RCM-060
- **Given** the retry topic contains requests A, B, C for service type "database" in that order
- **When** the SP recovers to Ready
- **Then** requests MUST be forwarded to the SP in order: A, then B, then C

##### AC-RCM-050: Creation-deletion dedup

- **Validates:** REQ-RCM-090, REQ-RCM-100, REQ-RCM-110
- **Given** the retry topic contains a creation request for `resource_id="res-123"` and a deletion request for `resource_id="res-123"`
- **When** the retry topic is processed
- **Then** both messages MUST be removed
- **And** the deletion acknowledgment MUST be published as a `dcm.agent.deletion-acknowledged` CloudEvent with `status: "DELETED"`
- **And** the cancellation MUST be logged

##### AC-RCM-060: Cancel topic drained at startup

- **Validates:** REQ-RCM-130, REQ-RTE-190, REQ-RCM-140
- **Given** the cancel topic has messages for `resource_id="res-456"` and `resource_id="res-789"`
- **When** the agent starts
- **Then** the deny list MUST contain both resource IDs before main topic processing begins
- **And** each cancel CE MUST contain `resource_id` and `service_type`

##### AC-RCM-045: Restart re-reads topics without extra CEs

- **Validates:** REQ-RCM-080
- **Given** the retry topic contains messages from a prior session
- **When** the agent restarts and re-reads the retry topic
- **Then** messages for still-Unhealthy SPs MUST be re-published without triggering additional `request-queued` CloudEvents

##### AC-RCM-047: Restart-drain is not skipped when messaging isn't immediately ready

- **Validates:** REQ-RCM-080, REQ-MSG-100
- **Given** the messaging system is unreachable at the instant the agent calls
  `messaging.Client.Start` (per AC-MSG-050 — `Start` is non-blocking and returns
  before NATS/JetStream is confirmed ready)
- **When** NATS/JetStream subsequently becomes reachable, whether moments later or
  after a longer outage
- **Then** the agent MUST still perform the restart-drain of the main/retry/cancel
  durable consumers (per REQ-RCM-080) before beginning live main/cancel consumption
- **And** this MUST NOT depend on the restart-drain call happening to run only once,
  synchronously, immediately after `Start` returns — restart-drain has no retry of
  its own, so triggering it too early (before JetStream is ready) must not silently
  skip it permanently

##### AC-RCM-070: MaxDeliver exceeded — terminal error CE and termination

- **Validates:** REQ-RCM-150, REQ-RCM-160
- **Given** `AGENT_MESSAGING_MAX_DELIVER=5` and a creation request for `resource_id="res-999"` that repeatedly fails to reach a finalized routing outcome (e.g. the handler crashes before acking)
- **When** the message has been delivered 5 times without acknowledgment
- **Then** the agent MUST publish a `dcm.agent.error` CloudEvent for `resource_id="res-999"` with `error: "MAX_DELIVERY_EXCEEDED"`
- **And** the message MUST be acknowledged/terminated so a 6th delivery does not occur

##### AC-RCM-071: MaxDeliver exceeded on the retry topic — terminal error CE and termination

- **Validates:** REQ-RCM-150, REQ-RCM-165
- **Given** `AGENT_MESSAGING_MAX_DELIVER=N` and a retry-subject message for some `resource_id` whose target SP never becomes healthy again, so the message is never Ack'd
- **When** the message has been delivered `N` times on the retry topic without acknowledgment
- **Then** `retry.Processor` MUST publish a `dcm.agent.error` CloudEvent for that `resource_id` with `error: "MAX_DELIVERY_EXCEEDED"`, mirroring AC-RCM-070 for the retry topic
- **And** the message MUST be terminated so a further delivery does not occur

##### AC-RCM-080: MaxDeliver is configurable

- **Validates:** REQ-RCM-170
- **Given** `AGENT_MESSAGING_MAX_DELIVER=10`
- **When** the agent creates the main and retry topic durable consumers at startup
- **Then** both consumers MUST be configured with a maximum of 10 delivery attempts

##### AC-RCM-090: Handler deadline aborts a hung SP call

- **Validates:** REQ-RCM-180, REQ-RCM-190
- **Given** `AGENT_ROUTING_HANDLER_TIMEOUT=30s`
- **And** an external SP accepts the connection but never responds to a forwarded creation request
- **When** 30s elapses without a response
- **Then** the agent MUST cancel the in-flight call
- **And** MUST treat the outcome as a retryable error per REQ-RTE-110
- **And** MUST NOT acknowledge the message

##### AC-RCM-100: Handler deadline is configurable

- **Validates:** REQ-RCM-200
- **Given** `AGENT_ROUTING_HANDLER_TIMEOUT=45s`
- **When** the agent begins processing a message consumed from the main topic
- **Then** the context passed to the SP call MUST carry a deadline no later than 45s from consumption

##### AC-RCM-110: Idempotency-Key forwarded to external SP

- **Validates:** REQ-RCM-210
- **Given** an inbound `dcm.request.create` CloudEvent with `id="evt-abc-123"` and `resource_id="res-1"`
- **When** the agent forwards the request via `POST {endpoint}`
- **Then** the HTTP request MUST include the header `Idempotency-Key: evt-abc-123`

##### AC-RCM-120: Idempotency-Key stable across redelivery and retries

- **Validates:** REQ-RCM-220
- **Given** an inbound CloudEvent with `id="evt-abc-123"` initially triggers a retryable SP error
- **When** the agent retries the forward in-line per REQ-RTE-110, and separately when the same message is later redelivered by JetStream or reprocessed from the retry topic
- **Then** every attempt MUST forward the same `Idempotency-Key: evt-abc-123` header

##### AC-RCM-130: Idempotency-Key available to embedded SPs

- **Validates:** REQ-RCM-230
- **Given** an embedded SP is registered and Ready for service type "container"
- **And** an inbound CloudEvent with `id="evt-xyz-789"` requests creation of a "container" resource
- **When** the agent forwards the request via in-process call
- **Then** the embedded SP call MUST receive `"evt-xyz-789"` as the CE id

#### Dependencies

Depends on Topic 5 (SP Health Monitoring), Topic 7 (Messaging System
Integration), and Topic 8 (Resource Operation Routing).

---

## 5. Cross-Cutting Concerns

### 5.1 Error Handling

#### Requirements

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-XC-ERR-010 | All HTTP error responses MUST conform to RFC 7807 (Problem Details for HTTP APIs) using the Error schema defined in the OpenAPI spec | MUST | |
| REQ-XC-ERR-020 | Error responses MUST set `Content-Type: application/problem+json` | MUST | |
| REQ-XC-ERR-030 | Error responses SHOULD include `detail` and `instance` fields. The `instance` field SHOULD be the request URI | SHOULD | |
| REQ-XC-ERR-040 | Error responses for INTERNAL errors MUST NOT include stack traces, file paths, internal error messages, or hostnames | MUST | |

**Error type mapping:**

> Error `type` values are short token enum strings (e.g., `INVALID_ARGUMENT`, `CONFLICT`), not URIs.

| Error Condition | HTTP Status | Error Type |
|-----------------|-------------|------------|
| Invalid request body | 400 | INVALID_ARGUMENT |
| Unauthorized request | 401 | UNAUTHORIZED |
| Resource not found | 404 | NOT_FOUND |
| Service type conflict | 409 | CONFLICT |
| Validation failure | 422 | UNPROCESSABLE_ENTITY |
| Unexpected error | 500 | INTERNAL |
| Service unavailable | 503 | UNAVAILABLE |

#### Acceptance Criteria

##### AC-XC-ERR-010: RFC 7807 compliance

- **Validates:** REQ-XC-ERR-010, REQ-XC-ERR-030
- **Given** any error condition in the API
- **When** an error response is returned
- **Then** the body MUST conform to the RFC 7807 Error schema with at minimum `type` and `title` fields
- **And** SHOULD include `detail` and `instance` (= request URI) fields

##### AC-XC-ERR-020: Error content type

- **Validates:** REQ-XC-ERR-020
- **Given** any error response
- **When** the response is sent
- **Then** the `Content-Type` header MUST be `application/problem+json`

##### AC-XC-ERR-030: No implementation detail leakage

- **Validates:** REQ-XC-ERR-040
- **Given** an internal error occurs
- **When** the error response is returned
- **Then** the `detail` field MUST contain a static, non-revealing message (e.g., `"An unexpected internal error occurred"`)
- **And** the response MUST NOT contain stack traces, file paths, internal error messages, or hostnames

### 5.2 CloudEvent Definitions

#### Requirements

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-XC-CE-010 | All messages exchanged through the messaging system MUST use CloudEvents v1.0 specification | MUST | |
| REQ-XC-CE-020 | All agent-originated CloudEvents MUST include: `id` (unique), `source`, `type`, `specversion`, and `time` (RFC 3339 timestamp) | MUST | |
| REQ-XC-CE-030 | The `source` attribute for agent-originated CloudEvents MUST be `dcm/agents/{agentName}` (see DD-200; not the DCM-assigned agent ID) | MUST | |
| REQ-XC-CE-040 | The `specversion` MUST be `"1.0"` | MUST | |
| REQ-XC-CE-050 | Agent-originated CloudEvents published to JetStream (responses and health) MUST set the JetStream `Nats-Msg-Id` header to the CloudEvent's own `id` attribute for server-side deduplication | MUST | See REQ-MSG-135 |

**CloudEvent message definitions:**

| Message | `type` | `source` | NATS destination | `data` |
|---------|--------|----------|------------------|--------|
| Creation Request (inbound) | `dcm.request.create` | `dcm/control-plane` | `dcm.agent.{base}` | `{resource_id, service_type, spec}` |
| Deletion Request (inbound) | `dcm.request.delete` | `dcm/control-plane` | `dcm.agent.{base}` | `{resource_id, service_type}` |
| Cancel Request (inbound) | `dcm.request.cancel` | `dcm/control-plane` | `dcm.agent.{base}.cancel` | `{resource_id, service_type}` |
| Creation Acknowledged | `dcm.agent.creation-acknowledged` | `dcm/agents/{agentName}` | `dcm.agents.responses` | `{resource_id, agent_name, topic_name, status: "PROVISIONING"}` |
| Deletion Acknowledged | `dcm.agent.deletion-acknowledged` | `dcm/agents/{agentName}` | `dcm.agents.responses` | `{resource_id, agent_name, topic_name, status: "DELETING"}`. When dedup occurs (create+delete both in retry topic, per REQ-RCM-110), use `status: "DELETED"` (terminal). Normal SP-accepted deletes use `status: "DELETING"` |
| Cancel Acknowledged | `dcm.agent.cancel-acknowledged` | `dcm/agents/{agentName}` | `dcm.agents.responses` | `{resource_id, agent_name, topic_name, service_type}` |
| Cancel Rejected | `dcm.agent.cancel-rejected` | `dcm/agents/{agentName}` | `dcm.agents.responses` | `{resource_id, agent_name, topic_name, reason}` |
| Request Queued | `dcm.agent.request-queued` | `dcm/agents/{agentName}` | `dcm.agents.responses` | `{resource_id, agent_name, topic_name, service_type, status: "QUEUED"}` |
| Error | `dcm.agent.error` | `dcm/agents/{agentName}` | `dcm.agents.responses` | `{resource_id, agent_name, topic_name, error, details}` |
| Health Degraded | `dcm.agent.health.service-type-degraded` | `dcm/agents/{agentName}` | `dcm.agents.health` | `{agent_id, agent_name, topic_name, service_type, reason, affected_provider}` — `agent_id` uses `agent_name` until DCM registration completes (DD-200) |
| Health Unavailable | `dcm.agent.health.service-type-unavailable` | `dcm/agents/{agentName}` | `dcm.agents.health` | `{agent_id, agent_name, topic_name, service_type, reason, affected_provider}` — same `agent_id` convention as Health Degraded |

#### Acceptance Criteria

##### AC-XC-CE-010: CloudEvent v1.0 compliance

- **Validates:** REQ-XC-CE-010, REQ-XC-CE-020, REQ-XC-CE-040
- **Given** the agent publishes any CloudEvent
- **When** the event is constructed
- **Then** it MUST include `id` (unique), `source`, `type`, `time` (RFC 3339 timestamp), and `specversion="1.0"`

##### AC-XC-CE-020: Source attribute format

- **Validates:** REQ-XC-CE-030
- **Given** the agent is configured with `AGENT_NAME="agent-prod-1"`
- **When** a CloudEvent is published
- **Then** the `source` attribute MUST be `"dcm/agents/agent-prod-1"`

### 5.3 Logging

#### Requirements

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-XC-LOG-010 | Structured logging MUST be used in JSON format with required fields: time (RFC 3339), level, msg. Additional fields (caller, error, component) are OPTIONAL | MUST | |
| REQ-XC-LOG-020 | Log levels MUST follow the defined convention: ERROR (unrecoverable failures), WARN (recoverable issues), INFO (lifecycle events), DEBUG (detailed data) | MUST | |

**Log level convention:**

| Level | Usage |
|-------|-------|
| ERROR | Unrecoverable failures, messaging system errors, DCM registration failures (non-retryable) |
| WARN | Recoverable issues, registration retries, SP health transitions, pod condition update failures |
| INFO | Lifecycle events, SP registration/deregistration, health state changes, request routing |
| DEBUG | Detailed request/response data, health check results, CloudEvent payloads |

#### Acceptance Criteria

##### AC-XC-LOG-010: Structured logging

- **Validates:** REQ-XC-LOG-010
- **Given** any operation occurs in the agent
- **When** the operation is logged
- **Then** the log output MUST be a JSON object containing at minimum the fields `time` (RFC 3339), `level`, and `msg`

##### AC-XC-LOG-020: Log levels follow convention

- **Validates:** REQ-XC-LOG-020
- **Given** an unrecoverable failure occurs
- **When** the event is logged
- **Then** it MUST use ERROR level
- **And** recoverable issues MUST use WARN
- **And** lifecycle events MUST use INFO

### 5.4 Configuration Management

#### Requirements

| ID | Requirement | Priority | Notes |
|----|-------------|----------|-------|
| REQ-XC-CFG-010 | All configuration MUST be loadable from environment variables. Configuration MAY additionally be loaded from a configuration file. When both sources provide a value, environment variables MUST take precedence over file-based configuration | MUST | |
| REQ-XC-CFG-020 | The agent MUST fail fast on startup when required configuration values are absent or empty | MUST | |
| REQ-XC-CFG-030 | Required configuration values: AGENT_NAME, AGENT_ENVIRONMENT, AGENT_COST, DCM_REGISTRATION_URL, AGENT_MESSAGING_URL | MUST | |
| REQ-XC-CFG-040 | AGENT_COST MUST be one of: `low`, `medium-low`, `medium`, `medium-high`, `high` | MUST | |
| REQ-XC-CFG-041 | Invalid AGENT_COST values MUST cause the agent to fail fast on startup | MUST | |
| REQ-XC-CFG-050 | Configuration values outside the defined valid ranges MUST cause the agent to fail fast on startup with an error identifying the invalid value and its valid range | MUST | |

#### Acceptance Criteria

##### AC-XC-CFG-010: Environment variable configuration

- **Validates:** REQ-XC-CFG-010
- **Given** any configuration value
- **When** the corresponding environment variable is set
- **Then** the agent MUST use the value from the environment variable

##### AC-XC-CFG-011: Configuration file loading

- **Validates:** REQ-XC-CFG-010
- **Given** `AGENT_CONFIG_FILE` points at a `.env`-style file (one `KEY=VALUE` pair per line, using
  the same environment variable names as the corresponding config struct tags, e.g.
  `AGENT_SERVER_ADDRESS=:9090`) providing a value
- **And** the corresponding environment variable is not set
- **When** the agent starts
- **Then** the agent MUST use the value from the configuration file

##### AC-XC-CFG-012: Environment variable takes precedence over file

- **Validates:** REQ-XC-CFG-010
- **Given** the config file (referenced by `AGENT_CONFIG_FILE`) provides `AGENT_SERVER_ADDRESS=:9090`
- **And** the environment variable `AGENT_SERVER_ADDRESS=:7070` is also set
- **When** the agent starts
- **Then** the agent MUST use `:7070` (environment variable wins)

##### AC-XC-CFG-013: Config file format details

- **Validates:** REQ-XC-CFG-010
- **Given** a config file referenced by `AGENT_CONFIG_FILE`
- **Then** blank lines and lines starting with `#` MUST be ignored
- **And** a line without an `=` separator MUST cause the agent to fail fast with an error
  identifying `AGENT_CONFIG_FILE` and the offending line
- **And** a nonexistent or unreadable file referenced by `AGENT_CONFIG_FILE` MUST cause the agent
  to fail fast with an error identifying `AGENT_CONFIG_FILE`
- **And** loading MUST NOT mutate the process's real environment variables (`os.Setenv`) as a side
  effect — merging happens in an isolated map so `Load()` remains safe to call repeatedly

##### AC-XC-CFG-020: Fail-fast on missing required config

- **Validates:** REQ-XC-CFG-020, REQ-XC-CFG-030
- **Given** a required config value (AGENT_NAME, AGENT_ENVIRONMENT, AGENT_COST, DCM_REGISTRATION_URL, AGENT_MESSAGING_URL) is absent or empty
- **When** the agent starts
- **Then** the agent MUST return an error identifying the missing field
- **And** MUST exit before starting the HTTP server or any subsystem

##### AC-XC-CFG-030: Fail-fast on invalid AGENT_COST

- **Validates:** REQ-XC-CFG-040, REQ-XC-CFG-041
- **Given** AGENT_COST is set to an invalid value (e.g., "expensive")
- **When** the agent starts
- **Then** the agent MUST return an error identifying the invalid configuration
- **And** MUST exit before starting any subsystem

##### AC-XC-CFG-050: Invalid config range fails fast

- **Validates:** REQ-XC-CFG-050
- **Given** `AGENT_HEALTH_CHECK_INTERVAL=0ms` (below minimum)
- **When** the agent starts
- **Then** the agent MUST exit with an error identifying the invalid value and its valid range

---

## 6. Consolidated Configuration Reference

All configuration is loadable from environment variables. Configuration files are also supported; environment variables take precedence.

| Config Key | Env Var | Default | Required | Min | Max | Unit | Topic |
|------------|---------|---------|----------|-----|-----|------|-------|
| config.file | AGENT_CONFIG_FILE | (unset — no file loaded) | No | - | - | - | XC |
| server.address | AGENT_SERVER_ADDRESS | :8080 | No | - | - | - | 1 |
| server.shutdownTimeout | AGENT_SERVER_SHUTDOWN_TIMEOUT | 15s | No | 1s | 5m | duration | 1 |
| server.requestTimeout | AGENT_SERVER_REQUEST_TIMEOUT | 30s | No | 1s | 10m | duration | 1 |
| sp.embedded | AGENT_EMBEDDED_SPS | (empty) | No | - | - | - | 3 |
| sp.persistencePath | AGENT_SP_PERSISTENCE_PATH | /var/lib/environment-agent/registrations | No | - | - | - | 3 |
| health.checkInterval | AGENT_HEALTH_CHECK_INTERVAL | 10s | No | 1s | 5m | duration | 5 |
| health.checkTimeout | AGENT_HEALTH_CHECK_TIMEOUT | 5s | No | 500ms | health.checkInterval | duration | 5 |
| health.failureThreshold | AGENT_HEALTH_FAILURE_THRESHOLD | 3 | No | 1 | 100 | integer | 5 |
| health.podConditionsEnabled | AGENT_POD_CONDITIONS_ENABLED | auto | No | - | - | - | 5 |
| agent.name | AGENT_NAME | - | Yes | - | - | - | 6 |
| agent.environment | AGENT_ENVIRONMENT | - | Yes | - | - | - | 6 |
| agent.cost | AGENT_COST | - | Yes | - | - | - | 6 |
| dcm.registrationUrl | DCM_REGISTRATION_URL | - | Yes | - | - | - | 6 |
| dcm.initialBackoff | DCM_REGISTRATION_INITIAL_BACKOFF | 1s | No | 100ms | dcm.maxBackoff | duration | 6 |
| dcm.maxBackoff | DCM_REGISTRATION_MAX_BACKOFF | 5m | No | dcm.initialBackoff | 1h | duration | 6 |
| heartbeat.interval | AGENT_HEARTBEAT_INTERVAL | 30s | No | 5s | 10m | duration | 6 |
| messaging.url | AGENT_MESSAGING_URL | - | Yes | - | - | - | 7 |
| messaging.topicName | AGENT_TOPIC_NAME | (derived from AGENT_NAME) | No | - | - | Unprefixed base name; CP-facing main subject is `dcm.agent.{base}` | 7 |
| routing.retryMaxAttempts | AGENT_ROUTING_RETRY_MAX | 3 | No | 0 | 20 | integer | 8 |
| routing.retryBackoff | AGENT_ROUTING_RETRY_BACKOFF | 2s | No | 100ms | routing.retryMaxBackoff | duration | 8 |
| routing.retryMaxBackoff | AGENT_ROUTING_RETRY_MAX_BACKOFF | 30s | No | routing.retryBackoff | 5m | duration | 8 |
| routing.denyListMaxSize | AGENT_DENY_LIST_MAX_SIZE | 100000 | No | 1000 | 10000000 | integer | 8 |

---

## 7. Design Decisions

See [Design Decisions](../decisions/environment-agent.decisions.md).

---

## 8. Assumptions

- NATS with JetStream is deployed and accessible to both DCM and the agent
  (consistent with other DCM components). The control plane pre-provisions
  `dcm-agent-requests` and `dcm-agent-responses` streams; the agent creates
  durable consumers and its own internal retry stream
- The agent has outbound network connectivity to DCM's REST API (for
  registration and heartbeats)
- External SPs have network connectivity to the agent's REST API (for
  registration and health checks)
- The agent has access to local persistent storage for persisting SP
  registrations across restarts
- For Kubernetes/OpenShift deployments: the agent's service account has RBAC
  permissions for the `pods/status` subresource
- SP idempotent creation behavior is the final safety net for duplicate requests
  (see DD-060)
- ASM-RTE-010: Service Providers MUST implement idempotent create and delete
  operations with respect to resource ID (`resource_id`). Duplicate invocations (from JetStream
  redelivery or retry) MUST NOT produce side effects beyond the first successful
  execution.
- NATS JetStream provides durable persistence for streams using file-based
  storage (messages survive consumer and server restarts)

---

## 9. Requirement ID Index

| Prefix | Topic | Count |
|--------|-------|-------|
| REQ-HTTP-NNN | 4.1: HTTP Server | 12 |
| REQ-HLT-NNN | 4.2: Health Service | 7 |
| REQ-SPR-NNN | 4.3: SP Registration & Management | 26 |
| REQ-STS-NNN | 4.4: Provider Query Endpoints | 7 |
| REQ-HMN-NNN | 4.5: SP Health Monitoring | 31 |
| REQ-DCM-NNN | 4.6: DCM Registration & Heartbeat | 18 |
| REQ-MSG-NNN | 4.7: Messaging System Integration | 24 |
| REQ-RTE-NNN | 4.8: Resource Operation Routing | 22 |
| REQ-RCM-NNN | 4.9: Retry & Cancel Mechanisms | 23 |
| REQ-XC-ERR-NNN | 5.1: Error Handling | 4 |
| REQ-XC-CE-NNN | 5.2: CloudEvent Definitions | 5 |
| REQ-XC-LOG-NNN | 5.3: Logging | 2 |
| REQ-XC-CFG-NNN | 5.4: Configuration Management | 6 |
| **Total** | | **187** |
