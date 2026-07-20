# Integration Test Plan: Environment Agent

## Overview

This document outlines the Integration Test Plan for the DCM Environment Agent, covering all 9 topics and cross-cutting concerns. Integration tests assert **observable behaviors** that require real dependencies — HTTP servers, persistence, messaging systems, and network calls to SPs and DCM.

**BDD scope:** Each test asserts what a consumer (HTTP client, DCM Control Plane, SP, or NATS subscriber) would observe. Pure algorithms (backoff formula, LRU data structure internals, pattern validation) are unit-tested; integration tests only assert the observable outcomes of those mechanisms.

**Caller-Trust:** Preconditions that upper layers already guarantee are not re-tested.

## Test Infrastructure Assumptions

| Dependency | Approach |
|------------|----------|
| HTTP Server | Real HTTP server started on random available port |
| File System | Real filesystem (OS-level temp directory) for persistence tests |
| NATS JetStream | Real server (embedded `nats-server` with JetStream or testcontainer) |
| DCM Control Plane | Mock HTTP server capturing `POST /api/v1/agents` and `PUT /api/v1/agents/{agentId}/heartbeat` |
| External SPs | Mock HTTP servers simulating health responses and create/delete responses |
| Embedded SPs | Real in-process SP stubs with controllable health |
| Clock/timing | Shortened intervals in test config for determinism |

### Shared Fixtures

Unless overridden, tests use:
- `AGENT_NAME=agent-prod-1`
- `AGENT_ENVIRONMENT=production`
- `AGENT_COST=medium`
- `AGENT_TOPIC_NAME=agent-prod-1`
- `DCM_REGISTRATION_URL=<mock DCM base URL>`
- `AGENT_MESSAGING_URL=<test NATS URL>`

---

## Topic 1: HTTP Server

### IT-HTTP-010: Server starts on configured address

- **Validates AC:** AC-HTTP-010
- **Test Infrastructure:** Real HTTP server, OS network stack
- **Given** the agent is configured with `AGENT_SERVER_ADDRESS=:0` (random port)
- **When** the agent starts
- **Then** the HTTP server MUST accept TCP connections on the allocated port
- **And** `GET /api/v1alpha1/health` MUST return HTTP 200

---

### IT-HTTP-020: All OpenAPI routes are registered

- **Validates AC:** AC-HTTP-020
- **Test Infrastructure:** Real HTTP server
- **Given** the HTTP server has started
- **When** requests are made to each defined endpoint:
  - `GET /api/v1alpha1/health`
  - `GET /api/v1alpha1/providers`
  - `POST /api/v1alpha1/providers`
  - `GET /api/v1alpha1/providers/{provider_id}`
- **Then** each request MUST be routed to the corresponding handler (i.e., MUST NOT return 404)

---

### IT-HTTP-030: Graceful shutdown on SIGTERM

- **Validates AC:** AC-HTTP-030, AC-HTTP-080
- **Test Infrastructure:** Real HTTP server, OS process signaling, slow endpoint
- **Given** the agent is running and a slow in-flight request is active (handler sleeps < shutdown timeout)
- **When** SIGTERM is sent to the process
- **Then** new connection attempts after the signal MUST be refused
- **And** the in-flight request MUST complete normally
- **And** the process MUST exit with code 0
- **And** logs MUST contain a shutdown initiation message

---

### IT-HTTP-040: Graceful shutdown on SIGINT

- **Validates AC:** AC-HTTP-040
- **Test Infrastructure:** Same as IT-HTTP-030
- **Given** the agent is running with an in-flight request
- **When** SIGINT is sent to the process
- **Then** behavior MUST be identical to SIGTERM (as tested in IT-HTTP-030)

---

### IT-HTTP-050: Shutdown timeout ejects in-flight requests with 503

- **Validates AC:** AC-HTTP-030
- **Test Infrastructure:** Real HTTP server, `AGENT_SERVER_SHUTDOWN_TIMEOUT=1s`, handler that sleeps 5s
- **Given** the agent is running with shutdown timeout of 1s and a request in-flight that takes 5s
- **When** SIGTERM is sent
- **Then** the in-flight request MUST receive HTTP 503 after the 1s drain timeout
- **And** the process MUST exit with code 0

---

### IT-HTTP-060: Configuration loaded from environment variables

- **Validates AC:** AC-HTTP-050
- **Test Infrastructure:** Real HTTP server
- **Given** `AGENT_SERVER_ADDRESS=:9090` is set
- **When** the agent starts
- **Then** the server MUST listen on port 9090

---

### IT-HTTP-070: Request logging includes required fields

- **Validates AC:** AC-HTTP-060
- **Test Infrastructure:** Real HTTP server, log capture
- **Given** the agent is running
- **When** `GET /api/v1alpha1/health` is processed
- **Then** logs MUST contain an INFO-level entry with: method=`GET`, path=`/api/v1alpha1/health`, status=`200`, and a non-zero duration

---

### IT-HTTP-080: Panic recovery returns RFC 7807 INTERNAL error

- **Validates AC:** AC-HTTP-070
- **Test Infrastructure:** Real HTTP server with a test handler that panics
- **Given** a handler is registered that triggers a panic
- **When** a request is made to that handler
- **Then** the response MUST be HTTP 500
- **And** the body MUST be RFC 7807 with `type=INTERNAL`
- **And** the body MUST NOT contain stack traces or file paths
- **And** the process MUST NOT crash
- **And** logs MUST contain an ERROR-level entry with the panic and stack trace

---

### IT-HTTP-090: Lifecycle logging on startup and shutdown

- **Validates AC:** AC-HTTP-080
- **Test Infrastructure:** Real HTTP server, log capture
- **Given** the agent starts and then is shut down
- **When** startup and shutdown complete
- **Then** logs MUST contain the listen address on startup
- **And** logs MUST contain shutdown initiation and completion messages

---

### IT-HTTP-100: Malformed request returns 400 with RFC 7807

- **Validates AC:** AC-HTTP-090
- **Test Infrastructure:** Real HTTP server
- **Given** the agent is running
- **When** `POST /api/v1alpha1/providers` is called with an invalid JSON body (`{"bad":`)
- **Then** the response MUST be HTTP 400
- **And** the body MUST be RFC 7807 with `type=INVALID_ARGUMENT`

---

### IT-HTTP-110: Framework-layer error responses are RFC 7807

- **Validates AC:** AC-HTTP-091
- **Test Infrastructure:** Real HTTP server
- **Given** the agent is running
- **When** a request triggers a framework-level parsing failure
- **Then** the error response MUST have `Content-Type: application/problem+json`
- **And** the body MUST conform to RFC 7807

---

### IT-HTTP-120: Per-request timeout enforcement

- **Validates AC:** AC-HTTP-095
- **Test Infrastructure:** Real HTTP server, `AGENT_SERVER_REQUEST_TIMEOUT=1s`, slow handler (sleeps 3s)
- **Given** the agent is configured with a per-request timeout of 1s
- **When** a request reaches a handler that takes longer than 1s
- **Then** the response MUST be HTTP 503 with RFC 7807 body (`type=UNAVAILABLE`)
- **And** the request context MUST be cancelled (handler observes context done)

---

## Topic 2: Health Service

### IT-HLT-010: Health endpoint returns 200 when healthy

- **Validates AC:** AC-HLT-010, AC-HLT-040
- **Test Infrastructure:** Real HTTP server with messaging system connected
- **Given** the HTTP server is running and messaging system is connected
- **When** `GET /api/v1alpha1/health` is called
- **Then** the response MUST be HTTP 200 OK
- **And** `Content-Type` MUST be `application/json`

---

### IT-HLT-020: Health response body — healthy state

- **Validates AC:** AC-HLT-020
- **Test Infrastructure:** Real HTTP server, messaging connected
- **Given** the agent is operational and the messaging system is connected
- **When** `GET /api/v1alpha1/health` is called
- **Then** the response body MUST contain:
  - `"status": "healthy"`
  - `"path": "health"`

---

### IT-HLT-030: Health response body — unhealthy state

- **Validates AC:** AC-HLT-030
- **Test Infrastructure:** Real HTTP server, messaging system disconnected/unavailable
- **Given** the agent is running but the messaging system is disconnected
- **When** `GET /api/v1alpha1/health` is called
- **Then** the response MUST be HTTP 200 OK
- **And** the response body MUST contain `"status": "unhealthy"`

---

### IT-HLT-040: Health endpoint performs no blocking I/O

- **Validates AC:** AC-HLT-050
- **Test Infrastructure:** Real HTTP server, messaging system unreachable (no response possible)
- **Given** the agent is running but external dependencies are unreachable
- **When** `GET /api/v1alpha1/health` is called 100 times (after 10 warm-up requests)
- **Then** the p99 response time MUST be below 5ms
- **And** the response MUST be derived from in-memory state

---

## Topic 3: SP Registration & Management

### IT-SPR-010: Embedded SP registration at startup

- **Validates AC:** AC-SPR-010
- **Test Infrastructure:** Real HTTP server, `AGENT_EMBEDDED_SPS=container,cluster`
- **Given** the agent is configured with `AGENT_EMBEDDED_SPS=container,cluster`
- **When** the agent starts
- **Then** `GET /api/v1alpha1/providers` MUST list providers for "container" and "cluster" service types
- **And** their `type` field MUST be `"embedded"`
- **And** no outbound HTTP calls to SP endpoints MUST occur during registration

---

### IT-SPR-020: Embedded SPs not active by default

- **Validates AC:** AC-SPR-020
- **Test Infrastructure:** Real HTTP server, `AGENT_EMBEDDED_SPS` not set
- **Given** `AGENT_EMBEDDED_SPS` is empty or not set
- **When** the agent starts
- **Then** `GET /api/v1alpha1/providers` MUST return an empty `results` array

---

### IT-SPR-030: Embedded SP skipped when slot occupied by persisted external SP

- **Validates AC:** AC-SPR-030
- **Test Infrastructure:** Real HTTP server, persistence with pre-existing external SP, log capture
- **Given** a persisted external SP registration exists for service type "container"
- **And** the agent is configured with `AGENT_EMBEDDED_SPS=container`
- **When** the agent starts
- **Then** `GET /api/v1alpha1/providers` MUST show only the external SP for "container"
- **And** logs MUST contain a WARN-level entry about the skipped embedded SP
- **And** the agent MUST be fully operational (HTTP server listening, health returning 200)

---

### IT-SPR-040: External SP registration — success (new)

- **Validates AC:** AC-SPR-040
- **Test Infrastructure:** Real HTTP server
- **Given** no SP is currently serving service type "database"
- **When** `POST /api/v1alpha1/providers` is called with body:
  ```json
  {"name": "db-provider", "endpoint": "https://sp.example.com:8080", "service_type": "database", "schema_version": "v1alpha1"}
  ```
- **Then** the response MUST be HTTP 201 Created
- **And** the response body MUST include server-set fields: `id`, `path`, `create_time`, `update_time`
- **And** `type` MUST be `"external"`

---

### IT-SPR-050: External SP re-registration (idempotent update)

- **Validates AC:** AC-SPR-050
- **Test Infrastructure:** Real HTTP server, pre-registered SP
- **Given** a provider named "db-provider" is already registered for service type "database"
- **When** `POST /api/v1alpha1/providers` is called with the same `name` and `service_type`
- **Then** the response MUST be HTTP 200 OK
- **And** `update_time` MUST be refreshed (later than before)
- **And** `create_time` MUST remain unchanged

---

### IT-SPR-060: Service type conflict

- **Validates AC:** AC-SPR-060
- **Test Infrastructure:** Real HTTP server, embedded SP for "container"
- **Given** an embedded SP is serving service type "container"
- **When** `POST /api/v1alpha1/providers` is called with `{"name": "new-sp", "service_type": "container", ...}`
- **Then** the response MUST be HTTP 409 Conflict
- **And** the error body MUST identify the conflicting provider name and service type

---

### IT-SPR-070: Same SP re-registers for same service type (idempotent, not conflict)

- **Validates AC:** AC-SPR-070
- **Test Infrastructure:** Real HTTP server
- **Given** "vm-provider" is registered for service type "vm"
- **When** "vm-provider" re-registers for service type "vm"
- **Then** the response MUST be HTTP 200 OK (not 409)

---

### IT-SPR-080: Persistence across restart

- **Validates AC:** AC-SPR-090
- **Test Infrastructure:** Real HTTP server, real filesystem persistence
- **Given** an external SP "db-provider" is registered for service type "database"
- **When** the agent process is stopped and restarted
- **Then** `GET /api/v1alpha1/providers` MUST include "db-provider" with service type "database"
- **And** the registration MUST be loaded before accepting new registrations

---

### IT-SPR-090: Re-registration with changed service type (available)

- **Validates AC:** AC-SPR-095
- **Test Infrastructure:** Real HTTP server
- **Given** "db-provider" is registered for service type "database"
- **And** no SP is serving service type "analytics"
- **When** `POST /api/v1alpha1/providers` is called with `{"name": "db-provider", "service_type": "analytics", ...}`
- **Then** the response MUST be HTTP 200 OK
- **And** "db-provider" MUST now serve "analytics"
- **And** the "database" slot MUST be freed (another SP can register for it)

---

### IT-SPR-100: Re-registration with changed service type (conflict)

- **Validates AC:** AC-SPR-096
- **Test Infrastructure:** Real HTTP server
- **Given** "db-provider" is registered for "database" and "other-provider" serves "analytics"
- **When** "db-provider" re-registers with `service_type="analytics"`
- **Then** the response MUST be HTTP 409 Conflict
- **And** "db-provider" MUST still serve "database" (no partial state change)

---

### IT-SPR-110: Invalid registration body returns 400

- **Validates AC:** AC-SPR-100
- **Test Infrastructure:** Real HTTP server
- **Given** the agent is running
- **When** `POST /api/v1alpha1/providers` is called with body missing required `service_type`
- **Then** the response MUST be HTTP 400 Bad Request with RFC 7807 error body

---

### IT-SPR-120: Provider ID from query parameter

- **Validates AC:** AC-SPR-105
- **Test Infrastructure:** Real HTTP server
- **Given** the agent is running
- **When** `POST /api/v1alpha1/providers?id=custom-001` is called with a valid body
- **Then** the response `id` MUST equal `"custom-001"`

---

### IT-SPR-130: Provider ID auto-generated as UUID v4

- **Validates AC:** AC-SPR-106
- **Test Infrastructure:** Real HTTP server
- **Given** `POST /api/v1alpha1/providers` is called with no `?id=` parameter
- **When** the registration succeeds
- **Then** the response `id` MUST be a non-empty string matching UUID v4 format

---

### IT-SPR-140: Provider ID AEP-122 pattern violation returns 422

- **Validates AC:** AC-SPR-106b
- **Test Infrastructure:** Real HTTP server
- **Given** the agent is running
- **When** `POST /api/v1alpha1/providers?id=INVALID_ID!` is called
- **Then** the response MUST be HTTP 422 Unprocessable Entity
- **And** the body MUST be RFC 7807 with `type=UNPROCESSABLE_ENTITY`
- **And** the error MUST identify the `?id=` pattern violation

---

### IT-SPR-150: Provider schema_version required

- **Validates AC:** AC-SPR-107
- **Test Infrastructure:** Real HTTP server
- **Given** `POST /api/v1alpha1/providers` is called with body missing `schema_version`
- **When** the request is processed
- **Then** the response MUST be HTTP 400 Bad Request

---

### IT-SPR-160: Semantic validation returns 422

- **Validates AC:** AC-SPR-108
- **Test Infrastructure:** Real HTTP server
- **Given** `POST /api/v1alpha1/providers` is called with `schema_version="invalid-version"`
- **When** the request is processed
- **Then** the response MUST be HTTP 422 Unprocessable Entity
- **And** the body MUST be RFC 7807 with `type=UNPROCESSABLE_ENTITY`

---

### IT-SPR-165: Endpoint URI semantic validation returns 422

- **Validates AC:** AC-SPR-108b
- **Test Infrastructure:** Real HTTP server
- **Given** the agent is running
- **When** `POST /api/v1alpha1/providers` is called with body `{"name": "bad-sp", "endpoint": "not-a-url", "service_type": "database", "schema_version": "v1alpha1"}`
- **Then** the response MUST be HTTP 422 Unprocessable Entity
- **And** the body MUST be RFC 7807 with `type=UNPROCESSABLE_ENTITY`

---

### IT-SPR-170: Persistence load failure causes fail-fast

- **Validates AC:** AC-SPR-109
- **Test Infrastructure:** Corrupted persistence file
- **Given** the persistence store contains corrupted/unreadable data
- **When** the agent starts
- **Then** the agent MUST log the error and exit immediately
- **And** the HTTP server MUST NOT start

---

### IT-SPR-180: One SP per service type enforced (any combination)

- **Validates AC:** AC-SPR-110
- **Test Infrastructure:** Real HTTP server
- **Given** an SP is registered for service type "database" (any type)
- **When** a different SP attempts to register for "database"
- **Then** the response MUST be HTTP 409 Conflict

---

## Topic 4: Provider Query Endpoints

### IT-STS-010: List providers with multiple providers

- **Validates AC:** AC-STS-010
- **Test Infrastructure:** Real HTTP server, mixed embedded + external SPs
- **Given** an embedded SP "k8s-container" (container, Ready) and an external SP "db-provider" (database, Unhealthy) are registered
- **When** `GET /api/v1alpha1/providers` is called
- **Then** the response MUST be HTTP 200 OK
- **And** the body MUST contain a `results` array with both providers
- **And** each entry MUST include `type`, `status`, `last_check_time`

---

### IT-STS-020: List providers with no providers

- **Validates AC:** AC-STS-020
- **Test Infrastructure:** Real HTTP server, no SPs registered
- **Given** no SPs are registered
- **When** `GET /api/v1alpha1/providers` is called
- **Then** the response MUST be HTTP 200 OK with `{"results": []}`

---

### IT-STS-030: List includes all health states

- **Validates AC:** AC-STS-022
- **Test Infrastructure:** Real HTTP server, SPs in Ready, Unhealthy, and Unavailable states
- **Given** SPs exist in Ready, Unhealthy, and Unavailable states
- **When** `GET /api/v1alpha1/providers` is called
- **Then** all SPs MUST appear in the `results` array regardless of health state

---

### IT-STS-040: Get provider by ID

- **Validates AC:** AC-STS-025
- **Test Infrastructure:** Real HTTP server, registered SP
- **Given** an SP "db-provider" is registered with ID "sp-db-001"
- **When** `GET /api/v1alpha1/providers/sp-db-001` is called
- **Then** the response MUST be HTTP 200 OK with the full Provider resource
- **And** the response MUST include `type`, `status`, `last_check_time`

---

### IT-STS-050: Get provider — not found

- **Validates AC:** AC-STS-026
- **Test Infrastructure:** Real HTTP server
- **Given** no SP is registered with ID "nonexistent"
- **When** `GET /api/v1alpha1/providers/nonexistent` is called
- **Then** the response MUST be HTTP 404 Not Found with RFC 7807 error body

---

### IT-STS-060: Provider list reflects real-time health transitions

- **Validates AC:** AC-STS-030
- **Test Infrastructure:** Real HTTP server, controllable mock SP endpoint
- **Given** an external SP transitions from Ready to Unhealthy
- **When** `GET /api/v1alpha1/providers` is called after the transition
- **Then** the provider's `status` MUST be `"Unhealthy"`

---

### IT-STS-070: Content-Type on provider endpoints

- **Validates AC:** AC-STS-035
- **Test Infrastructure:** Real HTTP server
- **Given** any GET request to `/api/v1alpha1/providers` or `/api/v1alpha1/providers/{id}`
- **When** the response is returned
- **Then** `Content-Type` MUST be `application/json`

---

## Topic 5: SP Health Monitoring

### IT-HMN-010: External SP health check — Ready

- **Validates AC:** AC-HMN-010
- **Test Infrastructure:** Real HTTP server, mock SP health endpoint returning `{"status": "healthy"}`
- **Given** an external SP is registered with endpoint pointing to the mock
- **When** the health check polls `GET {endpoint}/health`
- **And** the SP responds with `200 OK` and `{"status": "healthy"}`
- **Then** the SP MUST be marked as Ready

---

### IT-HMN-020: External SP health check — Unhealthy

- **Validates AC:** AC-HMN-020
- **Test Infrastructure:** Mock SP returning `{"status": "unhealthy"}`
- **Given** an external SP is registered
- **When** the health check receives `200 OK` with `{"status": "unhealthy"}`
- **Then** the SP MUST be marked as Unhealthy
- **And** the failure counter MUST NOT be incremented

---

### IT-HMN-030: External SP health check — Unavailable after threshold

- **Validates AC:** AC-HMN-030
- **Test Infrastructure:** Mock SP that is unreachable, `AGENT_HEALTH_FAILURE_THRESHOLD=3`
- **Given** an external SP is registered and the mock refuses connections
- **When** the health check fails 3 consecutive times
- **Then** the SP MUST be marked as Unavailable

---

### IT-HMN-040: Healthy response resets failure counter

- **Validates AC:** AC-HMN-040
- **Test Infrastructure:** Mock SP toggling between error and healthy
- **Given** an external SP has 2 consecutive failures (threshold=3)
- **When** the next health check receives a healthy response
- **Then** the failure counter MUST reset to 0
- **And** a subsequent single failure MUST NOT trigger Unavailable

---

### IT-HMN-050: Configurable health check interval and timeout

- **Validates AC:** AC-HMN-005
- **Test Infrastructure:** Mock SP, `AGENT_HEALTH_CHECK_INTERVAL=200ms`, `AGENT_HEALTH_CHECK_TIMEOUT=50ms`
- **Given** the health check is configured with interval 200ms and timeout 50ms
- **When** health checks run for 1s
- **Then** between 4 and 6 checks MUST occur
- **And** a slow SP exceeding the 50ms timeout MUST be treated as a failed check

---

### IT-HMN-060: Embedded SP health check in-process

- **Validates AC:** AC-HMN-015
- **Test Infrastructure:** Embedded SP stub with controllable health
- **Given** an embedded SP is registered
- **When** the health check runs
- **Then** it MUST execute in-process (no outbound HTTP call to any URL)
- **And** the SP state MUST reflect the in-process check result

---

### IT-HMN-070: External SP starts Unhealthy

- **Validates AC:** AC-HMN-051
- **Test Infrastructure:** Real HTTP server, external SP registration
- **Given** an external SP registers successfully
- **When** the registration response is returned (before any health check runs)
- **Then** the SP state MUST be Unhealthy (initial state per spec)

---

### IT-HMN-080: Embedded SP immediate health check — passes

- **Validates AC:** AC-HMN-052
- **Test Infrastructure:** Embedded SP stub reporting healthy
- **Given** an embedded SP is registered at startup
- **When** the in-process health check passes immediately
- **Then** the SP state MUST be Ready

---

### IT-HMN-090: Embedded SP immediate health check — reports unhealthy

- **Validates AC:** AC-HMN-053
- **Test Infrastructure:** Embedded SP stub reporting unhealthy
- **Given** an embedded SP is registered at startup
- **When** the in-process health check reports unhealthy
- **Then** the SP state MUST be Unhealthy

---

### IT-HMN-100: Unhealthy SP keeps service type advertised but stops routing

- **Validates AC:** AC-HMN-050
- **Test Infrastructure:** Mock DCM, mock SP transitioning to Unhealthy, NATS with create request
- **Given** the SP for service type "database" becomes Unhealthy
- **When** the agent evaluates its advertised service types
- **Then** "database" MUST remain in the advertised list to DCM
- **And** new requests for "database" MUST NOT be routed to the SP (queued to retry instead)

---

### IT-HMN-110: Unavailable SP removes service type from DCM

- **Validates AC:** AC-HMN-060
- **Test Infrastructure:** Mock DCM, SP forced to Unavailable
- **Given** the SP for service type "database" becomes Unavailable
- **When** the agent updates DCM
- **Then** the registration payload MUST NOT include "database" in `serviceTypes`

---

### IT-HMN-120: SP recovery from Unavailable

- **Validates AC:** AC-HMN-070
- **Test Infrastructure:** Mock DCM, mock SP recovering, NATS with retry topic
- **Given** the SP for "database" was Unavailable and removed from DCM
- **When** the SP recovers to Ready
- **Then** the agent MUST re-add "database" to its advertised list
- **And** MUST send an updated registration to DCM including "database"
- **And** MUST process held requests from the retry topic

---

### IT-HMN-130: Health degraded CloudEvent

- **Validates AC:** AC-HMN-080
- **Test Infrastructure:** NATS subscriber on `dcm.agents.health`, SP transitioning to Unhealthy
- **Given** the SP for "database" transitions to Unhealthy
- **When** the state change is detected
- **Then** a CloudEvent MUST be published to `dcm.agents.health`
- **And** `type` MUST be `dcm.agent.health.service-type-degraded`
- **And** the CE data MUST include `agentId`, `agentName`, `topicName`, `serviceType`, `reason`, and `affectedProvider`

---

### IT-HMN-140: Pod conditions updated on state change

- **Validates AC:** AC-HMN-100
- **Test Infrastructure:** Mock Kubernetes API, `AGENT_POD_CONDITIONS_ENABLED=true`
- **Given** the agent runs on Kubernetes with pod conditions enabled
- **And** SP "db-provider" transitions from Ready to Unhealthy
- **When** the state change is detected
- **Then** a pod condition patch MUST be issued with `status=False`, `reason=Unhealthy`
- **And** the condition type MUST incorporate provider ID and service type
- **And** the `message` MUST include SP name, service type, and health state

---

### IT-HMN-150: Pod conditions non-fatal when unavailable

- **Validates AC:** AC-HMN-110
- **Test Infrastructure:** No Kubernetes API available (or returns 403)
- **Given** the agent runs outside Kubernetes or lacks RBAC permissions
- **When** a pod condition update fails
- **Then** a WARN-level log MUST be emitted
- **And** the agent MUST continue operating normally (health checks, routing, heartbeats)

---

### IT-HMN-160: Pod Readiness Gates used for conditions

- **Validates AC:** AC-HMN-120
- **Test Infrastructure:** Mock Kubernetes API, `AGENT_POD_CONDITIONS_ENABLED=true`
- **Given** the agent runs on Kubernetes with pod conditions enabled
- **When** an SP health state changes
- **Then** the agent SHOULD use Pod Readiness Gates to surface the condition
- **And** SHOULD use in-cluster authentication to patch the pod's `status.conditions`

---

### IT-HMN-170: Unavailable to Unhealthy re-advertises service type

- **Validates AC:** AC-HMN-185
- **Test Infrastructure:** Mock DCM, SP transitioning from Unavailable to Unhealthy
- **Given** an SP was Unavailable and its service type was removed from DCM
- **When** the SP responds with `{"status": "unhealthy"}` (reachable but not healthy)
- **Then** the agent MUST re-add the service type to its advertised list
- **And** MUST send updated registration to DCM
- **And** the retry topic MUST NOT be processed (only on Ready transition)

---

## Topic 6: DCM Registration & Heartbeat

### IT-DCM-010: Initial registration after first non-Unavailable SP

- **Validates AC:** AC-DCM-010
- **Test Infrastructure:** Real NATS; mock DCM (`POST /api/v1/agents` → 201 with `agentId="agent-123"`); embedded SP stub
- **Given** the agent starts with one embedded SP configured that starts Unhealthy then becomes Ready
- **And** mock DCM has received zero registration calls
- **When** the embedded SP health check succeeds and the SP transitions to Ready
- **Then** mock DCM MUST receive exactly one `POST /api/v1/agents` with non-empty `serviceTypes`
- **And** subsequent heartbeats MUST use path `/api/v1/agents/agent-123/heartbeat`

---

### IT-DCM-015: agentId absent before DCM registration

- **Validates AC:** AC-DCM-010
- **Test Infrastructure:** Mock DCM that delays response; embedded SP
- **Given** the agent has not yet completed DCM registration (mock DCM has not responded)
- **When** the CloudEvent source is inspected (e.g., from a health-degraded event)
- **Then** the agent MUST NOT have an agentId in memory
- **And** heartbeats MUST NOT be sent before registration completes

---

### IT-DCM-020: Registration does not block HTTP startup

- **Validates AC:** AC-DCM-015
- **Test Infrastructure:** Mock DCM that hangs (never responds)
- **Given** `DCM_REGISTRATION_URL` points to a hanging mock
- **When** the agent process starts
- **Then** `GET /api/v1alpha1/health` MUST return 200 before any DCM registration completes
- **And** the agent process MUST remain running

---

### IT-DCM-030: Registration waits until a non-Unavailable SP exists

- **Validates AC:** AC-DCM-020
- **Test Infrastructure:** Mock DCM; agent started with no SPs
- **Given** the agent is running with an empty provider set
- **When** a wait window elapses longer than normal registration timing
- **Then** mock DCM MUST have received zero `POST /api/v1/agents` calls

---

### IT-DCM-040: Pre-registration defers service type changes

- **Validates AC:** AC-DCM-025
- **Test Infrastructure:** Mock DCM that delays first success; two external SPs
- **Given** the agent is not yet registered (mock DCM has not returned agentId)
- **When** two SPs register and become Ready before DCM accepts registration
- **Then** mock DCM MUST NOT receive intermediate update-only registrations
- **And** the first successful registration MUST include both service types

---

### IT-DCM-050: Registration payload correctness

- **Validates AC:** AC-DCM-030
- **Test Infrastructure:** Mock DCM capturing request body
- **Given** config `name="agent-prod-1"`, `environment="production"`, `cost="medium"`, `topicName="agent-prod-1"` and service types ["container", "database"]
- **When** the agent registers to DCM
- **Then** the payload MUST include all required fields with correct values

---

### IT-DCM-060: resourcesAvailable in registration

- **Validates AC:** AC-DCM-035
- **Test Infrastructure:** Mock DCM; resource availability configured
- **Given** resource availability is available
- **When** the agent registers to DCM
- **Then** the payload SHOULD include `resourcesAvailable`

---

### IT-DCM-070: Idempotent re-registration on restart

- **Validates AC:** AC-DCM-040
- **Test Infrastructure:** Mock DCM keyed by name; restart agent
- **Given** a prior run registered successfully with `name="agent-prod-1"` and `agentId="agent-123"`
- **When** the agent restarts and re-registers with the same name
- **Then** mock DCM MUST receive another `POST /api/v1/agents`
- **And** the returned agentId MUST be the same
- **And** heartbeats MUST target `/api/v1/agents/agent-123/heartbeat`

---

### IT-DCM-080: Registration retry with exponential backoff

- **Validates AC:** AC-DCM-050
- **Test Infrastructure:** Mock DCM returning 503 then 201
- **Given** DCM is initially failing and at least one SP is Ready
- **When** registration eventually succeeds
- **Then** mock DCM MUST receive multiple attempts before success
- **And** each inter-attempt gap MUST fall within `[0, min(initialInterval × 2^attempt, maxInterval)]` (full jitter range)
- **And** `GET /api/v1alpha1/health` MUST remain 200 throughout

---

### IT-DCM-090: Non-retryable error stops retries

- **Validates AC:** AC-DCM-060
- **Test Infrastructure:** Mock DCM returning 400
- **Given** at least one SP is Ready and DCM returns 400
- **When** the agent receives that response
- **Then** mock DCM MUST receive exactly one registration attempt
- **And** logs MUST contain an ERROR-level entry
- **And** the agent MUST NOT exit

---

### IT-DCM-100: 429 respects Retry-After

- **Validates AC:** AC-DCM-061
- **Test Infrastructure:** Mock DCM returning 429 with `Retry-After: 2` then 201
- **Given** DCM returns 429 with `Retry-After: 2` on first attempt
- **When** the agent retries
- **Then** the second attempt MUST occur no earlier than ~2s after the first response
- **And** when 429 has no `Retry-After`, standard backoff MUST apply

---

### IT-DCM-110: Service type change triggers DCM update

- **Validates AC:** AC-DCM-070
- **Test Infrastructure:** Mock DCM; agent registered with ["container"]; new SP for "database"
- **Given** the agent is registered with `serviceTypes=["container"]`
- **When** an external SP registers for "database" and becomes non-Unavailable
- **Then** mock DCM MUST receive `POST /api/v1/agents` with `serviceTypes` including both

---

### IT-DCM-120: Periodic heartbeat

- **Validates AC:** AC-DCM-080
- **Test Infrastructure:** Mock DCM; `AGENT_HEARTBEAT_INTERVAL=1s`
- **Given** registration succeeded with `agentId="agent-123"`
- **When** at least two heartbeat intervals elapse
- **Then** mock DCM MUST receive ≥2 `PUT /api/v1/agents/agent-123/heartbeat` calls
- **And** each body MUST include `timestamp` (RFC 3339) and numeric `consumerLag`

---

### IT-DCM-130: Configurable heartbeat interval

- **Validates AC:** AC-DCM-085
- **Test Infrastructure:** Mock DCM; `AGENT_HEARTBEAT_INTERVAL=2s`
- **Given** the agent is registered
- **When** heartbeats are observed for ≥6s
- **Then** median interval MUST be approximately 2s (not default 30s)

---

### IT-DCM-140: Heartbeat includes consumer lag (main topic only)

- **Validates AC:** AC-DCM-090
- **Test Infrastructure:** Real JetStream; mock DCM; blocked SP
- **Given** the agent's main durable consumer has exactly 5 unacknowledged messages
- **And** retry/cancel topics also have pending messages
- **When** a heartbeat is sent
- **Then** `consumerLag` MUST equal 5 (excludes retry/cancel)

---

### IT-DCM-150: Heartbeat failure resilience

- **Validates AC:** AC-DCM-095
- **Test Infrastructure:** Mock DCM returning 500 on heartbeats
- **Given** the agent is registered and heartbeat requests fail
- **When** the next heartbeat interval elapses
- **Then** mock DCM MUST receive another attempt
- **And** the agent MUST NOT exit
- **And** health endpoint MUST still return 200

---

### IT-DCM-160: All SPs unavailable — empty serviceTypes

- **Validates AC:** AC-DCM-100
- **Test Infrastructure:** Mock DCM; two SPs forced to Unavailable
- **Given** the agent is registered with `serviceTypes=["container","database"]`
- **When** both SPs transition to Unavailable
- **Then** mock DCM MUST receive `POST /api/v1/agents` with `serviceTypes=[]`
- **And** heartbeats MUST continue (agent remains registered)

---

### IT-DCM-170: Advertised serviceTypes exclude Unavailable SPs

- **Validates AC:** AC-DCM-105
- **Test Infrastructure:** Mock DCM; SPs in Ready, Unhealthy, and Unavailable states
- **Given** SPs for "container" (Ready), "database" (Unhealthy), "storage" (Unavailable)
- **When** the agent sends a registration/update to DCM
- **Then** `serviceTypes` MUST include "container" and "database"
- **And** MUST NOT include "storage"

---

## Topic 7: Messaging System Integration

### IT-MSG-010: Topic creation at startup

- **Validates AC:** AC-MSG-010
- **Test Infrastructure:** Fresh JetStream; `AGENT_TOPIC_NAME="agent-prod-1"`
- **Given** no streams exist for `agent-prod-1*`
- **When** the agent starts
- **Then** JetStream MUST expose subjects for `agent-prod-1`, `agent-prod-1.retry`, `agent-prod-1.cancel`

---

### IT-MSG-020: Durable consumer creation on first start

- **Validates AC:** AC-MSG-015
- **Test Infrastructure:** Fresh JetStream
- **Given** no durable consumers exist
- **When** the agent starts
- **Then** JetStream MUST list durable consumers with deterministic names derived from topic name

---

### IT-MSG-030: Consumer reuse on restart

- **Validates AC:** AC-MSG-016
- **Test Infrastructure:** JetStream with existing consumers; restart agent
- **Given** durable consumers exist from a prior session
- **When** the agent restarts
- **Then** the set of consumer names MUST be unchanged (no duplicates created)

---

### IT-MSG-040: Unacked message redelivered after crash

- **Validates AC:** AC-MSG-018
- **Test Infrastructure:** JetStream; SP mock that blocks forever; kill agent
- **Given** a message was delivered but not acked (SP call in-flight)
- **When** the agent is killed and restarted
- **Then** the unacknowledged message MUST be redelivered

---

### IT-MSG-050: Topic reuse on restart

- **Validates AC:** AC-MSG-020
- **Test Infrastructure:** JetStream with existing streams; restart agent
- **Given** topics already exist from a prior session
- **When** the agent restarts
- **Then** startup MUST succeed without errors (reuses existing topics)

---

### IT-MSG-060: Only main topic advertised to DCM

- **Validates AC:** AC-MSG-025
- **Test Infrastructure:** Mock DCM
- **Given** the agent registers to DCM
- **When** the registration payload is inspected
- **Then** `topicName` MUST be `"agent-prod-1"` (not retry or cancel variants)

---

### IT-MSG-070: Main topic consumption

- **Validates AC:** AC-MSG-030
- **Test Infrastructure:** Real NATS; Ready SP mock; responses subscriber
- **Given** the agent is running with a Ready SP for "database"
- **When** a `dcm.request.create` CloudEvent is published to `agent-prod-1`
- **Then** the SP mock MUST receive the create call
- **And** `dcm.agents.responses` MUST receive a `dcm.agent.creation-acknowledged` CloudEvent

---

### IT-MSG-080: Continuous cancel consumption updates deny list

- **Validates AC:** AC-MSG-035
- **Test Infrastructure:** Real NATS; Ready SP; cancel then create published
- **Given** the agent is running
- **When** a `dcm.request.cancel` for `resourceId="res-cancel-1"` is published to `agent-prod-1.cancel`
- **And** a `dcm.request.create` for `resourceId="res-cancel-1"` is published to `agent-prod-1`
- **Then** the SP MUST NOT receive a create for `res-cancel-1`

---

### IT-MSG-090: Cancel topic drained before main processing on startup

- **Validates AC:** AC-MSG-040
- **Test Infrastructure:** JetStream pre-seeded with cancel and create for same resource
- **Given** cancel topic has cancel for `resourceId="res-drain-1"` and main topic has create for same
- **When** the agent starts
- **Then** the create MUST be dropped (deny list populated from cancel drain first)

---

### IT-MSG-095: Cancel topic drain timeout

- **Validates AC:** AC-MSG-040
- **Test Infrastructure:** JetStream; cancel topic with many messages; slow consumer simulation
- **Given** the cancel topic contains a large number of messages that cannot all be consumed within 5s
- **When** the agent starts
- **Then** drain MUST complete within the 5s timeout window
- **And** main topic processing MUST begin after the timeout
- **And** only cancels consumed within the window MUST populate the deny list

---

### IT-MSG-100: Messaging failure does not block HTTP; reconnects

- **Validates AC:** AC-MSG-050
- **Test Infrastructure:** Agent with unreachable NATS URL
- **Given** the messaging system is unreachable at startup
- **When** the agent starts
- **Then** `GET /api/v1alpha1/health` MUST return 200 promptly
- **And** after NATS becomes available, the agent MUST reconnect and consume messages

---

### IT-MSG-110: Ack only after routing outcome finalized

- **Validates AC:** AC-MSG-055
- **Test Infrastructure:** JetStream; SP mock with delayed response
- **Given** a create message is consumed and SP has not yet responded
- **When** routing is still in-flight
- **Then** the message MUST remain unacknowledged
- **And** after SP returns, the message MUST be acknowledged

---

### IT-MSG-120: Response CloudEvents include correlation fields

- **Validates AC:** AC-MSG-060
- **Test Infrastructure:** Ready SP; NATS subscriber on `dcm.agents.responses`
- **Given** the agent routes a create successfully
- **When** the response CloudEvent is observed
- **Then** it MUST conform to CloudEvents v1.0
- **And** `data` MUST include `agentName` and `topicName`
- **And** MUST be published to `dcm.agents.responses`

---

## Topic 8: Resource Operation Routing

### IT-RTE-010: Route creation to Ready embedded SP

- **Validates AC:** AC-RTE-010
- **Test Infrastructure:** Embedded SP stub; NATS; responses subscriber
- **Given** an embedded SP is registered and Ready for "container"
- **When** a `dcm.request.create` with `serviceType="container"` is consumed
- **Then** the embedded SP MUST receive an in-process create call (no outbound HTTP)
- **And** `dcm.agents.responses` MUST receive `dcm.agent.creation-acknowledged` with `status="PROVISIONING"`
- **And** the CE data MUST include `resourceId`, `agentName`, and `topicName`

---

### IT-RTE-015: Route deletion to Ready embedded SP

- **Validates AC:** AC-RTE-010
- **Test Infrastructure:** Embedded SP stub; NATS; responses subscriber
- **Given** an embedded SP is registered and Ready for "container"
- **When** a `dcm.request.delete` with `serviceType="container"` and `resourceId="res-embed-del"` is consumed
- **Then** the embedded SP MUST receive an in-process delete call (no outbound HTTP)
- **And** `dcm.agents.responses` MUST receive `dcm.agent.deletion-acknowledged` with `status="DELETING"`

---

### IT-RTE-020: Route creation to Ready external SP

- **Validates AC:** AC-RTE-020
- **Test Infrastructure:** External SP mock; NATS; responses subscriber
- **Given** an external SP at `http://mock:8080` is Ready for "database"
- **When** a `dcm.request.create` with `serviceType="database"` is consumed
- **Then** SP mock MUST receive `POST http://mock:8080` with the spec
- **And** `dcm.agents.responses` MUST receive `dcm.agent.creation-acknowledged` with `status="PROVISIONING"`
- **And** the CE data MUST include `resourceId`, `agentName`, and `topicName`

---

### IT-RTE-030: Route deletion to Ready external SP

- **Validates AC:** AC-RTE-030
- **Test Infrastructure:** External SP mock; NATS
- **Given** an external SP is Ready for "database"
- **When** a `dcm.request.delete` with `resourceId="res-123"` is consumed
- **Then** SP mock MUST receive `DELETE http://mock:8080/res-123`
- **And** `dcm.agents.responses` MUST receive `dcm.agent.deletion-acknowledged` with `status="DELETING"`
- **And** the CE data MUST include `resourceId`, `agentName`, and `topicName`

---

### IT-RTE-040: Unsupported service type yields error CE

- **Validates AC:** AC-RTE-040
- **Test Infrastructure:** NATS; no SP for "storage"; responses subscriber
- **Given** no SP exists for "storage"
- **When** a create with `serviceType="storage"` is consumed
- **Then** `dcm.agents.responses` MUST receive `dcm.agent.error` with `error="UNSUPPORTED_SERVICE_TYPE"`

---

### IT-RTE-050: SP is Unavailable — request rejected immediately

- **Validates AC:** AC-RTE-045
- **Test Infrastructure:** Unavailable SP; NATS; responses subscriber
- **Given** the SP for "database" is Unavailable
- **When** a create for "database" is consumed
- **Then** `dcm.agents.responses` MUST receive `dcm.agent.error` indicating unavailable
- **And** `agent-prod-1.retry` MUST NOT receive the message

---

### IT-RTE-060: SP is Unhealthy — request queued

- **Validates AC:** AC-RTE-050
- **Test Infrastructure:** Unhealthy SP; NATS; responses and retry subscribers
- **Given** the SP for "database" is Unhealthy
- **When** a create is consumed from main topic
- **Then** the original CloudEvent MUST be published to `agent-prod-1.retry`
- **And** `dcm.agents.responses` MUST receive `dcm.agent.request-queued` with `status="QUEUED"`
- **And** the CE data MUST include `resourceId`, `agentName`, and `topicName`
- **And** SP MUST NOT receive a call

---

### IT-RTE-070: Configurable retry policy exhaustion

- **Validates AC:** AC-RTE-055
- **Test Infrastructure:** Ready SP always returning 503; `AGENT_ROUTING_RETRY_MAX=5`
- **Given** the SP is Ready but always returns 503
- **When** a create is consumed
- **Then** SP MUST receive the configured number of attempts (1 initial + retries)
- **And** `dcm.agents.responses` MUST receive `dcm.agent.error`
- **And** retry topic MUST NOT receive the request

---

### IT-RTE-080: Retryable error applies retry policy (minimal budget)

- **Validates AC:** AC-RTE-060
- **Test Infrastructure:** Ready SP returning 503; `AGENT_ROUTING_RETRY_MAX=1`
- **Given** SP is Ready and returns 503 with `AGENT_ROUTING_RETRY_MAX=1` (no retries beyond initial)
- **When** the initial attempt fails
- **Then** SP mock call count MUST equal 1
- **And** `dcm.agent.error` MUST be published immediately

---

### IT-RTE-090: Non-retryable 4xx fails immediately

- **Validates AC:** AC-RTE-065
- **Test Infrastructure:** Ready SP returning 400
- **Given** SP is Ready and returns 400
- **When** the error response is received
- **Then** SP MUST receive exactly one call
- **And** `dcm.agent.error` MUST be published immediately

---

### IT-RTE-100: Deny list filters cancelled create

- **Validates AC:** AC-RTE-070
- **Test Infrastructure:** NATS; Ready SP; cancel then create
- **Given** the deny list contains `resourceId="res-456"`
- **When** a create for `resourceId="res-456"` is processed from main topic
- **Then** SP MUST NOT be called
- **And** no `creation-acknowledged` CE MUST be published
- **And** the message MUST be acknowledged

---

### IT-RTE-105: Deny list consume-on-use

- **Validates AC:** AC-RTE-076
- **Test Infrastructure:** NATS; Ready SP; cancel then two creates for same resourceId
- **Given** a cancel for `resourceId="res-consume"` populates the deny list
- **When** a first create for `resourceId="res-consume"` is consumed and filtered (entry consumed)
- **And** a second create for `resourceId="res-consume"` is consumed
- **Then** SP MUST receive the second create (entry was consumed by the first)

---

### IT-RTE-110: Deny list LRU eviction observable behavior

- **Validates AC:** AC-RTE-075
- **Test Infrastructure:** `AGENT_DENY_LIST_MAX_SIZE` set to small value; NATS; Ready SP
- **Given** deny list is filled to capacity, oldest entry being `res-oldest`
- **When** a new cancel arrives for `res-newest` (evicting oldest)
- **And** creates are published for both `res-oldest` and `res-newest`
- **Then** SP MUST receive create for `res-oldest` (evicted, no longer filtered)
- **And** SP MUST NOT receive create for `res-newest` (still denied)

---

### IT-RTE-120: Cancel for request in retry topic

- **Validates AC:** AC-RTE-080
- **Test Infrastructure:** Unhealthy SP; retry topic; cancel; then SP Ready
- **Given** create for `resourceId="res-789"` is in retry topic
- **And** cancel for `res-789` is consumed (deny list updated)
- **When** SP transitions to Ready and retry topic is processed
- **Then** SP MUST NOT receive create for `res-789`
- **And** `dcm.agents.responses` MUST receive `dcm.agent.cancel-acknowledged`

---

### IT-RTE-130: Cancel rejected for in-flight provisioning

- **Validates AC:** AC-RTE-090
- **Test Infrastructure:** Ready SP; cancel arrives after successful forward
- **Given** create for `resourceId="res-101"` was forwarded and acknowledged
- **When** cancel for `res-101` arrives on cancel topic
- **Then** `dcm.agents.responses` MUST receive `dcm.agent.cancel-rejected` with a `reason`
- **And** the CE data MUST include `resourceId`, `agentName`, and `topicName`
- **And** `res-101` MUST NOT be added to the deny list

---

## Topic 9: Retry & Cancel Mechanisms

### IT-RCM-010: SP recovers — retry topic processed

- **Validates AC:** AC-RCM-010
- **Test Infrastructure:** NATS; retry topic with 3 creates; SP transitions to Ready
- **Given** retry topic contains creates for `res-a`, `res-b`, `res-c` (serviceType="database")
- **And** SP for "database" is Unhealthy
- **When** SP transitions to Ready
- **Then** SP MUST receive creates for all three resources
- **And** `dcm.agents.responses` MUST receive three `dcm.agent.creation-acknowledged` events
- **And** forwarded payloads MUST be original CE bytes (no wrapping)

---

### IT-RCM-020: SP becomes Unavailable — held requests rejected

- **Validates AC:** AC-RCM-020
- **Test Infrastructure:** NATS; retry topic with 2 creates; SP becomes Unavailable
- **Given** retry topic contains creates for `res-d1`, `res-d2` (serviceType="database")
- **When** SP transitions to Unavailable
- **Then** `dcm.agents.responses` MUST receive two `dcm.agent.error` events
- **And** SP MUST NOT receive calls for them

---

### IT-RCM-030: Mixed service types — reject Unavailable, requeue Unhealthy

- **Validates AC:** AC-RCM-030
- **Test Infrastructure:** Two SPs; retry topic with mixed types; responses subscriber
- **Given** retry topic has create for "database" (SP Unhealthy) and "container" (SP Unavailable)
- **When** retry processing triggers
- **Then** "container" request MUST be rejected with error CE
- **And** "database" request MUST be re-published to retry topic
- **And** no additional `dcm.agent.request-queued` CEs MUST be published for "database"

---

### IT-RCM-040: FIFO ordering per service type

- **Validates AC:** AC-RCM-040
- **Test Infrastructure:** SP mock recording order; retry topic seeded A→B→C
- **Given** retry topic contains creates for "database" in order: A, B, C
- **When** SP recovers to Ready
- **Then** SP invocation order MUST be A, B, C

---

### IT-RCM-050: Restart re-reads retry without extra request-queued CEs

- **Validates AC:** AC-RCM-045
- **Test Infrastructure:** JetStream; SP remains Unhealthy; restart agent
- **Given** retry topic has messages from prior session; initial `request-queued` CEs already published
- **When** the agent restarts
- **Then** no additional `dcm.agent.request-queued` CEs MUST be published
- **And** messages MUST remain held until Ready/Unavailable transition

---

### IT-RCM-060: Creation-deletion dedup in retry topic

- **Validates AC:** AC-RCM-050
- **Test Infrastructure:** Retry topic with both create and delete for same resource
- **Given** retry topic contains `dcm.request.create` and `dcm.request.delete` for `resourceId="res-123"`
- **When** retry topic is processed
- **Then** neither MUST be forwarded to SP
- **And** `dcm.agents.responses` MUST receive `dcm.agent.deletion-acknowledged` with `status="DELETED"`
- **And** both messages MUST be acknowledged
- **And** the dedup MUST be logged

---

### IT-RCM-070: Cancel topic drained at startup

- **Validates AC:** AC-RCM-060
- **Test Infrastructure:** JetStream; cancel topic seeded; main topic with matching creates; Ready SP
- **Given** cancel topic has cancels for `res-456` and `res-789`
- **And** main topic has creates for both
- **When** the agent starts
- **Then** both cancels MUST be consumed before creates are forwarded
- **And** SP MUST NOT receive creates for `res-456` or `res-789`

---

## Cross-Cutting: Error Handling

### IT-XC-ERR-010: RFC 7807 compliance across error conditions

- **Validates AC:** AC-XC-ERR-010
- **Test Infrastructure:** Real HTTP server
- **Given** any error condition in the API (404, 409, 422, 500)
- **When** the error response is returned
- **Then** the body MUST contain at minimum `type` and `title` fields
- **And** SHOULD include `detail` and `instance` (= request URI)

---

### IT-XC-ERR-020: Error content type

- **Validates AC:** AC-XC-ERR-020
- **Test Infrastructure:** Real HTTP server
- **Given** any error response is returned
- **When** the headers are inspected
- **Then** `Content-Type` MUST be `application/problem+json`

---

### IT-XC-ERR-030: No implementation detail leakage

- **Validates AC:** AC-XC-ERR-030
- **Test Infrastructure:** Real HTTP server, trigger internal error
- **Given** an internal error occurs (e.g., via fault injection)
- **When** the HTTP 500 response is returned
- **Then** the `detail` MUST be a static non-revealing message
- **And** the response MUST NOT contain stack traces, file paths, or internal error messages

---

## Cross-Cutting: CloudEvent Definitions

### IT-XC-CE-010: CloudEvent v1.0 compliance

- **Validates AC:** AC-XC-CE-010
- **Test Infrastructure:** Any routing path producing a response CE; NATS subscriber
- **Given** the agent publishes any CloudEvent
- **When** the event is inspected
- **Then** it MUST include `id` (unique), `source`, `type`, `time` (RFC 3339), `specversion="1.0"`

---

### IT-XC-CE-020: Source attribute format

- **Validates AC:** AC-XC-CE-020
- **Test Infrastructure:** Mock DCM returning `agentId="agent-123"`; responses subscriber
- **Given** the agent registered with `agentId="agent-123"`
- **When** any agent-originated CloudEvent is published
- **Then** `source` MUST equal `"dcm/agents/agent-123"`

---

## Cross-Cutting: Logging

### IT-XC-LOG-010: Structured JSON logging

- **Validates AC:** AC-XC-LOG-010
- **Test Infrastructure:** Real HTTP server, log capture
- **Given** any operation occurs
- **When** the operation is logged
- **Then** log output MUST be JSON with fields: `time` (RFC 3339), `level`, `msg`

---

### IT-XC-LOG-020: Log levels follow convention

- **Validates AC:** AC-XC-LOG-020
- **Test Infrastructure:** Log capture across various scenarios
- **Given** various operations occur (error, warning, lifecycle events)
- **When** logs are inspected
- **Then** unrecoverable failures MUST use ERROR level
- **And** recoverable issues (retries, SP health transitions) MUST use WARN
- **And** lifecycle events (startup, SP registration) MUST use INFO

---

## Cross-Cutting: Configuration Management

### IT-XC-CFG-010: Environment variable configuration

- **Validates AC:** AC-XC-CFG-010
- **Test Infrastructure:** Agent startup with env vars
- **Given** `AGENT_SERVER_ADDRESS=:9090` is set
- **When** the agent starts
- **Then** the server MUST listen on port 9090

---

### IT-XC-CFG-020: Configuration file loading

- **Validates AC:** AC-XC-CFG-011
- **Test Infrastructure:** Configuration file, no env var override
- **Given** a config file provides `server.address=:9090`
- **And** `AGENT_SERVER_ADDRESS` is not set
- **When** the agent starts
- **Then** the server MUST listen on port 9090

---

### IT-XC-CFG-030: Environment variable takes precedence over file

- **Validates AC:** AC-XC-CFG-012
- **Test Infrastructure:** Config file + env var both set
- **Given** config file provides `server.address=:9090`
- **And** `AGENT_SERVER_ADDRESS=:7070` is set
- **When** the agent starts
- **Then** the server MUST listen on port 7070

---

### IT-XC-CFG-040: Fail-fast on missing required config

- **Validates AC:** AC-XC-CFG-020
- **Test Infrastructure:** Agent startup without required config
- **Given** `AGENT_NAME` is absent or empty
- **When** the agent starts
- **Then** the agent MUST exit before starting the HTTP server
- **And** MUST log an error identifying the missing field

---

### IT-XC-CFG-050: Fail-fast on invalid AGENT_COST

- **Validates AC:** AC-XC-CFG-030
- **Test Infrastructure:** Agent startup with invalid cost
- **Given** `AGENT_COST="expensive"`
- **When** the agent starts
- **Then** the agent MUST exit before starting any subsystem
- **And** MUST log an error identifying the invalid value

---

### IT-XC-CFG-060: Invalid config range fails fast

- **Validates AC:** AC-XC-CFG-050
- **Test Infrastructure:** Agent startup with out-of-range value
- **Given** `AGENT_HEALTH_CHECK_INTERVAL=0ms` (below minimum 1s)
- **When** the agent starts
- **Then** the agent MUST exit with an error identifying the invalid value and valid range

---

## Traceability Matrix

| AC ID | IT ID(s) |
|-------|----------|
| AC-HTTP-010 | IT-HTTP-010 |
| AC-HTTP-020 | IT-HTTP-020 |
| AC-HTTP-030 | IT-HTTP-030, IT-HTTP-050 |
| AC-HTTP-040 | IT-HTTP-040 |
| AC-HTTP-050 | IT-HTTP-060 |
| AC-HTTP-060 | IT-HTTP-070 |
| AC-HTTP-070 | IT-HTTP-080 |
| AC-HTTP-080 | IT-HTTP-030, IT-HTTP-090 |
| AC-HTTP-090 | IT-HTTP-100 |
| AC-HTTP-091 | IT-HTTP-110 |
| AC-HTTP-095 | IT-HTTP-120 |
| AC-HLT-010 | IT-HLT-010 |
| AC-HLT-020 | IT-HLT-020 |
| AC-HLT-030 | IT-HLT-030 |
| AC-HLT-040 | IT-HLT-010 |
| AC-HLT-050 | IT-HLT-040 |
| AC-SPR-010 | IT-SPR-010 |
| AC-SPR-020 | IT-SPR-020 |
| AC-SPR-030 | IT-SPR-030 |
| AC-SPR-040 | IT-SPR-040 |
| AC-SPR-050 | IT-SPR-050 |
| AC-SPR-060 | IT-SPR-060 |
| AC-SPR-070 | IT-SPR-070 |
| AC-SPR-090 | IT-SPR-080 |
| AC-SPR-095 | IT-SPR-090 |
| AC-SPR-096 | IT-SPR-100 |
| AC-SPR-100 | IT-SPR-110 |
| AC-SPR-105 | IT-SPR-120 |
| AC-SPR-106 | IT-SPR-130 |
| AC-SPR-106b | IT-SPR-140 |
| AC-SPR-107 | IT-SPR-150 |
| AC-SPR-108 | IT-SPR-160 |
| AC-SPR-108b | IT-SPR-165 |
| AC-SPR-109 | IT-SPR-170 |
| AC-SPR-110 | IT-SPR-180 |
| AC-STS-010 | IT-STS-010 |
| AC-STS-020 | IT-STS-020 |
| AC-STS-022 | IT-STS-030 |
| AC-STS-025 | IT-STS-040 |
| AC-STS-026 | IT-STS-050 |
| AC-STS-030 | IT-STS-060 |
| AC-STS-035 | IT-STS-070 |
| AC-HMN-005 | IT-HMN-050 |
| AC-HMN-010 | IT-HMN-010 |
| AC-HMN-015 | IT-HMN-060 |
| AC-HMN-020 | IT-HMN-020 |
| AC-HMN-030 | IT-HMN-030 |
| AC-HMN-040 | IT-HMN-040 |
| AC-HMN-050 | IT-HMN-100 |
| AC-HMN-051 | IT-HMN-070 |
| AC-HMN-052 | IT-HMN-080 |
| AC-HMN-053 | IT-HMN-090 |
| AC-HMN-060 | IT-HMN-110 |
| AC-HMN-070 | IT-HMN-120 |
| AC-HMN-080 | IT-HMN-130 |
| AC-HMN-100 | IT-HMN-140 |
| AC-HMN-110 | IT-HMN-150 |
| AC-HMN-120 | IT-HMN-160 |
| AC-HMN-185 | IT-HMN-170 |
| AC-DCM-010 | IT-DCM-010 |
| AC-DCM-015 | IT-DCM-020 |
| AC-DCM-020 | IT-DCM-030 |
| AC-DCM-025 | IT-DCM-040 |
| AC-DCM-030 | IT-DCM-050 |
| AC-DCM-035 | IT-DCM-060 |
| AC-DCM-040 | IT-DCM-070 |
| AC-DCM-050 | IT-DCM-080 |
| AC-DCM-060 | IT-DCM-090 |
| AC-DCM-061 | IT-DCM-100 |
| AC-DCM-070 | IT-DCM-110 |
| AC-DCM-080 | IT-DCM-120 |
| AC-DCM-085 | IT-DCM-130 |
| AC-DCM-090 | IT-DCM-140 |
| AC-DCM-095 | IT-DCM-150 |
| AC-DCM-100 | IT-DCM-160 |
| AC-DCM-105 | IT-DCM-170 |
| AC-MSG-010 | IT-MSG-010 |
| AC-MSG-015 | IT-MSG-020 |
| AC-MSG-016 | IT-MSG-030 |
| AC-MSG-018 | IT-MSG-040 |
| AC-MSG-020 | IT-MSG-050 |
| AC-MSG-025 | IT-MSG-060 |
| AC-MSG-030 | IT-MSG-070 |
| AC-MSG-035 | IT-MSG-080 |
| AC-MSG-040 | IT-MSG-090 |
| AC-MSG-050 | IT-MSG-100 |
| AC-MSG-055 | IT-MSG-110 |
| AC-MSG-060 | IT-MSG-120 |
| AC-RTE-010 | IT-RTE-010, IT-RTE-015 |
| AC-RTE-020 | IT-RTE-020 |
| AC-RTE-030 | IT-RTE-030 |
| AC-RTE-040 | IT-RTE-040 |
| AC-RTE-045 | IT-RTE-050 |
| AC-RTE-050 | IT-RTE-060 |
| AC-RTE-055 | IT-RTE-070 |
| AC-RTE-060 | IT-RTE-080 |
| AC-RTE-065 | IT-RTE-090 |
| AC-RTE-070 | IT-RTE-100 |
| AC-RTE-076 | IT-RTE-105 |
| AC-RTE-075 | IT-RTE-110 |
| AC-RTE-080 | IT-RTE-120 |
| AC-RTE-090 | IT-RTE-130 |
| AC-RCM-010 | IT-RCM-010 |
| AC-RCM-020 | IT-RCM-020 |
| AC-RCM-030 | IT-RCM-030 |
| AC-RCM-040 | IT-RCM-040 |
| AC-RCM-045 | IT-RCM-050 |
| AC-RCM-050 | IT-RCM-060 |
| AC-RCM-060 | IT-RCM-070 |
| AC-XC-ERR-010 | IT-XC-ERR-010 |
| AC-XC-ERR-020 | IT-XC-ERR-020 |
| AC-XC-ERR-030 | IT-XC-ERR-030 |
| AC-XC-CE-010 | IT-XC-CE-010 |
| AC-XC-CE-020 | IT-XC-CE-020 |
| AC-XC-LOG-010 | IT-XC-LOG-010 |
| AC-XC-LOG-020 | IT-XC-LOG-020 |
| AC-XC-CFG-010 | IT-XC-CFG-010 |
| AC-XC-CFG-011 | IT-XC-CFG-020 |
| AC-XC-CFG-012 | IT-XC-CFG-030 |
| AC-XC-CFG-020 | IT-XC-CFG-040 |
| AC-XC-CFG-030 | IT-XC-CFG-050 |
| AC-XC-CFG-050 | IT-XC-CFG-060 |
