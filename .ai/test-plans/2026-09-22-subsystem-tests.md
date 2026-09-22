# Subsystem Test Plan: Environment Agent

## Overview

Subsystem tests exercise the full agent **binary**, built and run as a real
container, against real external dependencies (a real Keycloak container, a
real NATS container) via `docker-compose` — not embedded fakes or in-process
mocks. They are Go-build-tag-gated (`-tags=subsystem`, package
`subsystem_test`, distinct from the `integration`/`unit` Ginkgo `Label`s used
elsewhere in this repo) and run by their own CI workflow
(`.github/workflows/subsystem.yaml`), separate from `make test`/`make ci`,
mirroring control-plane's `test/subsystem` pattern.

## Test Infrastructure Assumptions

| Dependency | Approach |
|------------|----------|
| Environment Agent | Real container, built from the repo `Containerfile`, `AGENT_AUTH_DISABLED=false` |
| Keycloak | Real container (`quay.io/keycloak/keycloak:26.0.1`), realm imported from `test/subsystem/auth/keycloak/realm-export.json` |
| NATS | Real container (`docker.io/library/nats:2-alpine`) — required because `AGENT_MESSAGING_URL` is non-optional config |

## Topic: Auth (real Keycloak)

### ST-AUTH-010: Valid Keycloak-issued token accepted end-to-end

- **Validates AC:** AC-AUTH-130, AC-AUTH-010
- **Test Infrastructure:** Full stack per above
- **Given** the agent is running against the real Keycloak container with auth enabled
- **When** `GET /api/v1alpha1/providers` is sent with a client_credentials access token from
  the `environment-agent` Keycloak client (whose `aud` matches `AGENT_AUTH_JWT_AUDIENCE`)
- **Then** the response MUST be HTTP 200

---

### ST-AUTH-020: Missing Bearer token rejected end-to-end

- **Validates AC:** AC-AUTH-130, AC-AUTH-030
- **Test Infrastructure:** Full stack per above
- **Given** the agent is running against the real Keycloak container with auth enabled
- **When** `GET /api/v1alpha1/providers` is sent with no `Authorization` header
- **Then** the response MUST be HTTP 401 with an RFC 7807 body (`type: UNAUTHORIZED`,
  `detail: "invalid Bearer token"`) and a `WWW-Authenticate: Bearer` header

---

### ST-AUTH-030: Tampered token rejected end-to-end

- **Validates AC:** AC-AUTH-130, AC-AUTH-040
- **Test Infrastructure:** Full stack per above
- **Given** a genuine token issued by the real Keycloak container
- **When** the token's signature segment is mutated and sent as the Bearer token to
  `GET /api/v1alpha1/providers`
- **Then** the response MUST be HTTP 401

---

### ST-AUTH-040: Wrong-audience token rejected end-to-end

- **Validates AC:** AC-AUTH-130
- **Test Infrastructure:** Full stack per above, `environment-agent-no-audience` Keycloak client
  (no audience mapper configured)
- **Given** `AGENT_AUTH_JWT_AUDIENCE=environment-agent-api` is configured on the agent
- **When** `GET /api/v1alpha1/providers` is sent with a validly-signed client_credentials token
  from `environment-agent-no-audience` (no `aud` claim at all)
- **Then** the response MUST be HTTP 401
