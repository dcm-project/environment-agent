# DCM Environment Agent

The Environment Agent is a lightweight process that runs in a target
environment, acting as the intermediary between DCM and the Service Providers
deployed in that environment.

It registers the environment to DCM, consumes resource operation requests from
a messaging system (NATS), and routes them to the appropriate Service Provider.

## Architecture

The agent supports a hybrid SP model:

- **Embedded SPs:** SP code shipped within the agent binary (K8s Container, ACM
  Cluster, KubeVirt), enabled via configuration.
- **External SPs:** Standalone SP processes that register to the agent via the
  REST API (`POST /api/v1alpha1/providers`).

Only one SP — embedded or external — may serve a given service type per agent.

For the full design, see the
[Environment Agent enhancement](https://github.com/dcm-project/enhancements/blob/main/enhancements/environment-agent/environment-agent.md).

## Development

### Prerequisites

- Go 1.25.5+
- [golangci-lint](https://golangci-lint.run/)
- [Spectral](https://stoplight.io/open-source/spectral) (for AEP compliance
  checks)

### Build and Run

```bash
make build      # Build the binary
make run        # Run the agent
make test       # Run unit tests
make lint       # Run golangci-lint
make fmt        # Format code
make vet        # Run go vet
```

### API Development

This project uses OpenAPI-first development. To modify the API:

1. Edit `api/v1alpha1/openapi.yaml`
2. Run `make generate-api` to regenerate code
3. Run `make check-aep` to validate AEP compliance

Never edit generated files (`*.gen.go`) directly.

### Container Image

```bash
make image-build   # Build container image using podman/docker
```

### Local deployment (Kind + Compose)

See [deploy/DEPLOY.md](deploy/DEPLOY.md) for full setup. This requires Kind with a running cluster and the
[utilities](https://github.com/dcm-project/utilities) repo as a sibling directory (`../utilities`):

```bash
cp deploy/.env.example deploy/.env
make install-kubevirt          # when vm is in AGENT_EMBEDDED_SPS
make kubeconfig-for-compose
make compose-up
make kind-connect
make deploy-verify
```

For integration with the control-plane stack, see
[control-plane deploy/docs/environment-agent-kind.md](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md).

To run the agent on the same cluster as embedded SP workloads, see
[deploy/docs/in-cluster.md](deploy/docs/in-cluster.md) (`make k8s-deploy` on Kind).

## Control Plane Authentication

The agent authenticates outbound HTTP requests (registration and heartbeat) to
the DCM control plane when the control plane has authentication enabled. Two
modes are available; when neither is configured, requests are sent without an
`Authorization` header (backward-compatible default).

See the DCM [authentication user guide](https://dcm-project.github.io/docs/getting-started/authentication/)
([dcm-project.github.io#22](https://github.com/dcm-project/dcm-project.github.io/pull/22))
for the control plane's OIDC/JWT behavior, issuer/audience configuration, and
the reference Keycloak stack used by `make compose-up`.

### Mode 1: OAuth2 Client Credentials (recommended for production)

The agent obtains short-lived JWTs from an OIDC token endpoint using the
`client_credentials` grant, and refreshes them automatically before expiry.

| Variable | Description |
|---|---|
| `DCM_AUTH_TOKEN_ENDPOINT` | OIDC token endpoint URL (e.g. `https://keycloak:8443/realms/dcm/protocol/openid-connect/token`) |
| `DCM_AUTH_CLIENT_ID` | OAuth2 client ID for the agent's service account |
| `DCM_AUTH_CLIENT_SECRET` | OAuth2 client secret |

> **Caution:** Using `http://` for `DCM_AUTH_TOKEN_ENDPOINT` sends the client secret unencrypted
> on every token request. The agent logs a startup warning in this case; prefer `https://` in any
> non-local deployment. The agent also refuses to follow any HTTP redirect returned by the token
> endpoint, so the client secret is never resent to a different origin.

All three variables must be set together. Partial configuration (e.g. endpoint
without client ID) causes a startup-fatal error identifying the missing fields.

Tokens are cached in memory and refreshed proactively (with a 10-second safety
buffer before the `expires_in` deadline). Token fetch failures do not crash the
agent — the affected registration or heartbeat call fails and is retried by the
existing backoff logic.

#### Keycloak Setup

1. Create a service-account client in the DCM realm (e.g. `environment-agent`).
2. Enable **Client authentication** and **Service accounts roles**.
3. Assign the role(s) the control plane expects for agent registration.
4. Attach an audience mapper for `aud=dcm-api` to the client (see caveat below).
5. Set the three `DCM_AUTH_*` variables to the client's credentials.

> **Known gap — audience mapper missing for the agent client:** The control
> plane requires `aud=dcm-api` on inbound tokens, but in the reference
> Keycloak realm the `dcm-api` audience mapper is shipped attached only to the
> `dcm-proxy` and `dcm-cli` clients, not to the agent's service-account client.
> Validating the full registration flow against that stock realm produces a
> token without the `dcm-api` audience, and the control plane rejects it with
> `401`. Until an equivalent mapper is added for the agent's client (or the
> realm's reference config is updated upstream), explicitly attach a
> `dcm-api` audience mapper to the client created above before relying on
> Mode 1 against the reference stack.

#### Kubernetes Secrets

Store the client secret in a Kubernetes Secret and inject it via `envFrom`:

```yaml
envFrom:
  - secretRef:
      name: environment-agent-auth
```

This avoids embedding secrets in pod specs or config maps.

### Mode 2: Static Bearer Token (dev / simple deployments)

| Variable | Description |
|---|---|
| `DCM_AUTH_TOKEN` | A pre-obtained JWT sent as-is on every request |

**Limitations:**

- No automatic refresh. When the token expires, the control plane returns 401,
  which is a non-retryable error — registration halts permanently until the
  agent is restarted with a fresh token.
- Intended for development or short-lived environments where token lifetime
  exceeds the agent's expected uptime.

### Config Precedence

If both modes are configured, client credentials takes precedence over the
static token. The resolution order is:

1. **Client Credentials** — `DCM_AUTH_TOKEN_ENDPOINT` + `DCM_AUTH_CLIENT_ID` +
   `DCM_AUTH_CLIENT_SECRET` all set
2. **Static Token** — `DCM_AUTH_TOKEN` set
3. **No Auth** — neither configured (no `Authorization` header)

## API Endpoints

| Method | Endpoint                              | Description                         |
|--------|---------------------------------------|-------------------------------------|
| GET    | /api/v1alpha1/health                  | Agent health check                  |
| GET    | /api/v1alpha1/providers               | List all SPs (includes health)      |
| POST   | /api/v1alpha1/providers               | External SP registration            |
| GET    | /api/v1alpha1/providers/{provider_id} | Get a single SP by ID               |

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.
