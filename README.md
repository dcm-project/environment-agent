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

See [deploy/DEPLOY.md](deploy/DEPLOY.md) for full setup. Requires Kind with a running cluster:

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

## API Endpoints

| Method | Endpoint                              | Description                         |
|--------|---------------------------------------|-------------------------------------|
| GET    | /api/v1alpha1/health                  | Agent health check                  |
| GET    | /api/v1alpha1/providers               | List all SPs (includes health)      |
| POST   | /api/v1alpha1/providers               | External SP registration            |
| GET    | /api/v1alpha1/providers/{provider_id} | Get a single SP by ID               |

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.
