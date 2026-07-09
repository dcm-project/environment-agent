# Contributing

Thank you for your interest in contributing to the DCM Environment Agent.

## Getting Started

1. Read the [DCM Enhancements](https://dcm-project.github.io/docs/enhancements/) for context
2. Fork the repository
3. Clone your fork and create a branch for your changes
4. Make your changes and ensure tests pass
5. Submit a pull request

## Development

```bash
make build      # Build the binary
make test       # Run unit tests
make fmt        # Format code
make vet        # Run go vet
make lint       # Run golangci-lint
```

## API Changes

This project uses OpenAPI-first development. To modify the API:

1. Edit `api/v1alpha1/openapi.yaml`
2. Run `make generate-api` to regenerate code
3. Run `make check-aep` to validate AEP compliance

Never edit generated files (`*.gen.go`) directly.

## Pull Requests

- All commits must be signed off (DCO): `git commit -s`
- PRs require approval from a code owner
- CI must pass before merging

## Code of Conduct

Be respectful and constructive in all interactions.
