BINARY_NAME := environment-agent

# CONTAINER_ENGINE: container runtime command. Set to override; otherwise auto-detect podman or docker.
CONTAINER_ENGINE ?= $(shell \
	if command -v podman >/dev/null 2>&1; then \
		echo podman; \
	elif command -v docker >/dev/null 2>&1; then \
		echo docker; \
	fi)

# CONTAINER_IMAGE_NAME: FQDN (without tag) of the container image. Set to override.
CONTAINER_IMAGE_NAME ?= quay.io/dcm-project/${BINARY_NAME}

# CONTAINER_IMAGE_TAG: Container image tag. Set to override; otherwise git short hash is used.
CONTAINER_IMAGE_TAG ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

GOLANGCI_LINT_VERSION ?= v2.12.2

build:
	CGO_ENABLED=0 go build -buildvcs=false -o bin/$(BINARY_NAME) ./cmd/$(BINARY_NAME)

run:
	go run ./cmd/$(BINARY_NAME)

clean:
	rm -rf bin/

fmt:
	gofmt -s -w .

vet:
	go vet ./...

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

test:
	go run github.com/onsi/ginkgo/v2/ginkgo -r --randomize-all --fail-on-pending

tidy:
	go mod tidy

generate-types:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=api/v1alpha1/types.gen.cfg \
		-o api/v1alpha1/types.gen.go \
		api/v1alpha1/openapi.yaml

generate-spec:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=api/v1alpha1/spec.gen.cfg \
		-o api/v1alpha1/spec.gen.go \
		api/v1alpha1/openapi.yaml

generate-server:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=internal/api/server/server.gen.cfg \
		-o internal/api/server/server.gen.go \
		api/v1alpha1/openapi.yaml

generate-client:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=pkg/client/client.gen.cfg \
		-o pkg/client/client.gen.go \
		api/v1alpha1/openapi.yaml

generate-api: generate-types generate-spec generate-server generate-client

check-generate-api: generate-api
	git diff --exit-code api/ internal/api/server/ pkg/client/ || \
		(echo "Generated files out of sync. Run 'make generate-api'." && exit 1)

check-aep:
	npx --yes @stoplight/spectral-cli lint --fail-severity=warn ./api/v1alpha1/openapi.yaml

check-container-engine:
	@if [ -z "$(CONTAINER_ENGINE)" ]; then \
		echo "Error: No supported container engine found. Please install podman or docker, or set CONTAINER_ENGINE explicitly." >&2; \
		exit 1; \
	fi

image-build: check-container-engine
	$(CONTAINER_ENGINE) build -t $(CONTAINER_IMAGE_NAME):$(CONTAINER_IMAGE_TAG) .

.PHONY: build run clean fmt vet lint test tidy generate-types generate-spec generate-server generate-client generate-api check-generate-api check-aep check-container-engine image-build
