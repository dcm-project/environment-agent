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
	go run github.com/onsi/ginkgo/v2/ginkgo -r --randomize-all --fail-on-pending --skip-package=test/e2e

test-unit:
	go run github.com/onsi/ginkgo/v2/ginkgo -r --randomize-all --fail-on-pending --label-filter=unit ./internal/config ./internal/httperror ./internal/provider ./internal/health/monitor ./internal/backoff ./internal/dcm ./internal/messaging ./internal/cloudevent ./internal/routing ./cmd/environment-agent

test-integration:
	go run github.com/onsi/ginkgo/v2/ginkgo -r --randomize-all --fail-on-pending --label-filter=integration ./internal/apiserver ./internal/health ./internal/health/monitor ./internal/provider ./internal/dcm ./internal/messaging ./internal/routing

test-race:
	go run github.com/onsi/ginkgo/v2/ginkgo -r --race --randomize-all --fail-on-pending --skip-package=test/e2e

test-e2e:
	go run github.com/onsi/ginkgo/v2/ginkgo -r --randomize-all --fail-on-pending --tags=e2e ./test/e2e/...

test-all: test test-e2e

coverage:
	go run github.com/onsi/ginkgo/v2/ginkgo -r --randomize-all --fail-on-pending --cover --coverprofile=coverage.out ./internal/... ./cmd/...

ci: check-tidy check-generate-api vet lint test

tidy:
	go mod tidy

check-tidy:
	go mod tidy -diff

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

# Embedded SP OpenAPI contracts (generic capability paths, not SP package names).
generate-cluster-types:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=api/cluster/v1alpha1/types.gen.cfg \
		-o api/cluster/v1alpha1/types.gen.go \
		api/cluster/v1alpha1/openapi.yaml

generate-cluster-spec:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=api/cluster/v1alpha1/spec.gen.cfg \
		-o api/cluster/v1alpha1/spec.gen.go \
		api/cluster/v1alpha1/openapi.yaml

generate-cluster-api: generate-cluster-types generate-cluster-spec

generate-container-types:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=api/container/v1alpha1/types.gen.cfg \
		-o api/container/v1alpha1/types.gen.go \
		api/container/v1alpha1/openapi.yaml

generate-container-spec:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=api/container/v1alpha1/spec.gen.cfg \
		-o api/container/v1alpha1/spec.gen.go \
		api/container/v1alpha1/openapi.yaml

generate-container-api: generate-container-types generate-container-spec

generate-storage-types:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=api/storage/v1alpha1/types.gen.cfg \
		-o api/storage/v1alpha1/types.gen.go \
		api/storage/v1alpha1/openapi.yaml

generate-storage-spec:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=api/storage/v1alpha1/spec.gen.cfg \
		-o api/storage/v1alpha1/spec.gen.go \
		api/storage/v1alpha1/openapi.yaml

generate-storage-api: generate-storage-types generate-storage-spec

bundle-vm-openapi:
	@echo "Bundling VM OpenAPI specification..."
	@command -v redocly >/dev/null 2>&1 || { \
		echo "Error: Redocly CLI is required but not installed."; \
		echo "Install it with: npm install -g @redocly/cli"; \
		exit 1; \
	}
	redocly bundle api/vm/v1alpha1/openapi.source.yaml -o api/vm/v1alpha1/openapi.yaml
	@echo "VM OpenAPI spec bundled successfully"

generate-vm-types:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=api/vm/v1alpha1/types.gen.cfg \
		-o api/vm/v1alpha1/types.gen.go \
		api/vm/v1alpha1/openapi.yaml

generate-vm-spec:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=api/vm/v1alpha1/spec.gen.cfg \
		-o api/vm/v1alpha1/spec.gen.go \
		api/vm/v1alpha1/openapi.yaml

generate-vm-server:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen \
		--config=internal/openshift/kubevirtvm/oapi/server/server.gen.cfg \
		-o internal/openshift/kubevirtvm/oapi/server/server.gen.go \
		api/vm/v1alpha1/openapi.yaml

generate-vm-api: generate-vm-types generate-vm-spec generate-vm-server

generate-sp-api: generate-cluster-api generate-container-api generate-storage-api generate-vm-api

generate-api: generate-types generate-spec generate-server generate-client generate-sp-api

check-generate-api: generate-api
	git diff --exit-code api/ internal/api/server/ pkg/client/ \
		internal/openshift/kubevirtvm/oapi/server/ || \
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

.PHONY: build run clean fmt vet lint test test-unit test-integration test-race test-e2e test-all coverage ci tidy check-tidy \
	generate-types generate-spec generate-server generate-client \
	generate-cluster-types generate-cluster-spec generate-cluster-api \
	generate-container-types generate-container-spec generate-container-server generate-container-api \
	generate-storage-types generate-storage-spec generate-storage-api \
	bundle-vm-openapi generate-vm-types generate-vm-spec generate-vm-server generate-vm-api \
	generate-sp-api generate-api check-generate-api check-aep check-container-engine image-build
