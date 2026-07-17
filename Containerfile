# Build stage
FROM registry.access.redhat.com/ubi9/go-toolset:9.8 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

USER root
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -o environment-agent ./cmd/environment-agent

# Runtime stage
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest

WORKDIR /app

COPY --from=builder /app/environment-agent .

# OpenShift runs arbitrary UIDs in group 0; g+rwX keeps /app usable.
RUN chown -R 1001:0 /app && chmod -R g+rwX /app

USER 1001

EXPOSE 8080

ENTRYPOINT ["./environment-agent"]
