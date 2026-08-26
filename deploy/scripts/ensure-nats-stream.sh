#!/usr/bin/env bash
# Ensure JetStream streams needed for local standalone dev exist.
# Full control-plane stack creates these on startup; agent-only compose needs this helper.
set -euo pipefail

NATS_URL="${AGENT_MESSAGING_URL:-nats://127.0.0.1:4222}"
NATS_BOX_IMAGE="${NATS_BOX_IMAGE:-docker.io/natsio/nats-box:0.14.3}"

REQUEST_STREAM_NAME="${REQUEST_STREAM_NAME:-dcm-agent-requests}"
STATUS_STREAM_NAME="${STATUS_STREAM_NAME:-dcm-status}"
RESPONSE_STREAM_NAME="${RESPONSE_STREAM_NAME:-dcm-agent-responses}"

detect_engine() {
	if [[ -n "${CONTAINER_ENGINE:-}" ]]; then
		return 0
	fi
	if command -v podman >/dev/null 2>&1; then
		CONTAINER_ENGINE=podman
	elif command -v docker >/dev/null 2>&1; then
		CONTAINER_ENGINE=docker
	else
		echo "error: install nats CLI or podman/docker for nats-box fallback" >&2
		exit 1
	fi
}

nats_cmd() {
	if command -v nats >/dev/null 2>&1; then
		nats --server "$NATS_URL" "$@"
	else
		detect_engine
		"${CONTAINER_ENGINE}" run --rm --network host "$NATS_BOX_IMAGE" \
			nats --server "$NATS_URL" "$@"
	fi
}

ensure_stream() {
	local name="$1"
	local subjects="$2"
	local retention="${3:-limits}"

	if nats_cmd stream info "$name" >/dev/null 2>&1; then
		echo "JetStream stream ${name} already exists"
		return 0
	fi

	echo "Creating JetStream stream ${name} (subjects: ${subjects})"
	nats_cmd stream add "$name" \
		--subjects "$subjects" \
		--storage file \
		--retention "$retention" \
		--discard old \
		--max-age 72h \
		--defaults
}

ensure_stream "$REQUEST_STREAM_NAME" "dcm.agent.>" limits
# VM (and other SP) status events publish to dcm.vm under dcm.* (control-plane default).
ensure_stream "$STATUS_STREAM_NAME" "dcm.*" limits
# Agent create/delete acks and errors (control-plane ResponseConsumer).
ensure_stream "$RESPONSE_STREAM_NAME" "dcm.agents.responses" workqueue
