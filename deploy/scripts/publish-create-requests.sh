#!/usr/bin/env bash
# Publish sample container + VM dcm.request.create CloudEvents to NATS JetStream.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
	set -a
	# shellcheck disable=SC1091
	source .env
	set +a
fi

export AGENT_MESSAGING_URL="${AGENT_MESSAGING_URL:-nats://127.0.0.1:4222}"
FIXTURES_DIR="${DEPLOY_FIXTURES_DIR:-${ROOT}/deploy/fixtures}"
AGENT_URL="${AGENT_URL:-http://localhost:8081}"
AGENT_NAME="${AGENT_NAME:-local-agent}"
TOPIC_BASE="${AGENT_TOPIC_NAME:-$AGENT_NAME}"
SUBJECT="dcm.agent.${TOPIC_BASE}"
NATS_BOX_IMAGE="${NATS_BOX_IMAGE:-docker.io/natsio/nats-box:0.14.3}"

if ! command -v jq >/dev/null 2>&1; then
	echo "error: jq is required" >&2
	exit 1
fi

nats_cmd() {
	if command -v nats >/dev/null 2>&1; then
		nats --server "$AGENT_MESSAGING_URL" "$@"
	else
		if [[ -z "${CONTAINER_ENGINE:-}" ]]; then
			if command -v podman >/dev/null 2>&1; then
				CONTAINER_ENGINE=podman
			elif command -v docker >/dev/null 2>&1; then
				CONTAINER_ENGINE=docker
			else
				echo "error: install nats CLI or podman/docker for nats-box fallback" >&2
				exit 1
			fi
		fi
		"${CONTAINER_ENGINE}" run --rm -i --network host "$NATS_BOX_IMAGE" \
			nats --server "$AGENT_MESSAGING_URL" "$@"
	fi
}

publish_to_nats() {
	local subject="$1"
	local msg_file="$2"
	if command -v nats >/dev/null 2>&1; then
		nats --server "$AGENT_MESSAGING_URL" pub "$subject" "@${msg_file}"
	else
		if [[ -z "${CONTAINER_ENGINE:-}" ]]; then
			if command -v podman >/dev/null 2>&1; then
				CONTAINER_ENGINE=podman
			else
				CONTAINER_ENGINE=docker
			fi
		fi
		"${CONTAINER_ENGINE}" run --rm --network host \
			-v "${msg_file}:/msg:ro" "$NATS_BOX_IMAGE" \
			nats --server "$AGENT_MESSAGING_URL" pub "$subject" "@/msg"
	fi
}

publish_create() {
	local service_type="$1"
	local spec_file="$2"
	local resource_id="$3"

	if [[ ! -f "$spec_file" ]]; then
		echo "error: spec file not found: $spec_file" >&2
		exit 1
	fi

	local payload event tmp
	payload="$(jq -n \
		--arg rid "$resource_id" \
		--arg st "$service_type" \
		--slurpfile spec "$spec_file" \
		'{resource_id: $rid, service_type: $st, spec: $spec[0]}')"

	event="$(jq -n \
		--arg id "$(uuidgen)" \
		--arg time "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
		--arg subject "$SUBJECT" \
		--argjson data "$payload" \
		'{
			specversion: "1.0",
			id: $id,
			source: "dcm/control-plane",
			type: "dcm.request.create",
			subject: $subject,
			time: $time,
			data: $data
		}')"

	tmp="$(mktemp)"
	jq -c . <<<"$event" >"$tmp"
	publish_to_nats "$SUBJECT" "$tmp"
	rm -f "$tmp"

	echo "published ${service_type} create: resource_id=${resource_id} subject=${SUBJECT}"
}

echo "==> Ensuring JetStream stream dcm-agent-requests"
"${ROOT}/deploy/scripts/ensure-nats-stream.sh"

if ! curl -sf "${AGENT_URL}/api/v1alpha1/health" >/dev/null 2>&1; then
	echo "warning: agent not healthy at ${AGENT_URL} — start with: make compose-up" >&2
	echo "         (publish will still run; agent must be up to route requests)" >&2
fi

container_id="${CONTAINER_RESOURCE_ID:-local-container-$(uuidgen | tr -d '-' | cut -c1-8)}"
vm_id="${VM_RESOURCE_ID:-local-vm-$(uuidgen | tr -d '-' | cut -c1-8)}"

echo "==> Publishing to subject ${SUBJECT} (stream dcm-agent-requests)"
publish_create container "${FIXTURES_DIR}/container-create-spec.json" "$container_id"
publish_create vm "${FIXTURES_DIR}/vm-create-spec.json" "$vm_id"

echo "Done. Watch agent logs or cluster resources:"
echo "  kubectl get deploy,svc -l dcm.project/managed-by=dcm"
echo "  kubectl get virtualmachines -A -l dcm.project/managed-by=dcm"
