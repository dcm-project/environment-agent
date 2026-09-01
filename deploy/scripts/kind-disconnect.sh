#!/usr/bin/env bash
# Disconnect Kind from compose networks before compose-down (mirrors control-plane deploy).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=kind-env.sh
source "${SCRIPT_DIR}/kind-env.sh"
if ! kind_resolve_from_context; then
	exit 0
fi

if [[ -n "${COMPOSE_NETWORK:-}" ]]; then
	NETWORKS="${COMPOSE_NETWORK}"
else
	NETWORKS="${COMPOSE_NETWORKS:-environment-agent_default control-plane_default deploy_default}"
fi

pick_engine_for_node() {
	if [[ -n "${CONTAINER_ENGINE:-}" ]]; then
		if "${CONTAINER_ENGINE}" inspect "${KIND_NODE}" >/dev/null 2>&1; then
			return 0
		fi
		return 1
	fi
	for candidate in podman docker; do
		if command -v "${candidate}" >/dev/null 2>&1 \
			&& "${candidate}" inspect "${KIND_NODE}" >/dev/null 2>&1; then
			CONTAINER_ENGINE="${candidate}"
			return 0
		fi
	done
	return 1
}

if ! pick_engine_for_node; then
	exit 0
fi

for network in ${NETWORKS}; do
	if "${CONTAINER_ENGINE}" network inspect "${network}" >/dev/null 2>&1; then
		if "${CONTAINER_ENGINE}" inspect "${KIND_NODE}" --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' \
			| grep -qw "${network}"; then
			echo "Disconnecting ${KIND_NODE} from ${network}"
			"${CONTAINER_ENGINE}" network disconnect -f "${network}" "${KIND_NODE}" 2>/dev/null || true
		fi
	fi
done
