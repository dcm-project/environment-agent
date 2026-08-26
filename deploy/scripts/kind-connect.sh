#!/usr/bin/env bash
# Connect the Kind control-plane node to a compose network so agent containers can reach the API server.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=kind-env.sh
source "${SCRIPT_DIR}/kind-env.sh"

COMPOSE_NETWORK="${COMPOSE_NETWORK:-environment-agent_default}"
ALIAS="${KIND_NETWORK_ALIAS:-kubernetes}"

kind_pick_engine "${KIND_NODE}" "${COMPOSE_NETWORK}"

if "${CONTAINER_ENGINE}" inspect "${KIND_NODE}" --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' \
	| grep -qw "${COMPOSE_NETWORK}"; then
	echo "Kind node already connected to '${COMPOSE_NETWORK}'."
	exit 0
fi

echo "Connecting ${KIND_NODE} on ${CONTAINER_ENGINE} to ${COMPOSE_NETWORK} (alias: ${ALIAS})"
"${CONTAINER_ENGINE}" network connect --alias "${ALIAS}" "${COMPOSE_NETWORK}" "${KIND_NODE}"
echo "Done."
