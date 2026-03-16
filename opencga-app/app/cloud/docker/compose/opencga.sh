#!/usr/bin/env bash
#
# OpenCGA CLI wrapper — auto-login as the owner user
#
# Usage:
#   ./opencga.sh                     # Interactive mode
#   ./opencga.sh jobs top            # Run a single command
#   ./opencga.sh studies search      # Run a single command
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_HOME="${OPENCGA_LOCAL_HOME:-${HOME}/.opencga/instances/local}"
ENV_FILE="${DATA_HOME}/.env"

# Load .env (must exist — run deploy.sh first to generate it)
if [ ! -f "${ENV_FILE}" ]; then
    echo "ERROR: ${ENV_FILE} not found. Run './deploy.sh up' first to generate it."
    exit 1
fi
set -a
source "${ENV_FILE}"
set +a

# Compute OPENCGA_IMAGE if not explicitly set
if [ -z "${OPENCGA_IMAGE:-}" ]; then
    export OPENCGA_IMAGE="${OPENCGA_DOCKER_ORG:-opencb}/opencga-base:${OPENCGA_VERSION:-latest}"
fi

# Export for docker-compose.yml interpolation
export OPENCGA_LOCAL_HOME="${DATA_HOME}"
export OPENCGA_LOCAL_SCRIPTS="${SCRIPT_DIR}/scripts"

# Derive project name from instance directory
INSTANCE_NAME="$(basename "${DATA_HOME}")"
COMPOSE_PROJECT="opencga-${INSTANCE_NAME}"

dc() {
    docker compose -p "${COMPOSE_PROJECT}" --env-file /dev/null -f "${SCRIPT_DIR}/docker-compose.yml" "$@"
}

if [ $# -eq 0 ]; then
    # Interactive mode: login + drop to bash
    dc run --rm --no-deps \
        -e OPENCGA_OWNER_ID="${OPENCGA_OWNER_ID}" \
        -e OPENCGA_OWNER_PASSWORD="${OPENCGA_OWNER_PASSWORD}" \
        -e OPENCGA_ORG_ID="${OPENCGA_ORG_ID}" \
        --entrypoint bash \
        opencga-rest -c "
            echo \"\${OPENCGA_OWNER_PASSWORD}\" | /opt/opencga/bin/opencga.sh users login -u \"\${OPENCGA_OWNER_ID}\" -p --organization \"\${OPENCGA_ORG_ID}\" >/dev/null 2>&1
            echo \"Logged in as \${OPENCGA_OWNER_ID} (org: \${OPENCGA_ORG_ID})\"
            export PS1='opencga> '
            exec bash --norc
        "
else
    # Build a properly escaped command string from all arguments
    cmd=""
    for arg in "$@"; do
        if [[ "$arg" =~ [[:space:]\'\"\$\\\`] ]]; then
            cmd+=" '${arg//\'/\'\\\'\'}'"
        else
            cmd+=" $arg"
        fi
    done
    # Single command mode: login + run opencga.sh with args
    dc run --rm --no-deps \
        -e OPENCGA_OWNER_ID="${OPENCGA_OWNER_ID}" \
        -e OPENCGA_OWNER_PASSWORD="${OPENCGA_OWNER_PASSWORD}" \
        -e OPENCGA_ORG_ID="${OPENCGA_ORG_ID}" \
        --entrypoint bash \
        opencga-rest -c "
            echo \"\${OPENCGA_OWNER_PASSWORD}\" | /opt/opencga/bin/opencga.sh users login -u \"\${OPENCGA_OWNER_ID}\" -p --organization \"\${OPENCGA_ORG_ID}\" >/dev/null 2>&1
            /opt/opencga/bin/opencga.sh${cmd}
        "
fi
