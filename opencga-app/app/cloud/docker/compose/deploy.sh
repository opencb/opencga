#!/usr/bin/env bash
#
# OpenCGA Local Docker Deployment
# Usage: ./deploy.sh [command] [options]
#
set -euo pipefail

# Colors (disabled if not a terminal)
if [ -t 1 ]; then
    C_BOLD='\033[1m'
    C_GREEN='\033[0;32m'
    C_CYAN='\033[0;36m'
    C_YELLOW='\033[0;33m'
    C_RED='\033[0;31m'
    C_DIM='\033[2m'
    C_RESET='\033[0m'
else
    C_BOLD='' C_GREEN='' C_CYAN='' C_YELLOW='' C_RED='' C_DIM='' C_RESET=''
fi

log_info()  { echo -e "${C_GREEN}[INFO]${C_RESET}  $*"; }
log_warn()  { echo -e "${C_YELLOW}[WARN]${C_RESET}  $*"; }
log_error() { echo -e "${C_RED}[ERROR]${C_RESET} $*"; }
log_step()  { echo -e "${C_CYAN}==>${C_RESET} ${C_BOLD}$*${C_RESET}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}" && git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "${PROJECT_ROOT}" ]; then
    log_error "Cannot detect project root (not inside a git repository)"
    exit 1
fi

# Base directory for all instances
OPENCGA_BASE="${HOME}/.opencga/instances"

# Pre-parse global options (--name / -n can appear anywhere in the arg list).
INSTANCE_NAME=""
_args=()
while [ $# -gt 0 ]; do
    case "$1" in
        --name|-n)
            if [ $# -lt 2 ] || [[ "$2" == -* ]]; then
                log_error "--name requires an instance name"
                exit 1
            fi
            INSTANCE_NAME="$2"; shift 2 ;;
        *)
            _args+=("$1"); shift ;;
    esac
done
set -- "${_args[@]}"

# Derive instance name: explicit --name > auto-detect from existing instances
if [ -z "${INSTANCE_NAME}" ]; then
    # Count existing instances
    _instances=()
    if [ -d "${OPENCGA_BASE}" ]; then
        for _d in "${OPENCGA_BASE}"/*/; do
            [ -d "$_d" ] && _instances+=("$(basename "$_d")")
        done
    fi
    case ${#_instances[@]} in
        0) INSTANCE_NAME="local" ;;
        1) INSTANCE_NAME="${_instances[0]}" ;;
        *)
            # Allow 'list' and 'help' without --name
            _cmd="${1:-help}"
            if [ "$_cmd" = "list" ] || [ "$_cmd" = "help" ] || [ "$_cmd" = "-h" ] || [ "$_cmd" = "--help" ]; then
                INSTANCE_NAME="${_instances[0]}"
            else
                log_error "Multiple instances found. Use --name to specify one: ${_instances[*]}"
                echo "       Run './deploy.sh list' to see all instances."
                exit 1
            fi
            ;;
    esac
fi
DATA_HOME="${OPENCGA_BASE}/${INSTANCE_NAME}"

CONF_SOURCE="${PROJECT_ROOT}/build/cloud/docker/compose/conf"
CONF_TARGET="${DATA_HOME}/conf"
DOCKERFILE="${PROJECT_ROOT}/build/cloud/docker/opencga-base/Dockerfile"
BUILD_CONTEXT="${PROJECT_ROOT}/build"

ENV_FILE="${DATA_HOME}/.env"
ENV_TEMPLATE="${SCRIPT_DIR}/.env.template"

# Export for docker-compose.yml interpolation
export OPENCGA_LOCAL_HOME="${DATA_HOME}"
export OPENCGA_LOCAL_SCRIPTS="${DATA_HOME}/scripts"

init_env() {
    # Ensure DATA_HOME directory exists
    mkdir -p "${DATA_HOME}"

    # If .env exists, source it (user may have edited it)
    if [ -f "${ENV_FILE}" ]; then
        set -a
        source "${ENV_FILE}"
        set +a
        return 0
    fi

    # Generate .env from template
    if [ ! -f "${ENV_TEMPLATE}" ]; then
        log_error ".env.template not found. Cannot generate .env."
        exit 1
    fi

    log_step "Generating .env from template"
    cp "${ENV_TEMPLATE}" "${ENV_FILE}"

    # Auto-detect OPENCGA_VERSION from pom.xml
    local pom_file="${PROJECT_ROOT}/pom.xml"
    if [ -f "${pom_file}" ]; then
        local version
        version="$(sed -n '/<artifactId>opencga<\/artifactId>/,/<version>/{ s/.*<version>\(.*\)<\/version>.*/\1/p; }' "${pom_file}" | head -1)"
        if [ -n "${version}" ]; then
            sed -i "s|^#OPENCGA_VERSION=.*|OPENCGA_VERSION=${version}|" "${ENV_FILE}"
        fi
    fi

    # Auto-detect Docker socket
    local docker_sock=""
    if [ -S "${XDG_RUNTIME_DIR:-}/docker.sock" ]; then
        docker_sock="${XDG_RUNTIME_DIR}/docker.sock"
    elif [ -S "$HOME/.docker/run/docker.sock" ]; then
        docker_sock="$HOME/.docker/run/docker.sock"
    elif [ -S /var/run/docker.sock ]; then
        docker_sock="/var/run/docker.sock"
    fi
    if [ -n "${docker_sock}" ]; then
        sed -i "s|^#DOCKER_SOCK=.*|DOCKER_SOCK=${docker_sock}|" "${ENV_FILE}"
    fi

    # Source the generated file
    set -a
    source "${ENV_FILE}"
    set +a
}

init_env

# Compute OPENCGA_IMAGE if not explicitly set
if [ -z "${OPENCGA_IMAGE:-}" ]; then
    export OPENCGA_IMAGE="${OPENCGA_DOCKER_ORG:-opencb}/opencga-base:${OPENCGA_VERSION:-latest}"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Memory helpers
# ─────────────────────────────────────────────────────────────────────────────

# Convert memory string (500m, 1g, 2.5g) to integer megabytes
to_mb() {
    local val="$1"
    case "${val}" in
        *[gG]) awk "BEGIN {printf \"%.0f\", ${val%[gG]} * 1024}" ;;
        *[mM]) echo "${val%[mM]}" ;;
        *)     echo "${val}" ;;
    esac
}

# Apply multiplier to a memory string, return result with 'm' suffix
apply_mem_multiplier() {
    local heap_mb
    heap_mb=$(to_mb "$1")
    local multiplier="$2"
    awk "BEGIN {printf \"%.0fm\", ${heap_mb} * ${multiplier}}"
}

# Compute and export derived memory env vars from heap settings
# (container mem_limit, JAVA_OPTS, HBASE_HEAPSIZE in MB)
export_heap_vars() {
    if [ -n "${OPENCGA_REST_HEAP:-}" ]; then
        export OPENCGA_REST_JAVA_OPTS="${OPENCGA_REST_JAVA_OPTS:--Xmx${OPENCGA_REST_HEAP}}"
        export OPENCGA_REST_MEM
        OPENCGA_REST_MEM=$(apply_mem_multiplier "${OPENCGA_REST_HEAP}" 1.3)
    fi
    if [ -n "${OPENCGA_MASTER_HEAP:-}" ]; then
        export OPENCGA_MASTER_JAVA_OPTS="${OPENCGA_MASTER_JAVA_OPTS:--Xmx${OPENCGA_MASTER_HEAP}}"
        # Container limit = (master_heap + job_heap) * 1.3 to account for JVM overhead + child processes
        local master_mb job_mb
        master_mb=$(to_mb "${OPENCGA_MASTER_HEAP}")
        job_mb=$(to_mb "${OPENCGA_JOB_HEAP:-1g}")
        export OPENCGA_MASTER_MEM
        OPENCGA_MASTER_MEM=$(awk "BEGIN {printf \"%.0fm\", ($master_mb + $job_mb) * 1.3}")
    fi
    if [ -n "${OPENCGA_HBASE_HEAP:-}" ]; then
        export OPENCGA_HBASE_MEM
        OPENCGA_HBASE_MEM=$(apply_mem_multiplier "${OPENCGA_HBASE_HEAP}" 1.3)
        export OPENCGA_HBASE_HEAP_MB
        OPENCGA_HBASE_HEAP_MB=$(to_mb "${OPENCGA_HBASE_HEAP}")
    fi
}

# Update a KEY=value in .env — logs only when the value actually changes
update_env() {
    local key="$1" val="$2"
    # Check current value (from env or .env file)
    local cur="${!key:-}"
    if [ "${cur}" = "${val}" ]; then
        return 0  # no change
    fi
    if [ -f "${ENV_FILE}" ]; then
        if grep -q "^${key}=" "${ENV_FILE}"; then
            sed -i "s|^${key}=.*|${key}=${val}|" "${ENV_FILE}"
        elif grep -q "^#${key}=" "${ENV_FILE}"; then
            sed -i "s|^#${key}=.*|${key}=${val}|" "${ENV_FILE}"
        else
            # Ensure file ends with a newline before appending
            [ -s "${ENV_FILE}" ] && [ "$(tail -c1 "${ENV_FILE}")" != "" ] && echo >> "${ENV_FILE}"
            echo "${key}=${val}" >> "${ENV_FILE}"
        fi
    fi
    if [ -n "${cur}" ]; then
        log_info "${key}: ${cur} -> ${val}"
    else
        log_info "${key}: ${val}"
    fi
    export "${key}=${val}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Functions
# ─────────────────────────────────────────────────────────────────────────────

usage() {
    cat <<EOF
Usage: ./deploy.sh [-n NAME] <command> [options]

Global options:
  -n, --name NAME   Instance name (default: local)
                Each instance is fully independent: config, data, volumes, containers.
                All instances live under ~/.opencga/instances/<name>/.
                Example: ./deploy.sh --name dev up --storage hadoop

Commands:
  up        Start all services (use --storage hadoop for HBase + Phoenix)
  down      Stop and remove all services
  restart   Restart all services (down + up)
  build     Build opencga-base Docker image from local Maven build output
  list      List all instances and their status
  load-demo Load demo data (project, study, VCF index jobs)
  cli       Open opencga.sh CLI (auto-login as owner)
  shell     Open interactive shell in opencga-base container
  mongosh   Open MongoDB shell
  status    Show service status
  top       Live resource usage (CPU, memory) for all containers
  health    Check health of all services
  info      Show deployment configuration and versions
  logs      Tail logs (pass service names as extra args)
  clean     Remove all data (data/ and conf/ directories)
  init-conf Generate conf/ from build config templates

Current instance: ${INSTANCE_NAME} (${DATA_HOME})

Run './deploy.sh <command> --help' for command-specific options.
EOF
}

usage_up() {
    cat <<'EOF'
Usage: ./deploy.sh up [options]

Start all services (MongoDB, Solr, OpenCGA REST, Master, IVA).

Options:
  --build       Build Docker image before starting
  --pull        Pull images before starting
  --regen-conf  Regenerate conf/ from build templates
  --load-demo   Load demo data after services are ready (for more options, use 'load-demo' command)
  --storage ENGINE  Storage engine: mongodb (default) or hadoop (HBase + Phoenix)
  --force       Skip running-jobs safety check

  Configuration (persisted to .env):
  --image [IMAGE]        Docker image to use (default: opencb/opencga-base:<version>)
                         If IMAGE is omitted, shows the current image.
                         Examples: --image myrepo/opencga:2.0.0
                                   --image opencb/opencga-base:latest
  --rest-heap SIZE       Java heap for REST server (e.g., 512m, 2g)
  --master-heap SIZE     Java heap for Master daemon (default: 400m)
  --job-heap SIZE        Java heap for child job processes (default: 1g)
  --hbase-heap SIZE      Java heap for HBase (default: 2g, hadoop mode only)
  --mongo-version VER    MongoDB version (e.g., 6.0, 7.0)
  --solr-version VER     Solr version (e.g., 8.11, 9.4)
  --mongo-port PORT      MongoDB host port (default: 27017)
  --solr-port PORT       Solr host port (default: 8983)
  --rest-port PORT       OpenCGA REST host port (default: 9090)
  --iva-port PORT        IVA host port (default: 8080)
  --org ID               Organization ID
  --user ID              Owner user ID
  --user-password PASS   Owner user password
  --admin-password PASS  OpenCGA admin password
EOF
}

usage_down() {
    cat <<'EOF'
Usage: ./deploy.sh down [options]

Stop and remove all services.

Options:
  -v, --volumes  Also remove Docker volumes (MongoDB, Solr data)
  --clean        Remove conf/, data/, iva/ directories (implies --volumes)
  --force        Skip running-jobs safety check
EOF
}

usage_build() {
    cat <<'EOF'
Usage: ./deploy.sh build [options]

Build the opencga-base Docker image from local Maven build output.
Requires 'mvn install -DskipTests' to have been run first.
When storage=hadoop, also builds the HBase + Phoenix image.

Options:
  --storage ENGINE  Storage engine: mongodb (default) or hadoop (HBase + Phoenix)
EOF
}

usage_load_demo() {
    cat <<'EOF'
Usage: ./deploy.sh load-demo [options]

Load demo data into the running OpenCGA instance.
Creates a project and study, fetches a VCF, and submits
variant index, annotation, stats, and secondary index jobs.

Requires services to be running (./deploy.sh up).

Options:
  --project NAME   Project ID (default: family)
  --study NAME     Study ID (default: corpasome)
EOF
}

usage_status() {
    cat <<'EOF'
Usage: ./deploy.sh status

Show status of all services.
EOF
}

usage_restart() {
    cat <<'EOF'
Usage: ./deploy.sh restart [options] [service...]

Restart services. If service names are given, restarts only those.
Otherwise, restarts all (down + up). Accepts container or service names.

Examples:
  ./deploy.sh restart                          # Full restart (all services)
  ./deploy.sh restart opencga-rest             # Restart REST only
  ./deploy.sh restart opencga-rest opencga-master  # Restart REST and Master

Options:
  --build       Build Docker image before starting
  --pull        Pull images before starting
  --regen-conf  Regenerate conf/ from build templates
  --storage ENGINE  Storage engine: mongodb (default) or hadoop (HBase + Phoenix)
  --force       Skip running-jobs safety check

  Configuration (persisted to .env):
  --image [IMAGE]        Docker image to use (default: opencb/opencga-base:<version>)
  --rest-heap SIZE       Java heap for REST server (e.g., 512m, 2g)
  --master-heap SIZE     Java heap for Master daemon (default: 400m)
  --job-heap SIZE        Java heap for child job processes (default: 1g)
  --hbase-heap SIZE      Java heap for HBase (default: 2g, hadoop mode only)
  --mongo-version VER    MongoDB version (e.g., 6.0, 7.0)
  --solr-version VER     Solr version (e.g., 8.11, 9.4)
  --mongo-port PORT      MongoDB host port (default: 27017)
  --solr-port PORT       Solr host port (default: 8983)
  --rest-port PORT       OpenCGA REST host port (default: 9090)
  --iva-port PORT        IVA host port (default: 8080)
  --org ID               Organization ID
  --user ID              Owner user ID
  --user-password PASS   Owner user password
  --admin-password PASS  OpenCGA admin password
EOF
}

usage_cli() {
    cat <<'EOF'
Usage: ./deploy.sh cli [opencga.sh args...]

Open the opencga.sh CLI, auto-logged-in as the owner user.
Optionally pass arguments to run a single command, e.g.:
  ./deploy.sh cli jobs top
  ./deploy.sh cli studies search
EOF
}

usage_shell() {
    cat <<'EOF'
Usage: ./deploy.sh shell

Open an interactive shell in an opencga-base container with
conf and scripts mounted. Useful for debugging and ad-hoc CLI commands.
EOF
}

usage_logs() {
    cat <<'EOF'
Usage: ./deploy.sh logs [options] [service...]

Tail logs from services. Pass service names to filter, e.g.:
  ./deploy.sh logs opencga-rest opencga-master
  ./deploy.sh logs hbase solr

Options:
  --no-follow   Print logs and exit (don't follow)
  --tail N      Show last N lines (default: 200)
  --all         Show all log lines (no tail limit)
EOF
}

usage_clean() {
    cat <<'EOF'
Usage: ./deploy.sh clean [options]

Remove all generated files (.env, conf/, data/, iva/) and Docker volumes.
Data is stored in ~/.opencga/local/ (or $OPENCGA_LOCAL_HOME).
Prompts for confirmation before proceeding.

Options:
  -y    Skip confirmation prompt
EOF
}

usage_init_conf() {
    cat <<'EOF'
Usage: ./deploy.sh init-conf

Regenerate conf/ from build config templates, patching hostnames
for Docker networking. Also regenerates iva/server.json.
EOF
}

check_prerequisites() {
    if ! command -v docker &>/dev/null; then
        log_error "docker is not installed or not in PATH"
        exit 1
    fi
    if ! docker compose version &>/dev/null; then
        log_error "'docker compose' plugin is not available"
        exit 1
    fi
}

# Check if a port is available (not in use)
check_port() {
    local port="$1" label="$2"
    if ss -tlnp 2>/dev/null | grep -q ":${port} " || \
       netstat -tln 2>/dev/null | grep -q ":${port} "; then
        log_warn "Port ${port} (${label}) is already in use"
        return 1
    fi
    return 0
}

# Project name derived from instance name (e.g., local → opencga-local)
COMPOSE_PROJECT="opencga-${INSTANCE_NAME}"

# docker compose wrapper — always reads compose files from the source directory.
# Snap-installed Docker cannot open files under dot-directories (~/.opencga/),
# so we avoid pointing -f at DATA_HOME.
dc() {
    # --env-file /dev/null prevents Docker Compose from auto-reading .env in the compose dir.
    # All variables are already exported via bash's 'source' in init_env().
    local compose_files=(-p "${COMPOSE_PROJECT}" --env-file /dev/null -f "${SCRIPT_DIR}/docker-compose.yml")
    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        compose_files+=(-f "${SCRIPT_DIR}/docker-compose.hadoop.yml")
    fi
    docker compose "${compose_files[@]}" "$@"
}

# Copy compose files, scripts, and hadoop/ into the instance directory.
# Called on every 'up' so the instance always has the latest files.
sync_instance_files() {
    mkdir -p "${DATA_HOME}"
    # Scripts (used by opencga-init, opencga-setup, load-demo containers)
    rm -rf "${DATA_HOME}/scripts"
    cp -r "${SCRIPT_DIR}/scripts" "${DATA_HOME}/scripts"
    # Hadoop Dockerfile + config (for building HBase image)
    if [ -d "${SCRIPT_DIR}/hadoop" ]; then
        rm -rf "${DATA_HOME}/hadoop"
        cp -r "${SCRIPT_DIR}/hadoop" "${DATA_HOME}/hadoop"
    fi
}

# Check all configured ports before starting (skip if our own containers hold them)
check_ports() {
    # If our compose project is already running, ports are ours — skip check
    if [ -n "$(dc ps -q 2>/dev/null)" ]; then
        return 0
    fi
    local failed=false
    check_port "${MONGO_PORT:-27017}" "MongoDB"       || failed=true
    check_port "${SOLR_PORT:-8983}" "Solr"             || failed=true
    check_port "${OPENCGA_REST_PORT:-9090}" "REST API" || failed=true
    check_port "${IVA_PORT:-8080}" "IVA"               || failed=true
    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        check_port "${HBASE_MASTER_PORT:-16010}" "HBase Master UI" || failed=true
        check_port "${ZOOKEEPER_PORT:-2181}" "ZooKeeper"           || failed=true
    fi
    if [ "$failed" = "true" ]; then
        log_error "Port conflict detected. Free the ports or change them in .env"
        exit 1
    fi
}

# Validate that heap fits within the container memory limit
check_heap_limits() {
    local heap_mb limit_mb
    if [ -n "${OPENCGA_REST_HEAP:-}" ]; then
        heap_mb=$(to_mb "${OPENCGA_REST_HEAP}")
        limit_mb=$(to_mb "$(apply_mem_multiplier "${OPENCGA_REST_HEAP}" 1.3)")
        if [ -n "${OPENCGA_REST_MEM:-}" ]; then
            local explicit_limit_mb
            explicit_limit_mb=$(to_mb "${OPENCGA_REST_MEM}")
            if [ "$heap_mb" -gt "$explicit_limit_mb" ]; then
                log_error "REST heap (${OPENCGA_REST_HEAP} = ${heap_mb}m) exceeds container limit (${OPENCGA_REST_MEM} = ${explicit_limit_mb}m)"
                exit 1
            fi
        fi
    fi
    if [ -n "${OPENCGA_MASTER_HEAP:-}" ]; then
        local combined_mb
        combined_mb=$(awk "BEGIN {printf \"%.0f\", $(to_mb "${OPENCGA_MASTER_HEAP}") + $(to_mb "${OPENCGA_JOB_HEAP:-1g}")}")
        if [ -n "${OPENCGA_MASTER_MEM:-}" ]; then
            local explicit_limit_mb
            explicit_limit_mb=$(to_mb "${OPENCGA_MASTER_MEM}")
            if [ "$combined_mb" -gt "$explicit_limit_mb" ]; then
                log_error "Master+Job heap (${OPENCGA_MASTER_HEAP} + ${OPENCGA_JOB_HEAP:-1g} = ${combined_mb}m) exceeds container limit (${OPENCGA_MASTER_MEM} = ${explicit_limit_mb}m)"
                exit 1
            fi
        fi
    fi
}

# Verify conf/ was patched correctly for Docker networking
check_conf() {
    local errors=0
    if [ ! -d "${CONF_TARGET}" ]; then
        return 0  # no conf yet, init_conf will create it
    fi
    if grep -q 'localhost:27017' "${CONF_TARGET}/configuration.yml" 2>/dev/null; then
        log_warn "configuration.yml still references localhost:27017 (expected mongodb:27017)"
        errors=$((errors + 1))
    fi
    if grep -q 'localhost:8983' "${CONF_TARGET}/storage-configuration.yml" 2>/dev/null; then
        log_warn "storage-configuration.yml still references localhost:8983 (expected solr:8983)"
        errors=$((errors + 1))
    fi
    if grep -q 'localhost:27017' "${CONF_TARGET}/storage-configuration.yml" 2>/dev/null; then
        log_warn "storage-configuration.yml still references localhost:27017 (expected mongodb:27017)"
        errors=$((errors + 1))
    fi
    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        if [ ! -f "${CONF_TARGET}/hadoop/hbase-site.xml" ]; then
            log_warn "hbase-site.xml not found in conf/hadoop/ (required for Hadoop mode)"
            errors=$((errors + 1))
        fi
    fi
    if [ "$errors" -gt 0 ]; then
        log_error "Config patching incomplete. Run './deploy.sh init-conf' or './deploy.sh up --regen-conf'"
        exit 1
    fi
}

# Check if any OpenCGA jobs are running in the master container.
# Blocks unless DO_FORCE=true.
check_running_jobs() {
    if [ "${DO_FORCE:-false}" = "true" ]; then
        return 0
    fi
    # Only check if the master container is running
    if ! dc ps -q opencga-master 2>/dev/null | head -1 | xargs -r docker inspect --format '{{.State.Status}}' 2>/dev/null | grep -q 'running'; then
        return 0
    fi
    local job_count
    job_count=$(dc exec -T opencga-master ps -eo args 2>/dev/null | grep -c "InternalMain" || true)
    if [ "${job_count:-0}" -gt 0 ]; then
        log_error "${job_count} job(s) running in opencga-master. Use --force to proceed anyway."
        dc exec -T opencga-master ps -eo pid,etime,args 2>/dev/null | grep "InternalMain" | \
            awk '{
                pid=$1; elapsed=$2
                cmd=""
                for(i=3;i<=NF;i++) {
                    if($i ~ /InternalMain$/) {
                        for(j=i+1;j<=NF;j++) {
                            if($j == "--opencga-token") break
                            if($j == "--outdir") { j++; continue }
                            if($j == "--job") { j++; continue }
                            cmd = cmd " " $j
                        }
                        break
                    }
                }
                gsub(/^ /, "", cmd)
                printf "  PID %-6s  %s  %s\n", pid, elapsed, cmd
            }' >&2
        exit 1
    fi
}

do_health() {
    local all_ok=true

    # MongoDB
    printf "  %-16s" "MongoDB"
    if dc exec -T mongodb mongosh --quiet --eval 'rs.status().ok' 2>/dev/null | grep -q '1'; then
        echo -e "${C_GREEN}healthy${C_RESET}"
    else
        echo -e "${C_RED}unreachable${C_RESET}"
        all_ok=false
    fi

    # Solr
    printf "  %-16s" "Solr"
    if dc exec -T solr curl -sf http://localhost:8983/solr/admin/info/system >/dev/null 2>&1; then
        echo -e "${C_GREEN}healthy${C_RESET}"
    else
        echo -e "${C_RED}unreachable${C_RESET}"
        all_ok=false
    fi

    # HBase (only in hadoop mode)
    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        printf "  %-16s" "HBase"
        if dc exec -T hbase hbase shell -n <<< 'status' 2>/dev/null | grep -q '1 active'; then
            echo -e "${C_GREEN}healthy${C_RESET}"
        else
            echo -e "${C_RED}unreachable${C_RESET}"
            all_ok=false
        fi
    fi

    # REST API
    printf "  %-16s" "REST API"
    local rest_url="http://localhost:${OPENCGA_REST_PORT:-9090}/opencga/webservices/rest/v3/meta/status"
    local rest_response
    rest_response=$(curl -sf "$rest_url" 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$rest_response" ]; then
        echo -e "${C_GREEN}healthy${C_RESET}"
    else
        echo -e "${C_RED}unreachable${C_RESET}"
        all_ok=false
    fi

    # IVA
    printf "  %-16s" "IVA"
    if curl -sf "http://localhost:${IVA_PORT:-8080}/iva/" >/dev/null 2>&1; then
        echo -e "${C_GREEN}healthy${C_RESET}"
    else
        echo -e "${C_RED}unreachable${C_RESET}"
        all_ok=false
    fi

    # Master
    printf "  %-16s" "Master"
    local master_cid
    master_cid=$(dc ps -q opencga-master 2>/dev/null | head -1)
    if [ -n "$master_cid" ]; then
        local master_status
        master_status=$(docker inspect --format '{{.State.Status}}' "$master_cid" 2>/dev/null)
        if [ "$master_status" = "running" ]; then
            echo -e "${C_GREEN}running${C_RESET}"
        else
            echo -e "${C_RED}${master_status:-unknown}${C_RESET}"
            all_ok=false
        fi
    else
        echo -e "${C_RED}not found${C_RESET}"
        all_ok=false
    fi

    echo ""
    if [ "$all_ok" = "true" ]; then
        log_info "All services healthy"
        return 0
    else
        log_error "Some services are not healthy"
        return 1
    fi
}

do_list() {
    printf "${C_DIM}%-16s %-14s %-40s %s${C_RESET}\n" "INSTANCE" "STATUS" "HOME" "STORAGE"
    printf "${C_DIM}%s${C_RESET}\n" "$(printf '─%.0s' $(seq 1 90))"

    # Get running project info from Docker
    local docker_projects
    docker_projects=$(docker compose ls --format json 2>/dev/null | jq -r '.[] | select(.Name | startswith("opencga-")) | .Name + "|" + .Status + "|" + .ConfigFiles' 2>/dev/null || true)

    # Scan ~/.opencga/ for instance directories
    local found=false
    for dir in "${OPENCGA_BASE}"/*/; do
        [ -d "$dir" ] || continue
        local name
        name=$(basename "$dir")
        local project="opencga-${name}"
        local status="${C_DIM}stopped${C_RESET}"
        local storage="mongodb"

        # Check Docker for running status
        if [ -n "$docker_projects" ]; then
            local docker_status
            docker_status=$(echo "$docker_projects" | grep "^${project}|" | head -1 | cut -d'|' -f2)
            if [ -n "$docker_status" ]; then
                status="${C_GREEN}${docker_status}${C_RESET}"
            fi
        fi

        # Read storage engine from .env
        if [ -f "${dir}.env" ]; then
            local engine
            engine=$(grep "^OPENCGA_STORAGE_ENGINE=" "${dir}.env" 2>/dev/null | cut -d= -f2)
            [ -n "$engine" ] && storage="$engine"
        fi

        printf "%-16s %-24b %-40s %s\n" "$name" "$status" "$dir" "$storage"
        found=true
    done

    if [ "$found" = "false" ]; then
        echo "  No instances found. Create one with: ./deploy.sh up"
    fi
}

do_info() {
    echo -e "${C_BOLD}OpenCGA Local Deployment${C_RESET}"
    echo ""

    # Version & image
    echo -e "${C_BOLD}Image${C_RESET}"
    printf "  %-22s %s\n" "OpenCGA version:" "${OPENCGA_VERSION:-unknown}"
    printf "  %-22s %s\n" "Docker image:" "${OPENCGA_IMAGE}"
    printf "  %-22s %s\n" "IVA image:" "${IVA_DOCKER_IMAGE:-opencb/iva-app}:${IVA_VERSION:-unknown}"
    local img_id
    img_id=$(docker image inspect "${OPENCGA_IMAGE}" --format '{{.Id}}' 2>/dev/null | cut -c8-19)
    if [ -n "$img_id" ]; then
        local img_created
        img_created=$(docker image inspect "${OPENCGA_IMAGE}" --format '{{.Created}}' 2>/dev/null | cut -c1-19)
        printf "  %-22s %s  (%s)\n" "Image ID:" "$img_id" "$img_created"
    else
        printf "  %-22s %s\n" "Image:" "${C_YELLOW}not found (run ./deploy.sh build)${C_RESET}"
    fi
    echo ""

    # Mode
    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        echo -e "${C_BOLD}Storage Engine${C_RESET}"
        printf "  %-22s %s\n" "Mode:" "hadoop (HBase + Phoenix)"
        printf "  %-22s %s\n" "MR Executor:" "embedded (in-process)"
        echo ""
    fi

    # Ports
    echo -e "${C_BOLD}Ports${C_RESET}"
    printf "  %-22s %s\n" "REST API:" "http://localhost:${OPENCGA_REST_PORT:-9090}/opencga"
    printf "  %-22s %s\n" "IVA:" "http://localhost:${IVA_PORT:-8080}/iva"
    printf "  %-22s %s\n" "MongoDB:" "localhost:${MONGO_PORT:-27017}"
    printf "  %-22s %s\n" "Solr:" "http://localhost:${SOLR_PORT:-8983}"
    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        printf "  %-22s %s\n" "HBase Master UI:" "http://localhost:${HBASE_MASTER_PORT:-16010}"
        printf "  %-22s %s\n" "ZooKeeper:" "localhost:${ZOOKEEPER_PORT:-2181}"
    fi
    echo ""

    # Memory
    echo -e "${C_BOLD}Memory${C_RESET}"
    local rest_heap="${OPENCGA_REST_HEAP:-500m}"
    local master_heap="${OPENCGA_MASTER_HEAP:-400m}"
    local job_heap="${OPENCGA_JOB_HEAP:-1g}"
    local rest_limit master_limit
    rest_limit=$(apply_mem_multiplier "$rest_heap" 1.3)
    local master_mb job_mb
    master_mb=$(to_mb "$master_heap")
    job_mb=$(to_mb "$job_heap")
    master_limit=$(awk "BEGIN {printf \"%.0fm\", ($master_mb + $job_mb) * 1.3}")
    printf "  %-22s heap %-5s  container %s  (1.3x)\n" "REST:" "$rest_heap" "$rest_limit"
    printf "  %-22s heap %-5s  job heap %-5s  container %s  ((master+job)*1.3)\n" "Master:" "$master_heap" "$job_heap" "$master_limit"
    printf "  %-22s %s\n" "MongoDB:" "${OPENCGA_MONGO_MEM:-1g}"
    printf "  %-22s %s\n" "Solr:" "${OPENCGA_SOLR_MEM:-1g}"
    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        local hbase_heap="${OPENCGA_HBASE_HEAP:-2g}"
        local hbase_limit
        hbase_limit=$(apply_mem_multiplier "$hbase_heap" 1.3)
        printf "  %-22s heap %-5s  container %s  (1.3x)\n" "HBase:" "$hbase_heap" "$hbase_limit"
    fi
    printf "  %-22s %s\n" "IVA:" "20m"
    echo ""

    # Credentials
    echo -e "${C_BOLD}Credentials${C_RESET}"
    printf "  %-22s %s\n" "Organization:" "${OPENCGA_ORG_ID:-}"
    printf "  %-22s %s\n" "Owner:" "${OPENCGA_OWNER_ID:-}"
    printf "  %-22s ${C_DIM}%s${C_RESET}\n" "Password:" "${OPENCGA_OWNER_PASSWORD:-}"
    echo ""

    # Paths
    echo -e "${C_BOLD}Paths${C_RESET}"
    printf "  %-22s %s\n" "Instance:" "${INSTANCE_NAME}"
    printf "  %-22s %s\n" "Home:" "${DATA_HOME}"
    printf "  %-22s %s\n" "Config:" "${CONF_TARGET}"
    printf "  %-22s %s\n" "Scripts:" "${SCRIPT_DIR}/scripts"
    printf "  %-22s %s\n" "Docker socket:" "${DOCKER_SOCK:-not set}"
    printf "  %-22s %s\n" "Project root:" "${PROJECT_ROOT}"
    echo ""

    # Infrastructure versions
    echo -e "${C_BOLD}Infrastructure${C_RESET}"
    printf "  %-22s %s\n" "MongoDB:" "mongo:${MONGO_VERSION:-6.0}"
    printf "  %-22s %s\n" "Solr:" "solr:${SOLR_VERSION:-8.11}"
    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        printf "  %-22s %s\n" "HBase:" "2.5.10 (standalone)"
        printf "  %-22s %s\n" "Phoenix:" "5.2.0"
    fi
    local compose_ver
    compose_ver=$(docker compose version --short 2>/dev/null)
    printf "  %-22s %s\n" "Docker Compose:" "${compose_ver:-unknown}"
    local docker_ver
    docker_ver=$(docker version --format '{{.Server.Version}}' 2>/dev/null)
    printf "  %-22s %s\n" "Docker Engine:" "${docker_ver:-unknown}"
}

init_conf() {
    local force="${1:-false}"

    if [ -d "${CONF_TARGET}" ] && [ "${force}" = "false" ]; then
        log_info "conf/ already exists (use --regen-conf to regenerate)"
        return 0
    fi

    if [ ! -d "${CONF_SOURCE}" ]; then
        log_error "Config source not found at ${CONF_SOURCE}"
        echo "       Run 'mvn install -DskipTests' from the project root first."
        exit 1
    fi

    log_step "Generating conf/ from ${CONF_SOURCE}"
    rm -rf "${CONF_TARGET}"
    cp -r "${CONF_SOURCE}" "${CONF_TARGET}"

    # Copy files from the Docker image that aren't in the build conf dir
    # (e.g., opencga-env.sh, conf/hadoop/) — the bind mount hides the image's /opt/opencga/conf/
    docker run --rm --user 0:0 -v "${CONF_TARGET}:/host-conf" "${OPENCGA_IMAGE}" \
        bash -c 'for f in /opt/opencga/conf/*; do
            name=$(basename "$f")
            if [ ! -e "/host-conf/$name" ]; then
                cp -r "$f" "/host-conf/$name"
            fi
        done
        chown -R '"$(id -u):$(id -g)"' /host-conf/'

    # Patch hostnames for Docker networking
    # configuration.yml: MongoDB host
    sed -i 's|localhost:27017|mongodb:27017|g' "${CONF_TARGET}/configuration.yml"

    # storage-configuration.yml: MongoDB host + Solr host
    sed -i 's|localhost:27017|mongodb:27017|g' "${CONF_TARGET}/storage-configuration.yml"
    sed -i 's|http://localhost:8983/solr/|http://solr:8983/solr/|g' "${CONF_TARGET}/storage-configuration.yml"

    # client-configuration.yml: REST host URL
    sed -i 's|https://ws.opencb.org/opencga-prod|http://opencga-rest:9090/opencga|g' "${CONF_TARGET}/client-configuration.yml"

    # log4j2.service.xml: add timestamp to log filenames and enable file logging
    local log4j_service="${CONF_TARGET}/log4j2.service.xml"
    if [ -f "${log4j_service}" ]; then
        sed -i 's|\${name}\.\${hostName}\.log|\${name}.\${date:yyyyMMdd_HHmmss}.log|g' "${log4j_service}"
        sed -i 's|\${name}\.\${hostName}\.%i\.log\.gz|\${name}.\${date:yyyyMMdd_HHmmss}.%i.log.gz|g' "${log4j_service}"
        sed -i 's|opencga.log.file.enabled}}|opencga.log.file.enabled:-TRUE}}|g' "${log4j_service}"
    fi

    # configuration.yml: set executor to "local" for all queues, max 1 concurrent job
    sed -i 's|executor: "k8s"|executor: "local"|g' "${CONF_TARGET}/configuration.yml"
    sed -i 's|local.maxConcurrentJobs: 2|local.maxConcurrentJobs: 1|g' "${CONF_TARGET}/configuration.yml"

    # Generate IVA server.json (uses OPENCGA_ORG_ID from .env)
    mkdir -p "${DATA_HOME}/iva"
    cat > "${DATA_HOME}/iva/server.json" <<EOJSON
{
    "host": "http://localhost:${OPENCGA_REST_PORT:-9090}/opencga",
    "organizations": [
        "${OPENCGA_ORG_ID:-myorg}",
        "opencga"
    ]
}
EOJSON

    # Hadoop storage engine configuration
    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        log_step "Patching storage-configuration.yml for Hadoop mode"

        local storage_conf="${CONF_TARGET}/storage-configuration.yml"

        # Set default engine to hadoop
        sed -i 's|defaultEngine:.*|defaultEngine: "hadoop"|' "${storage_conf}"

        # Set MR executor to embedded (no hadoop binary needed in container)
        sed -i 's|storage.hadoop.mr.executor:.*|storage.hadoop.mr.executor: "embedded"|' "${storage_conf}"

        # Replace snappy compression with gz (native snappy lib not available in this image)
        sed -i 's|compression: "snappy"|compression: "gz"|g' "${storage_conf}"

        # Reduce pre-split regions for local standalone HBase (500 is for production clusters)
        sed -i 's|variant.table.preSplit.numSplits:.*|variant.table.preSplit.numSplits: 10|' "${storage_conf}"

        # Skip reporting running jobs to the master (not needed locally, avoids connection issues)
        sed -i '/storage.hadoop.mr.executor:/a\        storage.hadoop.mr.skipReportRunningJobs: true' "${storage_conf}"

        # Generate client-side hbase-site.xml in conf/hadoop/ (added to classpath by opencga-env.sh)
        # This tells the HBase client where to find ZooKeeper (the hbase container)
        mkdir -p "${CONF_TARGET}/hadoop"
        cat > "${CONF_TARGET}/hadoop/hbase-site.xml" <<'EOXML'
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property><name>hbase.zookeeper.quorum</name><value>hbase</value></property>
  <property><name>hbase.zookeeper.property.clientPort</name><value>2181</value></property>
  <property><name>phoenix.schema.isNamespaceMappingEnabled</name><value>true</value></property>
  <property><name>phoenix.table.ttl.enabled</name><value>false</value></property>
  <property><name>phoenix.client.maxMetaDataCacheSize</name><value>268435456</value></property>
  <property><name>mapreduce.framework.name</name><value>local</value></property>
</configuration>
EOXML
        log_info "Generated hbase-site.xml for Docker containers"
    fi

    # Validate patching succeeded
    local patch_errors=0
    grep -q 'localhost:27017' "${CONF_TARGET}/configuration.yml" 2>/dev/null && patch_errors=$((patch_errors + 1))
    grep -q 'localhost:8983' "${CONF_TARGET}/storage-configuration.yml" 2>/dev/null && patch_errors=$((patch_errors + 1))
    grep -q 'localhost:27017' "${CONF_TARGET}/storage-configuration.yml" 2>/dev/null && patch_errors=$((patch_errors + 1))
    if [ "$patch_errors" -gt 0 ]; then
        log_warn "Some config hostnames were not patched — template format may have changed"
    fi

    log_info "conf/ generated and patched for Docker networking."
}

do_build() {
    if [ ! -f "${DOCKERFILE}" ]; then
        log_error "Dockerfile not found at ${DOCKERFILE}"
        echo "       Run 'mvn install -DskipTests' from the project root first."
        exit 1
    fi

    log_step "Building ${OPENCGA_IMAGE}"
    docker build \
        -t "${OPENCGA_IMAGE}" \
        -f "${DOCKERFILE}" \
        "${BUILD_CONTEXT}"
    log_info "Docker image built successfully."

    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        # Ensure hadoop/ is synced for the build context
        if [ -d "${SCRIPT_DIR}/hadoop" ]; then
            mkdir -p "${DATA_HOME}"
            rm -rf "${DATA_HOME}/hadoop"
            cp -r "${SCRIPT_DIR}/hadoop" "${DATA_HOME}/hadoop"
        fi
        log_step "Building HBase + Phoenix image"
        docker build \
            -t opencga-hbase:local \
            "${DATA_HOME}/hadoop"
        log_info "HBase image built successfully."
    fi
}

do_up() {
    local pull_flag=""
    if [ "${DO_PULL:-false}" = "true" ]; then
        pull_flag="--pull always"
    fi

    # Sync compose files, scripts, hadoop/ into instance directory
    sync_instance_files

    # Pre-flight checks
    check_running_jobs
    check_ports
    check_heap_limits
    check_conf

    export_heap_vars

    # Ensure data dirs exist and are owned by opencga user (UID 1001), no root files
    local data_dir="${DATA_HOME}/data"
    mkdir -p "${data_dir}"
    docker run --rm -v "${data_dir}:/data" alpine sh -c "mkdir -p /data/sessions /data/logs && chown -R 1001:1001 /data"

    # shellcheck disable=SC2086
    dc up -d ${pull_flag}
    echo ""
    log_info "Services starting. Use './deploy.sh top' to monitor or './deploy.sh logs' for logs."
    echo -e "  REST API: ${C_CYAN}http://localhost:${OPENCGA_REST_PORT:-9090}/opencga/webservices/rest/v2/meta/status${C_RESET}"
    echo -e "  IVA:      ${C_CYAN}http://localhost:${IVA_PORT:-8080}/iva${C_RESET}"
    if [ "${OPENCGA_STORAGE_ENGINE:-mongodb}" = "hadoop" ]; then
        echo -e "  HBase UI: ${C_CYAN}http://localhost:${HBASE_MASTER_PORT:-16010}${C_RESET}"
    fi
    if [ -n "${OPENCGA_ORG_ID:-}" ]; then
        echo ""
        echo -e "  Organization: ${C_BOLD}${OPENCGA_ORG_ID}${C_RESET}"
        echo -e "  User:         ${C_BOLD}${OPENCGA_OWNER_ID:-}${C_RESET}"
        echo -e "  Password:     ${C_DIM}${OPENCGA_OWNER_PASSWORD:-}${C_RESET}"
    fi
}

do_down() {
    check_running_jobs
    local vol_flag=""
    if [ "${DO_VOLUMES:-false}" = "true" ]; then
        vol_flag="-v"
    fi
    # shellcheck disable=SC2086
    dc down --remove-orphans ${vol_flag}
}

do_status() {
    dc ps -a
}

do_shell() {
    dc run --rm --no-deps \
        -v "${SCRIPT_DIR}/scripts:/opt/opencga/scripts:ro" \
        --entrypoint bash \
        opencga-rest
}

do_load_demo() {
    local env_args=(
        -e OPENCGA_OWNER_ID="${OPENCGA_OWNER_ID}"
        -e OPENCGA_OWNER_PASSWORD="${OPENCGA_OWNER_PASSWORD}"
        -e OPENCGA_ORG_ID="${OPENCGA_ORG_ID}"
    )
    [ -n "${DEMO_PROJECT:-}" ] && env_args+=(-e OPENCGA_DEMO_PROJECT="${DEMO_PROJECT}")
    [ -n "${DEMO_STUDY:-}" ] && env_args+=(-e OPENCGA_DEMO_STUDY="${DEMO_STUDY}")
    dc run --rm --no-deps \
        -v "${SCRIPT_DIR}/scripts:/opt/opencga/scripts:ro" \
        "${env_args[@]}" \
        --entrypoint "bash /opt/opencga/scripts/opencga-load-demo.sh" \
        opencga-rest
}

clean_files() {
    # Files created by containers are owned by UID 1001; use alpine to remove them
    local vol_args=()
    [ -d "${DATA_HOME}/data" ] && vol_args+=(-v "${DATA_HOME}/data:/data")
    [ -d "${DATA_HOME}/iva" ] && vol_args+=(-v "${DATA_HOME}/iva:/iva")
    if [ ${#vol_args[@]} -gt 0 ]; then
        docker run --rm "${vol_args[@]}" alpine sh -c "rm -rf /data/* /iva/*" 2>/dev/null || true
    fi
    for item in conf data iva; do
        if [ -e "${DATA_HOME}/${item}" ]; then
            rm -rf "${DATA_HOME}/${item}"
            log_info "Removed ${DATA_HOME}/${item}"
        fi
    done
    if [ -f "${DATA_HOME}/.env" ]; then
        rm -f "${DATA_HOME}/.env"
        log_info "Removed ${DATA_HOME}/.env"
    fi
}

_top_to_bytes() {
    local val="$1"
    case "$val" in
        *GiB) awk "BEGIN {printf \"%.0f\", ${val%GiB} * 1073741824}" ;;
        *MiB) awk "BEGIN {printf \"%.0f\", ${val%MiB} * 1048576}" ;;
        *KiB) awk "BEGIN {printf \"%.0f\", ${val%KiB} * 1024}" ;;
        *GB)  awk "BEGIN {printf \"%.0f\", ${val%GB} * 1000000000}" ;;
        *MB)  awk "BEGIN {printf \"%.0f\", ${val%MB} * 1000000}" ;;
        *kB|*KB) val="${val%B}"; val="${val%k}"; val="${val%K}"; awk "BEGIN {printf \"%.0f\", $val * 1000}" ;;
        *B)   echo "${val%B}" ;;
        *)    echo "0" ;;
    esac
}

_top_fmt_bytes() {
    local b="$1"
    if [ "$b" -ge 1073741824 ] 2>/dev/null; then
        awk "BEGIN {printf \"%.1fG\", $b / 1073741824}"
    elif [ "$b" -ge 1048576 ] 2>/dev/null; then
        awk "BEGIN {printf \"%.0fM\", $b / 1048576}"
    elif [ "$b" -ge 1024 ] 2>/dev/null; then
        awk "BEGIN {printf \"%.0fK\", $b / 1024}"
    else
        echo "${b}B"
    fi
}

_top_bar() {
    local pct=${1%.*} width=20
    [ -z "$pct" ] && pct=0
    local filled=$(( pct * width / 100 ))
    local empty=$(( width - filled ))
    local color="${C_GREEN}"
    [ "$pct" -ge 60 ] && color="${C_YELLOW}"
    [ "$pct" -ge 85 ] && color="${C_RED}"
    local filled_str="" empty_str=""
    [ "$filled" -gt 0 ] && filled_str=$(printf '█%.0s' $(seq 1 $filled))
    [ "$empty" -gt 0 ] && empty_str=$(printf '░%.0s' $(seq 1 $empty))
    printf "%b%s%b%s%b" "${color}" "$filled_str" "${C_DIM}" "$empty_str" "${C_RESET}"
}

do_top() {
    if [ -z "$(dc ps -q 2>/dev/null)" ]; then
        log_error "No containers running. Start with './deploy.sh up' first."
        exit 1
    fi
    local stats_file vol_file jobs_file inspect_file
    stats_file=$(mktemp)
    vol_file=$(mktemp)
    jobs_file=$(mktemp)
    inspect_file=$(mktemp)
    local bg_stats_pid="" bg_vol_pid="" bg_jobs_pid=""
    local saved_tty
    saved_tty=$(stty -g 2>/dev/null) || true
    _top_cleanup() {
        tput cnorm 2>/dev/null
        [ -n "${saved_tty:-}" ] && stty "$saved_tty" 2>/dev/null
        [ -n "${bg_stats_pid:-}" ] && kill "$bg_stats_pid" 2>/dev/null && wait "$bg_stats_pid" 2>/dev/null
        [ -n "${bg_vol_pid:-}" ] && kill "$bg_vol_pid" 2>/dev/null && wait "$bg_vol_pid" 2>/dev/null
        [ -n "${bg_jobs_pid:-}" ] && kill "$bg_jobs_pid" 2>/dev/null && wait "$bg_jobs_pid" 2>/dev/null
        rm -f "${stats_file:-}" "${vol_file:-}" "${jobs_file:-}" "${inspect_file:-}"
    }
    trap '_top_cleanup; exit 0' INT TERM
    trap '_top_cleanup' EXIT
    tput civis 2>/dev/null  # hide cursor

    # Background: container stats + inspect (every 2s)
    (
        trap 'exit 0' TERM
        while true; do
            ids=$(dc ps -aq 2>/dev/null)
            if [ -n "$ids" ]; then
                running=$(dc ps -q 2>/dev/null)
                if [ -n "$running" ]; then
                    docker stats --no-stream \
                        --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}|{{.PIDs}}|{{.BlockIO}}|{{.NetIO}}' \
                        $running 2>/dev/null | awk -F'|' '!seen[$1]++' > "$stats_file.tmp" && mv "$stats_file.tmp" "$stats_file"
                fi
                docker inspect \
                    --format '{{.Name}}|{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}—{{end}}|{{.State.StartedAt}}' \
                    $ids > "$inspect_file.tmp" 2>/dev/null && mv "$inspect_file.tmp" "$inspect_file"
            fi
            sleep 2
        done
    ) &
    bg_stats_pid=$!

    # Background: volume measurement (every 10s)
    (
        trap 'exit 0' TERM
        while true; do
            out=""
            project=$(dc config --format json 2>/dev/null | jq -r '.name' 2>/dev/null)
            if [ -n "$project" ]; then
                vol_mounts=(); vol_real_names=(); vol_short=(); du_paths=()
                for vol in mongodb solr hbase-data; do
                    real_name="${project}_opencga-${vol}"
                    if docker volume inspect "$real_name" >/dev/null 2>&1; then
                        vol_mounts+=(-v "${real_name}:/mnt/${vol}:ro")
                        vol_real_names+=("$real_name")
                        vol_short+=("$vol")
                        du_paths+=("/mnt/${vol}")
                    fi
                done
                if [ ${#vol_mounts[@]} -gt 0 ]; then
                    du_out=$(docker run --rm "${vol_mounts[@]}" alpine du -sh "${du_paths[@]}" 2>/dev/null || true)
                    for i in "${!vol_short[@]}"; do
                        size=$(echo "$du_out" | grep "/mnt/${vol_short[$i]}" | awk '{print $1}')
                        out+=$(printf "%-40s %8s\n" "${vol_real_names[$i]}" "${size:-?}")$'\n'
                    done
                fi
            fi
            local data_dir="${DATA_HOME}/data"
            for path in "${data_dir}/sessions" "${data_dir}/logs"; do
                if [ -d "$path" ]; then
                    size=$(du -sh "$path" 2>/dev/null | awk '{print $1}')
                    out+=$(printf "%-40s %8s\n" "$path" "${size:-?}")$'\n'
                fi
            done
            printf "%s" "$out" > "$vol_file.tmp" && mv "$vol_file.tmp" "$vol_file"
            sleep 10
        done
    ) &
    bg_vol_pid=$!

    # Background: running jobs in master (every 3s)
    (
        trap 'exit 0' TERM
        while true; do
            dc exec -T opencga-master ps -eo pid,pcpu,rss,etime,args 2>/dev/null | \
                grep "InternalMain" | \
                awk '{
                    pid=$1; cpu=$2; rss_kb=$3; elapsed=$4
                    # Convert RSS (KB) to human readable
                    if (rss_kb >= 1048576) mem_str=sprintf("%.1fG", rss_kb/1048576)
                    else if (rss_kb >= 1024) mem_str=sprintf("%.0fM", rss_kb/1024)
                    else mem_str=rss_kb"K"

                    # Convert etime (SS, MM:SS, HH:MM:SS, D-HH:MM:SS) to human format
                    days=0; hours=0; mins=0; secs=0
                    if (index(elapsed, "-")) {
                        split(elapsed, dp, "-"); days=dp[1]; elapsed=dp[2]
                    }
                    n=split(elapsed, tp, ":")
                    if (n==3) { hours=tp[1]; mins=tp[2]; secs=tp[3] }
                    else if (n==2) { mins=tp[1]; secs=tp[2] }
                    else { secs=tp[1] }
                    fmt=""
                    if (days>0) fmt=days"d"hours"h"
                    else if (hours>0) fmt=hours+0"h"mins+0"m"
                    else if (mins>0) fmt=mins+0"m"secs+0"s"
                    else fmt=secs+0"s"

                    cmd=""
                    for(i=5;i<=NF;i++) {
                        if($i ~ /InternalMain$/) {
                            for(j=i+1;j<=NF;j++) {
                                if($j == "--opencga-token") break
                                if($j == "--outdir") { j++; continue }
                                if($j == "--job") { j++; continue }
                                cmd = cmd " " $j
                            }
                            break
                        }
                    }
                    gsub(/^ /, "", cmd)
                    printf "%s|%s%%|%s|%s|%s\n", pid, cpu, mem_str, fmt, cmd
                }' > "$jobs_file.tmp" 2>/dev/null && mv "$jobs_file.tmp" "$jobs_file"
            sleep 3
        done
    ) &
    bg_jobs_pid=$!

    local sep50
    sep50=$(printf '─%.0s' $(seq 1 50))

    # Helper: compute uptime string from ISO timestamp
    _uptime() {
        local started="$1"
        local start_epoch now_epoch diff
        start_epoch=$(date -d "$started" +%s 2>/dev/null) || return
        now_epoch=$(date +%s)
        diff=$(( now_epoch - start_epoch ))
        if [ "$diff" -ge 86400 ]; then
            printf "%dd%dh" $(( diff / 86400 )) $(( (diff % 86400) / 3600 ))
        elif [ "$diff" -ge 3600 ]; then
            printf "%dh%dm" $(( diff / 3600 )) $(( (diff % 3600) / 60 ))
        elif [ "$diff" -ge 60 ]; then
            printf "%dm%ds" $(( diff / 60 )) $(( diff % 60 ))
        else
            printf "%ds" "$diff"
        fi
    }

    # Helper: color a status string
    _status_color() {
        local status="$1" health="$2" label
        if [ "$status" = "running" ]; then
            if [ "$health" = "healthy" ]; then
                label="healthy"
            elif [ "$health" = "unhealthy" ]; then
                label="unhealthy"
            elif [ "$health" = "starting" ]; then
                label="starting"
            else
                label="running"
            fi
        elif [ "$status" = "exited" ]; then
            label="exited"
        else
            label="$status"
        fi
        local color
        case "$label" in
            healthy)   color="${C_GREEN}" ;;
            unhealthy) color="${C_RED}" ;;
            starting)  color="${C_YELLOW}" ;;
            running)   color="${C_GREEN}" ;;
            exited)    color="${C_DIM}" ;;
            *)         color="${C_YELLOW}" ;;
        esac
        printf "%b%-9s%b" "$color" "$label" "${C_RESET}"
    }

    clear
    sleep 1  # let first stats collection finish
    while true; do
        local buf=""
        local raw
        raw=$(cat "$stats_file" 2>/dev/null)

        if [ -z "$raw" ]; then
            buf+="${C_BOLD}OpenCGA Docker — $(date '+%H:%M:%S')${C_RESET}\n"
            buf+="\n${C_DIM}Waiting for data...${C_RESET}\n"
        else
            # Compute totals
            local total_mem=0 total_limit=0 total_cpu=0
            while IFS='|' read -r name cpu mem_usage mem_pct pids bio nio; do
                local cpu_num=${cpu%%%}
                total_cpu=$(awk "BEGIN {printf \"%.1f\", $total_cpu + $cpu_num}")
                local used limit
                used=$(echo "$mem_usage" | awk -F' / ' '{print $1}' | tr -d ' ')
                limit=$(echo "$mem_usage" | awk -F' / ' '{print $2}' | tr -d ' ')
                total_mem=$(( total_mem + $(_top_to_bytes "$used") ))
                total_limit=$(( total_limit + $(_top_to_bytes "$limit") ))
            done <<< "$raw"

            # Header
            buf+="${C_BOLD}OpenCGA Docker — $(date '+%H:%M:%S')${C_RESET}"
            buf+="    CPU: ${C_CYAN}${total_cpu}%${C_RESET}"
            buf+="  MEM: ${C_CYAN}$(_top_fmt_bytes $total_mem) / $(_top_fmt_bytes $total_limit)${C_RESET}\n"

            # ── Container table ──
            buf+="\n"
            buf+="$(printf "${C_DIM}%-36s %6s  %-20s %5s %5s  %-17s %-17s %-9s %6s${C_RESET}" \
                "CONTAINER" "CPU%" "MEMORY" "USED" "LIMIT" "NET I/O" "BLOCK I/O" "STATUS" "UP")\n"
            local sep_line
            sep_line=$(printf '─%.0s' $(seq 1 131))
            buf+="${C_DIM}${sep_line}${C_RESET}\n"
            # Build a lookup from inspect data: name -> status|health|started
            local inspect_raw
            inspect_raw=$(cat "$inspect_file" 2>/dev/null)

            # Sort stats lines by container start time (oldest first)
            local sorted_raw="$raw"
            if [ -n "$inspect_raw" ]; then
                sorted_raw=$(while IFS='|' read -r n _rest; do
                    [ -z "$n" ] && continue
                    local ts
                    ts=$(echo "$inspect_raw" | grep "/${n}|" | head -1 | cut -d'|' -f4)
                    printf "%s\t%s\n" "${ts:-9999}" "$n|$_rest"
                done <<< "$raw" | sort -t$'\t' -k1,1 | cut -f2-)
            fi

            while IFS='|' read -r name cpu mem_usage mem_pct pids bio nio; do
                [ -z "$name" ] && continue
                local cpu_num=${cpu%%%}
                local mem_pct_num=${mem_pct%%%}
                local used limit
                used=$(echo "$mem_usage" | awk -F' / ' '{print $1}' | tr -d ' ')
                limit=$(echo "$mem_usage" | awk -F' / ' '{print $2}' | tr -d ' ')

                # Lookup health/uptime from inspect
                local status="running" health="—" started="" uptime_str="—"
                if [ -n "$inspect_raw" ]; then
                    local inspect_line
                    inspect_line=$(echo "$inspect_raw" | grep "/${name}|" | head -1)
                    if [ -n "$inspect_line" ]; then
                        status=$(echo "$inspect_line" | cut -d'|' -f2)
                        health=$(echo "$inspect_line" | cut -d'|' -f3)
                        started=$(echo "$inspect_line" | cut -d'|' -f4)
                        uptime_str=$(_uptime "$started")
                    fi
                fi
                local status_str
                status_str=$(_status_color "$status" "$health")

                # Net I/O and Block I/O: use raw docker stats format
                local nio_str bio_str
                nio_str=$(echo "$nio" | sed 's| / |/|')
                bio_str=$(echo "$bio" | sed 's| / |/|')

                local bar
                bar=$(_top_bar "$mem_pct_num")

                buf+="$(printf "%-36s %6s%%" "$name" "$cpu_num")"
                buf+="  ${bar}"
                buf+="$(printf " %5s %5s  %-17s %-17s " \
                    "$(_top_fmt_bytes $(_top_to_bytes "$used"))" \
                    "$(_top_fmt_bytes $(_top_to_bytes "$limit"))" \
                    "$nio_str" "$bio_str")"
                buf+="${status_str}"
                buf+="$(printf "%6s" "$uptime_str")\n"
            done <<< "$sorted_raw"

            # ── Jobs ──
            local jobs_raw
            jobs_raw=$(cat "$jobs_file" 2>/dev/null)
            if [ -n "$jobs_raw" ]; then
                local job_count
                job_count=$(echo "$jobs_raw" | wc -l)
                buf+="\n${C_BOLD}Jobs (${job_count})${C_RESET}\n"
                buf+="${C_DIM}${sep50}${C_RESET}\n"
                while IFS='|' read -r jpid jcpu jmem jelapsed jcmd; do
                    buf+="$(printf "  ${C_CYAN}%-40s${C_RESET} %6s cpu  %5s mem  %s" \
                        "$jcmd" "$jcpu" "$jmem" "$jelapsed")\n"
                done <<< "$jobs_raw"
            fi

            # ── Volumes ──
            local vol_cache
            vol_cache=$(cat "$vol_file" 2>/dev/null)
            if [ -n "$vol_cache" ]; then
                buf+="\n"
                buf+="$(printf "${C_DIM}%-40s %8s${C_RESET}" "VOLUME" "DISK")\n"
                buf+="${C_DIM}${sep50}${C_RESET}\n"
                buf+="${vol_cache}"
            fi
        fi

        buf+="\n${C_DIM}Press q or Ctrl+C to exit${C_RESET}\n"

        # Render: cursor to top-left, clear remainder of each line, clear below
        tput cup 0 0 2>/dev/null
        printf "%b" "${buf//\\n/\\033[K\\n}"
        tput ed 2>/dev/null

        # Wait 1s, but exit immediately on 'q'
        if read -rsn1 -t 1 key 2>/dev/null && [[ "$key" == "q" || "$key" == "Q" ]]; then
            break
        fi
    done
}

do_clean() {
    if [ "${SKIP_CONFIRM:-false}" != "true" ]; then
        read -r -p "This will remove instance '${INSTANCE_NAME}' (${DATA_HOME}) and its Docker volumes. Continue? [y/N] " response
        if [[ ! "${response}" =~ ^[Yy]$ ]]; then
            echo "Aborted."
            return 0
        fi
    fi
    # Always include all compose files so all volumes are removed (including hadoop)
    local all_compose=(-p "${COMPOSE_PROJECT}" --env-file /dev/null -f "${SCRIPT_DIR}/docker-compose.yml")
    if [ -f "${SCRIPT_DIR}/docker-compose.hadoop.yml" ]; then
        all_compose+=(-f "${SCRIPT_DIR}/docker-compose.hadoop.yml")
    fi
    log_info "Stopping and removing containers..."
    docker compose "${all_compose[@]}" down --remove-orphans 2>/dev/null || true

    # Remove project volumes (match by project name prefix)
    local vol
    for vol in $(docker volume ls -q 2>/dev/null | grep "^${COMPOSE_PROJECT}_"); do
        if docker volume rm "$vol" >/dev/null 2>&1; then
            log_info "Removed volume: $vol"
        else
            log_warn "Failed to remove volume: $vol (in use?)"
        fi
    done

    clean_files
}

# ─────────────────────────────────────────────────────────────────────────────
# Argument parsing helpers
# ─────────────────────────────────────────────────────────────────────────────

bad_option() {
    log_error "Unknown option '${1}' for command '${COMMAND}'"
    echo "Run './deploy.sh ${COMMAND} --help' for usage."
    exit 1
}

# Consume the next positional arg as a value for a flag.
# Usage: next_arg "$@"; VAR="${_next_val}"; shift
_next_val=""
next_arg() {
    if [ $# -lt 2 ] || [[ "$2" == -* ]]; then
        log_error "Option '$1' requires a value"
        exit 1
    fi
    _next_val="$2"
}

# Validate --storage value
validate_storage() {
    case "$1" in
        mongodb|hadoop) ;;
        *) log_error "Invalid storage engine '$1'. Must be 'mongodb' or 'hadoop'."; exit 1 ;;
    esac
}

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

check_prerequisites

COMMAND="${1:-help}"
shift 2>/dev/null || true

case "${COMMAND}" in
    help|-h|--help)
        usage
        ;;

    build)
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help) usage_build; exit 0 ;;
                --storage) next_arg "$@"; validate_storage "${_next_val}"; update_env OPENCGA_STORAGE_ENGINE "${_next_val}"; shift ;;
                *) bad_option "$1" ;;
            esac
            shift
        done
        do_build
        ;;

    list)
        do_list
        ;;

    up)
        DO_BUILD=false; DO_PULL=false; REGEN_CONF=false; DO_LOAD_DEMO=false; DO_FORCE=false
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help)        usage_up; exit 0 ;;
                --build)          DO_BUILD=true ;;
                --pull)           DO_PULL=true ;;
                --regen-conf)     REGEN_CONF=true ;;
                --load-demo)      DO_LOAD_DEMO=true ;;
                --storage)        next_arg "$@"; validate_storage "${_next_val}"; update_env OPENCGA_STORAGE_ENGINE "${_next_val}"; REGEN_CONF=true; shift ;;
                --force)          DO_FORCE=true ;;
                --image)          if [ $# -ge 2 ] && [[ "$2" != -* ]]; then
                                      update_env OPENCGA_IMAGE "$2"; shift
                                  else
                                      log_info "Current image: ${OPENCGA_IMAGE}"
                                  fi ;;
                --rest-heap)      next_arg "$@"; update_env OPENCGA_REST_HEAP "${_next_val}"; shift ;;
                --master-heap)    next_arg "$@"; update_env OPENCGA_MASTER_HEAP "${_next_val}"; shift ;;
                --job-heap)       next_arg "$@"; update_env OPENCGA_JOB_HEAP "${_next_val}"; shift ;;
                --hbase-heap)     next_arg "$@"; update_env OPENCGA_HBASE_HEAP "${_next_val}"; shift ;;
                --mongo-version)  next_arg "$@"; update_env MONGO_VERSION "${_next_val}"; shift ;;
                --solr-version)   next_arg "$@"; update_env SOLR_VERSION "${_next_val}"; shift ;;
                --mongo-port)     next_arg "$@"; update_env MONGO_PORT "${_next_val}"; shift ;;
                --solr-port)      next_arg "$@"; update_env SOLR_PORT "${_next_val}"; shift ;;
                --rest-port)      next_arg "$@"; update_env OPENCGA_REST_PORT "${_next_val}"; shift ;;
                --iva-port)       next_arg "$@"; update_env IVA_PORT "${_next_val}"; shift ;;
                --org)            next_arg "$@"; update_env OPENCGA_ORG_ID "${_next_val}"; shift ;;
                --user)           next_arg "$@"; update_env OPENCGA_OWNER_ID "${_next_val}"; shift ;;
                --user-password)  next_arg "$@"; update_env OPENCGA_OWNER_PASSWORD "${_next_val}"; shift ;;
                --admin-password) next_arg "$@"; update_env OPENCGA_ADMIN_PASSWORD "${_next_val}"; shift ;;
                *)                bad_option "$1" ;;
            esac
            shift
        done
        if [ "${DO_BUILD}" = "true" ]; then do_build; fi
        init_conf "${REGEN_CONF}"
        do_up
        if [ "${DO_LOAD_DEMO}" = "true" ]; then do_load_demo; fi
        ;;

    down)
        DO_VOLUMES=false; DO_CLEAN=false; DO_FORCE=false
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help)      usage_down; exit 0 ;;
                -v|--volumes)   DO_VOLUMES=true ;;
                --clean)        DO_CLEAN=true; DO_VOLUMES=true ;;
                --force)        DO_FORCE=true ;;
                *)              bad_option "$1" ;;
            esac
            shift
        done
        do_down
        if [ "${DO_CLEAN}" = "true" ]; then clean_files; fi
        ;;

    restart)
        DO_BUILD=false; DO_PULL=false; REGEN_CONF=false; DO_FORCE=false; RESTART_SERVICES=()
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help)        usage_restart; exit 0 ;;
                --build)          DO_BUILD=true ;;
                --pull)           DO_PULL=true ;;
                --regen-conf)     REGEN_CONF=true ;;
                --storage)        next_arg "$@"; validate_storage "${_next_val}"; update_env OPENCGA_STORAGE_ENGINE "${_next_val}"; REGEN_CONF=true; shift ;;
                --force)          DO_FORCE=true ;;
                --image)          if [ $# -ge 2 ] && [[ "$2" != -* ]]; then
                                      update_env OPENCGA_IMAGE "$2"; shift
                                  else
                                      log_info "Current image: ${OPENCGA_IMAGE}"
                                  fi ;;
                --rest-heap)      next_arg "$@"; update_env OPENCGA_REST_HEAP "${_next_val}"; shift ;;
                --master-heap)    next_arg "$@"; update_env OPENCGA_MASTER_HEAP "${_next_val}"; shift ;;
                --job-heap)       next_arg "$@"; update_env OPENCGA_JOB_HEAP "${_next_val}"; shift ;;
                --hbase-heap)     next_arg "$@"; update_env OPENCGA_HBASE_HEAP "${_next_val}"; shift ;;
                --mongo-version)  next_arg "$@"; update_env MONGO_VERSION "${_next_val}"; shift ;;
                --solr-version)   next_arg "$@"; update_env SOLR_VERSION "${_next_val}"; shift ;;
                --mongo-port)     next_arg "$@"; update_env MONGO_PORT "${_next_val}"; shift ;;
                --solr-port)      next_arg "$@"; update_env SOLR_PORT "${_next_val}"; shift ;;
                --rest-port)      next_arg "$@"; update_env OPENCGA_REST_PORT "${_next_val}"; shift ;;
                --iva-port)       next_arg "$@"; update_env IVA_PORT "${_next_val}"; shift ;;
                --org)            next_arg "$@"; update_env OPENCGA_ORG_ID "${_next_val}"; shift ;;
                --user)           next_arg "$@"; update_env OPENCGA_OWNER_ID "${_next_val}"; shift ;;
                --user-password)  next_arg "$@"; update_env OPENCGA_OWNER_PASSWORD "${_next_val}"; shift ;;
                --admin-password) next_arg "$@"; update_env OPENCGA_ADMIN_PASSWORD "${_next_val}"; shift ;;
                *)                RESTART_SERVICES+=("$1") ;;
            esac
            shift
        done
        if [ ${#RESTART_SERVICES[@]} -gt 0 ]; then
            # Recreate specific services — picks up any compose file changes
            # (env vars, mem_limit, restart policy, etc.) unlike plain "restart"
            export_heap_vars
            log_info "Recreating: ${RESTART_SERVICES[*]}"
            dc up -d --no-deps "${RESTART_SERVICES[@]}"
        else
            # Full restart (down + up)
            do_down
            if [ "${DO_BUILD}" = "true" ]; then do_build; fi
            init_conf "${REGEN_CONF}"
            do_up
        fi
        ;;

    load-demo)
        DEMO_PROJECT=""; DEMO_STUDY=""
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help)      usage_load_demo; exit 0 ;;
                --project)      next_arg "$@"; DEMO_PROJECT="${_next_val}"; shift ;;
                --study)        next_arg "$@"; DEMO_STUDY="${_next_val}"; shift ;;
                *)              bad_option "$1" ;;
            esac
            shift
        done
        do_load_demo
        ;;

    cli)
        # Pass all remaining args to opencga.sh (no validation — they're opencga.sh args)
        exec "${SCRIPT_DIR}/opencga.sh" "$@"
        ;;

    shell)
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help) usage_shell; exit 0 ;;
                *) bad_option "$1" ;;
            esac
            shift
        done
        do_shell
        ;;

    mongosh)
        dc exec mongodb mongosh "$@"
        ;;

    status)
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help) usage_status; exit 0 ;;
                *) bad_option "$1" ;;
            esac
            shift
        done
        do_status
        ;;

    top)
        do_top
        ;;

    health)
        do_health
        ;;

    info)
        do_info
        ;;

    logs)
        LOG_FOLLOW=true; LOG_TAIL="200"; EXTRA_ARGS=()
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help)      usage_logs; exit 0 ;;
                --no-follow)    LOG_FOLLOW=false ;;
                --tail)         next_arg "$@"; LOG_TAIL="${_next_val}"; shift ;;
                --all)          LOG_TAIL="" ;;
                *)              EXTRA_ARGS+=("$1") ;;
            esac
            shift
        done
        log_args=()
        if [ "${LOG_FOLLOW}" = "true" ]; then log_args+=(-f); fi
        if [ -n "${LOG_TAIL}" ]; then log_args+=(--tail "${LOG_TAIL}"); fi
        dc logs "${log_args[@]}" "${EXTRA_ARGS[@]}"
        ;;

    clean)
        SKIP_CONFIRM=false
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help) usage_clean; exit 0 ;;
                -y)        SKIP_CONFIRM=true ;;
                *)         bad_option "$1" ;;
            esac
            shift
        done
        do_clean
        ;;

    init-conf)
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help) usage_init_conf; exit 0 ;;
                *) bad_option "$1" ;;
            esac
            shift
        done
        init_conf true
        ;;

    *)
        log_error "Unknown command: ${COMMAND}"
        usage
        exit 1
        ;;
esac
