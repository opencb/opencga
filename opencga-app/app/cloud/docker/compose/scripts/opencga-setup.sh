#!/bin/bash

set -e
set -o pipefail

# Create organization and owner user in a local OpenCGA deployment.
# Idempotent: skips steps that have already been completed.

usage() {
    cat <<'EOF'
Usage: opencga-setup.sh [OPTIONS]

Create an organization and owner user in a local OpenCGA deployment.
Idempotent: skips steps that have already been completed.

Steps performed:
  1. Login as admin
  2. Create organization (skip if already exists)
  3. Create owner user (skip if already exists)
  4. Make user owner of the organization
  5. Verify owner login

Options (override environment variables):
  --opencga-home DIR        OpenCGA installation directory (env: OPENCGA_HOME, default: /opt/opencga)
  --admin-password PASS     Admin password (env: OPENCGA_ADMIN_PASSWORD) [required]
  --owner-password PASS     Owner user password (env: OPENCGA_OWNER_PASSWORD) [required]
  --org-id ID               Organization identifier (env: OPENCGA_ORG_ID, default: test)
  --owner-id ID             Owner user identifier (env: OPENCGA_OWNER_ID, default: test-user)
  --owner-name NAME         Owner display name (env: OPENCGA_OWNER_NAME, default: Test User)
  --owner-email EMAIL       Owner email (env: OPENCGA_OWNER_EMAIL, default: test@opencga.local)
  --host URL                REST host URL (exports OPENCGA_CLIENT_REST_HOST)
  -h, --help                Show this help message
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)          usage; exit 0 ;;
        --opencga-home)     OPENCGA_HOME="$2"; shift 2 ;;
        --host)             export OPENCGA_CLIENT_REST_HOST="$2"; shift 2 ;;
        --admin-password)   OPENCGA_ADMIN_PASSWORD="$2"; shift 2 ;;
        --org-id)           OPENCGA_ORG_ID="$2"; shift 2 ;;
        --owner-id)         OPENCGA_OWNER_ID="$2"; shift 2 ;;
        --owner-password)   OPENCGA_OWNER_PASSWORD="$2"; shift 2 ;;
        --owner-name)       OPENCGA_OWNER_NAME="$2"; shift 2 ;;
        --owner-email)      OPENCGA_OWNER_EMAIL="$2"; shift 2 ;;
        *)                  echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

OPENCGA_HOME=${OPENCGA_HOME:-/opt/opencga}
ADMIN_PASSWORD=${OPENCGA_ADMIN_PASSWORD:?Missing --admin-password or OPENCGA_ADMIN_PASSWORD}
ORG_ID=${OPENCGA_ORG_ID:-test}
OWNER_ID=${OPENCGA_OWNER_ID:-test-user}
OWNER_NAME=${OPENCGA_OWNER_NAME:-Test User}
OWNER_EMAIL=${OPENCGA_OWNER_EMAIL:-test@opencga.local}
OWNER_PASSWORD=${OPENCGA_OWNER_PASSWORD:?Missing --owner-password or OPENCGA_OWNER_PASSWORD}

echo "============================================="
echo " OpenCGA Setup"
echo "============================================="

# Login as admin
echo "Logging in as admin..."
echo "$ADMIN_PASSWORD" | "${OPENCGA_HOME}/bin/opencga.sh" users login -u opencga -p

# Create organization (skip if already exists)
echo "Creating organization '${ORG_ID}'..."
output=$("${OPENCGA_HOME}/bin/opencga.sh" organizations create --id "$ORG_ID" 2>&1) && rc=0 || rc=$?
if [ "$rc" -eq 0 ]; then
    echo "Organization '${ORG_ID}' created."
elif echo "$output" | grep -qi "already exists"; then
    echo "Organization '${ORG_ID}' already exists, skipping."
else
    echo "$output" >&2
    exit "$rc"
fi

# Create owner user (skip if already exists)
echo "Creating user '${OWNER_ID}' in organization '${ORG_ID}'..."
output=$(echo "$ADMIN_PASSWORD" | "${OPENCGA_HOME}/bin/opencga.sh" users create \
    --id "$OWNER_ID" \
    --name "$OWNER_NAME" \
    --email "$OWNER_EMAIL" \
    --password "$OWNER_PASSWORD" \
    --organization "$ORG_ID" 2>&1) && rc=0 || rc=$?
if [ "$rc" -eq 0 ]; then
    echo "User '${OWNER_ID}' created."
elif echo "$output" | grep -qi "already exists"; then
    echo "User '${OWNER_ID}' already exists, skipping."
else
    echo "$output" >&2
    exit "$rc"
fi

# Make user owner of the organization (idempotent)
echo "Making '${OWNER_ID}' owner of '${ORG_ID}'..."
"${OPENCGA_HOME}/bin/opencga.sh" organizations update --organization "$ORG_ID" --owner "$OWNER_ID"

# Verify: login as owner
echo "Verifying owner login..."
echo "$OWNER_PASSWORD" | "${OPENCGA_HOME}/bin/opencga.sh" users login -u "$OWNER_ID" -p --organization "$ORG_ID"

echo "============================================="
echo " OpenCGA setup complete!"
echo " Organization: ${ORG_ID}"
echo " Owner: ${OWNER_ID}"
echo "============================================="
