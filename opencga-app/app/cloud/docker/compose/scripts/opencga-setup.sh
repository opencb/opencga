#!/bin/bash

set -e
set -o pipefail

# Create organization and owner user in a local OpenCGA deployment.
# Idempotent: skips steps that have already been completed.

OPENCGA_HOME=${OPENCGA_HOME:-/opt/opencga}
ADMIN_PASSWORD=${OPENCGA_ADMIN_PASSWORD:?Missing OPENCGA_ADMIN_PASSWORD}
ORG_ID=${OPENCGA_ORG_ID:?Missing OPENCGA_ORG_ID}
OWNER_ID=${OPENCGA_OWNER_ID:?Missing OPENCGA_OWNER_ID}
OWNER_NAME=${OPENCGA_OWNER_NAME:-$OWNER_ID}
OWNER_EMAIL=${OPENCGA_OWNER_EMAIL:-${OWNER_ID}@opencga.local}
OWNER_PASSWORD=${OPENCGA_OWNER_PASSWORD:?Missing OPENCGA_OWNER_PASSWORD}

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
