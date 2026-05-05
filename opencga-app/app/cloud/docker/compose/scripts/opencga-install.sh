#!/bin/bash

set -e
set -o pipefail

# Install OpenCGA catalog database.
# Idempotent: skips if catalog is already installed.

OPENCGA_HOME=${OPENCGA_HOME:-/opt/opencga}
ADMIN_PASSWORD=${OPENCGA_ADMIN_PASSWORD:?Missing OPENCGA_ADMIN_PASSWORD}

echo "============================================="
echo " OpenCGA Catalog Installation"
echo "============================================="

# Check if catalog is already installed (catalog status doesn't need password)
INSTALLED=$("${OPENCGA_HOME}/bin/opencga-admin.sh" catalog status 2>/dev/null | jq -r '.installed' || echo "false")
if [ "$INSTALLED" = "true" ]; then
    echo "Catalog already installed, skipping."
else
    echo "Installing catalog..."
    echo "$ADMIN_PASSWORD" | "${OPENCGA_HOME}/bin/opencga-admin.sh" catalog install
fi

echo "============================================="
echo " OpenCGA Catalog ready!"
echo "============================================="