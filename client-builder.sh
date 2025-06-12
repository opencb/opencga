#!/bin/bash

set -e
set -o pipefail
set -o nounset

# Default: NO skip
SKIP_BUILD_OPENCGA=false
SKIP_PYTHON=false
SKIP_R=false
SKIP_JAVA=false
SKIP_JS=false

# Get the OpenCGA version from the Maven project
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
# Define the directory where the built clients will be stored

BUILD_DIR="./build"
DIST_DIR="$BUILD_DIR/dist"
CLIENTS_DIR="$BUILD_DIR/clients"
OPENCGA_BUILD_DIR="./opencga-home/build"

# Function to print main usage of the script
function print_usage() {
  echo ""
  echo "Build opencga clients."
  echo ""
  echo "Usage:   $(basename "$0") [options]"
  echo ""
  echo "  Options:"
  echo "     -b     --skip-build-opencga       FLAG         Skip Build OpenCGA-enterprise"
  echo "     -p     --skip-python              FLAG         Skip Build OpenCGA Python client"
  echo "     -r     --skip-r                   FLAG         Skip Build OpenCGA R client"
  echo "     -j     --skip-javascript          FLAG         Skip Build OpenCGA JavaScript client"
  echo "     -h     --help                     FLAG         Print this help and exit"
  echo ""
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -b|--skip-build-opencga)
      SKIP_BUILD_OPENCGA=true
      shift
      ;;
    -p|--skip-python)
      SKIP_PYTHON=true
      shift
      ;;
    -r|--skip-r)
      SKIP_R=true
      shift
      ;;
    -j|--skip-javascript)
      SKIP_JS=true
      shift
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "Error: Unknown option '$1'"
      print_usage
      exit 1
      ;;
  esac
done

if ! $SKIP_BUILD_OPENCGA; then
  echo ">> Building OpenCGA Enterprise ..."
  ./build.sh
fi

# Check if the build directory exists, and delete it to create a new one total clean
[ -d "${DIST_DIR}" ] && rm -rf "${DIST_DIR}"
# Create the directory for the clients distribution
mkdir -p ${DIST_DIR}




if ! $SKIP_R; then
  echo "Building R library"
  echo "==================="
  R_SOURCE_DIR="./opencga-enterprise-client/src/main/R"
  echo "Copying OpenCGA R client files to $CLIENTS_DIR"
  cp -r "$R_SOURCE_DIR" "$CLIENTS_DIR"
  # If Version is a SNAPSHOT, we need to replace it with a version that R can understand
  R_VERSION=$(echo "$VERSION" | sed 's/-SNAPSHOT/.9000/g')
  echo "Calculated R Version: $R_VERSION"
  # Update the DESCRIPTION file with the calculated version
  DESCRIPTION_FILE="$CLIENTS_DIR/R/DESCRIPTION"
  echo "Updating $DESCRIPTION_FILE with version $R_VERSION"
  sed -i "s/OPENCGA_R_VERSION/${R_VERSION}/" "$DESCRIPTION_FILE"
  export DOCKER_BUILDKIT=1
  docker build -t opencb/opencga-r-builder:dev -f opencga-home/opencga-app/app/cloud/docker/opencga-r-builder/Dockerfile opencga-home/opencga-app/app/cloud/docker/opencga-r-builder
  docker run --rm --mount type=bind,source="$CLIENTS_DIR/R",target=/opt/opencga/R --mount type=bind,source="$DIST_DIR",target=/opt/opencga opencb/opencga-r-builder:dev R CMD build /opt/opencga/R
  rm -rf ${DIST_DIR}/R
fi

if ! $SKIP_PYTHON; then
  echo "Building python library"
  echo "============================="
  echo "Prepare directory: Python"
  rm -rf "$CLIENTS_DIR/python"
  echo "Copying OpenCGA python client files to $CLIENTS_DIR"
  cp -r "$OPENCGA_BUILD_DIR/clients/python" "$CLIENTS_DIR"
  echo "Copying Python to $CLIENTS_DIR"
  cp -r "opencga-enterprise-client/src/main/python" "$CLIENTS_DIR"
  echo "Updating imports from pyopencga to pyopencga_enterprise"
  find "$CLIENTS_DIR/python/pyopencga" -type f -name "*.py" -exec sed -i.bak 's/from pyopencga/from pyopencga_enterprise/g' {} \;
  find "$CLIENTS_DIR/python/pyopencga" -name "*.bak" -delete
  echo "Calculating Python version"
  PYTHON_VERSION=$(python3 "opencga-enterprise-app/app/scripts/calculate_pypi_version.py" "$VERSION")
  echo "Updating setup.py with version $PYTHON_VERSION"
  sed -i "s/PYOPENCGA_ENTERPRISE_VERSION/${PYTHON_VERSION}/" "$CLIENTS_DIR/python/setup.py"
  echo "Renaming folder pyopencga to pyopencga_enterprise"
  mv "$CLIENTS_DIR/python/pyopencga" "$CLIENTS_DIR/python/pyopencga_enterprise"
  python3 -m pip install --upgrade pip
  pip install --upgrade setuptools packaging
  ./build/clients/python/python-build.sh build
  echo ">> Compressing OpenCGA Python client..."
  PYTHON_DIR="$CLIENTS_DIR/python"
  ARCHIVE_NAME="opencga-enterprise-python-client-$PYTHON_VERSION.tar.gz"
  echo ">> Compressing the Python client directory $PYTHON_DIR to $DIST_DIR/$ARCHIVE_NAME..."
  tar -czf "$DIST_DIR/$ARCHIVE_NAME" -C "$PYTHON_DIR" .
fi

if ! $SKIP_JS; then
  JAVASCRIPT_DIR="$CLIENTS_DIR/javascript"
  echo "Building JavaScript library"
  echo "============================="
  cp -r ./opencga-enterprise-client/src/main/javascript "$JAVASCRIPT_DIR"
  JAVASCRIPT_CLIENT_NAME="opencga-enterprise-javascript-client-$VERSION.tar.gz"
  echo ">> Compressing the Javascript client directory $JAVASCRIPT_DIR to $DIST_DIR/$JAVASCRIPT_CLIENT_NAME..."
  tar -czf "$DIST_DIR/$JAVASCRIPT_CLIENT_NAME" -C "$JAVASCRIPT_DIR" .
fi
