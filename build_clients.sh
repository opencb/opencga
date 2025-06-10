#!/bin/bash

set -e
set -o pipefail
set -o nounset

# Default: NO skip
SKIP_BUILD_OPENCGA=false
SKIP_PYTHON=false
SKIP_RCLIENT=false
SKIP_JAVA=false
SKIP_JS=false

# Get the OpenCGA version from the Maven project
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
# Define the directory where the built clients will be stored
BUILD_DIR="./build"
DIST_DIR="$BUILD_DIR/dist"

CLIENTS_DIR="$BUILD_DIR/clients"
# Function to print main usage of the script
function print_usage() {
  echo ""
  echo "Build opencga clients."
  echo ""
  echo "Usage:   $(basename "$0") [options]"
  echo ""
  echo "  Options:"
  echo "     -b     --skip-build-opencga       FLAG         Skip Build OpenCGA"
  echo "     -p     --skip-python              FLAG         Skip Build OpenCGA Python client"
  echo "     -r     --skip-rclient             FLAG         Skip Build OpenCGA R client"
  echo "     -w     --skip-javascript          FLAG         Skip Build OpenCGA JavaScript client"
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
    -p|--skip-python )
      SKIP_PYTHON=true
      shift
      ;;
    -r|--skip-rclient)
      SKIP_RCLIENT=true
      shift
      ;;
    -w|--skip-javascript)
      SKIP_JS=true
      shift
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "Error: Opción no reconocida '$1'"
      print_usage
      exit 1
      ;;
  esac
done



if ! $SKIP_BUILD_OPENCGA; then
  echo ">> Building OpenCGA..."
  mvn clean install -DskipTests -DskipITs -DskipCheckstyle -DskipSpotBugs -DskipJavadoc --no-transfer-progress
fi

# Check if the build directory exists, and delete it to create a new one total clean
[ -d "${DIST_DIR}" ] && rm -rf "${DIST_DIR}"
# Create the directory for the clients distribution
mkdir -p ${DIST_DIR}
mkdir -p ${CLIENTS_DIR}

if ! $SKIP_RCLIENT; then
  echo ">> Building OpenCGA R client..."
  R_DIR="$CLIENTS_DIR/R"
  mkdir -p "$R_DIR"
  cp -r ./opencga-client/src/main/R $CLIENTS_DIR
  R_VERSION=$(echo "$VERSION" | sed 's/-SNAPSHOT/.9000/g')
  echo "Calculated R Version: $R_VERSION"
  # Update the DESCRIPTION file with the calculated version
  DESCRIPTION_FILE="$CLIENTS_DIR/R/DESCRIPTION"
  echo "Updating $DESCRIPTION_FILE with version $R_VERSION"
  sed -i "s/OPENCGA_R_VERSION/${R_VERSION}/" "$DESCRIPTION_FILE"

  export DOCKER_BUILDKIT=1
  docker build -t opencb/opencga-r-builder:dev -f opencga-app/app/cloud/docker/opencga-r-builder/Dockerfile opencga-app/app/cloud/docker/opencga-r-builder
  docker run --rm  --mount type=bind,source="$R_DIR",target=/opt/opencga/R --mount type=bind,source="$DIST_DIR",target=/opt/opencga opencb/opencga-r-builder:dev R CMD build /opt/opencga/R
  rm -rf ${DIST_DIR}/R
fi

if ! $SKIP_PYTHON; then
  echo ">> Building OpenCGA Python client..."
  PYTHON_DIR="$CLIENTS_DIR/python"
  mkdir -p "$PYTHON_DIR"
  echo "Copying Python and R to $CLIENTS_DIR"
  cp -r "./opencga-client/src/main/python" "$CLIENTS_DIR"
  echo "Calculating Python version"
  PYOPENCGA_VERSION=$(python3 "./opencga-app/app/scripts/calculate_pypi_version.py" "$VERSION")
  echo "Calculated Python Version: ${PYOPENCGA_VERSION}"
  echo "Updating setup.py with version ${PYOPENCGA_VERSION}"
  sed -i "s/PYOPENCGA_VERSION/${PYOPENCGA_VERSION}/" "$CLIENTS_DIR/python/setup.py"

  python3 -m pip install --upgrade pip
  pip install --upgrade setuptools packaging
  ./build/clients/python/python-build.sh build
  PYPY_VERSION=$(python3 opencga-app/app/scripts/calculate_pypi_version.py "$VERSION")
  echo ">> Compressing OpenCGA Python client..."
  PYTHON_DIR="$CLIENTS_DIR/python"
  ARCHIVE_NAME="opencga-python-client-$PYPY_VERSION.tar.gz"
  echo ">> Compressing the Python client directory $PYTHON_DIR to $DIST_DIR/$ARCHIVE_NAME..."
  tar -czf "$DIST_DIR/$ARCHIVE_NAME" -C "$PYTHON_DIR" .
fi

if ! $SKIP_JS; then
  echo ">> Copying OpenCGA JavaScript client..."
  JAVASCRIPT_DIR="$CLIENTS_DIR/javascript"
  mkdir -p "$JAVASCRIPT_DIR"
  cp -r ./opencga-client/src/main/javascript "$CLIENTS_DIR"
  JAVASCRIPT_CLIENT_NAME="opencga-javascript-client-$VERSION.tar.gz"
  echo ">> Compressing the Javascript client directory $JAVASCRIPT_DIR to $DIST_DIR/$JAVASCRIPT_CLIENT_NAME..."
  tar -czf "$DIST_DIR/$JAVASCRIPT_CLIENT_NAME" -C "$JAVASCRIPT_DIR" .
fi