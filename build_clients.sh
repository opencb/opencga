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
  echo "     -r     --skip-rclient             FLAG         Skip Build OpenCGA R client"
  echo "     -j     --skip-java                FLAG         Skip Build OpenCGA Java client"
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
    -p|--skpython)
      SKIP_PYTHON=true
      shift
      ;;
    -r|--skrclient)
      SKIP_RCLIENT=true
      shift
      ;;
    -j|--skjava)
      SKIP_JAVA=true
      shift
      ;;
    -w|--skjavascript)
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
  echo ">> Building OpenCGA-enterprise..."
  ./build.sh
fi


mkdir -p ${DIST_DIR}

if ! $SKIP_RCLIENT; then
  echo ">> Building OpenCGA R client..."
  export DOCKER_BUILDKIT=1
  docker build -t opencb/opencga-r-builder:dev -f opencga-home/opencga-app/app/cloud/docker/opencga-r-builder/Dockerfile opencga-home/opencga-app/app/cloud/docker/opencga-r-builder
  docker run --rm --mount type=bind,source="./build/clients/R",target=/opt/opencga/R --mount type=bind,source="$DIST_DIR",target=/opt/opencga opencb/opencga-r-builder:dev R CMD build /opt/opencga/R
  rm -rf ${DIST_DIR}/R
fi

if ! $SKIP_PYTHON; then
  echo "Building python library"
  echo "============================="

  echo "Prepare directory: Python"
  rm -rf "$BUILD_DIR/clients/python"
  
  echo "Copying OpenCGA python client files to $BUILD_DIR/clients"
  cp -r "$OPENCGA_BUILD_DIR/clients/python" "$BUILD_DIR/clients"
  
  echo "Copying Python to $BUILD_DIR/clients"
  cp -r "opencga-enterprise-client/src/main/python" "$BUILD_DIR/clients"
  
  echo "Updating imports from pyopencga to pyopencga_enterprise"
  find "$BUILD_DIR/clients/python/pyopencga" -type f -name "*.py" -exec sed -i.bak 's/from pyopencga/from pyopencga_enterprise/g' {} \;
  find "$BUILD_DIR/clients/python/pyopencga" -name "*.bak" -delete

  echo "Calculating Python version"
  PYTHON_VERSION=$(python3 "opencga-enterprise-app/app/scripts/calculate_pypi_version.py" "$VERSION")
  echo "Calculated Python Version: $PYTHON_VERSION"
  
  echo "Updating setup.py with version $PYTHON_VERSION"
  sed -i "s/PYOPENCGA_ENTERPRISE_VERSION/${PYTHON_VERSION}/" "$BUILD_DIR/clients/python/setup.py"
  
  echo "Renaming folder pyopencga to pyopencga_enterprise"
  mv "$BUILD_DIR/clients/python/pyopencga" "$BUILD_DIR/clients/python/pyopencga_enterprise"

  python3 -m pip install --upgrade pip
  pip install --upgrade setuptools packaging
  ./build/clients/python/python-build.sh build

  echo ">> Compressing OpenCGA Python client..."
  PYTHON_DIR="./build/clients/python"
  ARCHIVE_NAME="opencga-enterprise-python-client-$PYTHON_VERSION.tar.gz"
  echo ">> Compressing the Python client directory $PYTHON_DIR to $DIST_DIR/$ARCHIVE_NAME..."
  tar -czf "$DIST_DIR/$ARCHIVE_NAME" -C "$PYTHON_DIR" .
fi

if ! $SKIP_JAVA; then
  echo ">> Copying OpenCGA Java client..."
  # Copiar el .jar generado al directorio build
  cp ./opencga-enterprise-client/target/opencga-enterprise-client-*.jar "$DIST_DIR"
fi


if ! $SKIP_JS; then
  echo ">> Copying OpenCGA JavaScript client..."
  JAVASCRIPT_DIR="./build/clients/javascript"
  JAVASCRIPT_CLIENT_NAME="opencga-enterprise-javascript-client-$VERSION.tar.gz"
  echo ">> Compressing the Javascript client directory $JAVASCRIPT_DIR to $DIST_DIR/$JAVASCRIPT_CLIENT_NAME..."
  tar -czf "$DIST_DIR/$JAVASCRIPT_CLIENT_NAME" -C "$JAVASCRIPT_DIR" .
fi

#tar -czvf build/opencga-enterprise-clients-$VERSION.tar.gz -C "$DIST_DIR" .