#!/bin/bash

if [ $# -eq 2 ]; then
  echo $0 $@
else
  echo "Error: invalid input parameters"
  echo "Usage: $0 <path to the opencga home directory> <opencga version>"
  exit 1
fi

OPENCGA_HOME=$1
VERSION=$2

OPENCGA_BUILD_DIR="$OPENCGA_HOME/build/"
SOLR_DIR="$OPENCGA_BUILD_DIR/misc/solr"


# Change version in INSTALL.md and install.sh
sed "s/REPLACEME_ENTERPRISE_VERSION/${VERSION}/g" "${OPENCGA_HOME}/opencga-enterprise-app/app/cvdb/INSTALL.md" >> "${SOLR_DIR}/INSTALL.md"
sed "s/REPLACEME_ENTERPRISE_VERSION/${VERSION}/g" "${OPENCGA_HOME}/opencga-enterprise-app/app/cvdb/install.sh" >> "${SOLR_DIR}/install.sh"

# Iterate over the different CVDB config sets
for name in ca ci cv cve; do
  CONFIG_SET_NAME="opencga-$name-configset-$VERSION"
  CONFIG_SET_DIR="$SOLR_DIR/$CONFIG_SET_NAME"

  # Assuming misc/solr/conf directory exits with the default configuration XML files (except the managed-schema file)
  echo Preparing config set for $name: $CONFIG_SET_NAME
  mkdir $CONFIG_SET_DIR
  cp -r $OPENCGA_HOME/opencga-enterprise-app/app/cvdb/conf $CONFIG_SET_DIR

  cp -v $OPENCGA_HOME/opencga-enterprise-cvdb/src/main/resources/solr/${name}-managed-schema $CONFIG_SET_DIR/conf/managed-schema
done
