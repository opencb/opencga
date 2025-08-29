#!/bin/bash

set -e
set -o pipefail
set -o nounset

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
sed -i "s/REPLACEME_OPENCGA_VERSION/${VERSION}/g" "${SOLR_DIR}/INSTALL.md"

# Iterate over the different config sets
for name in variant rga rga-aux; do
  CONFIG_SET_NAME="opencga-$name-configset"
  CONFIG_SET_DIR="$SOLR_DIR/tmp"

  # Assuming misc/solr/conf directory exits with the default configuration XML files (except the managed-schema file)
  echo Preparing config set for $name: $CONFIG_SET_NAME
  mkdir $CONFIG_SET_DIR
  cp -r $OPENCGA_BUILD_DIR/misc/solr/conf $CONFIG_SET_DIR

  if [ $name == "variant" ]; then
    CONFIG_SET_NAME=$(basename "$OPENCGA_HOME/opencga-storage/opencga-storage-core/target/solr/"opencga-variant-configset*)
    cp -v "$OPENCGA_HOME/opencga-storage/opencga-storage-core/target/solr/${CONFIG_SET_NAME}/managed-schema" "$CONFIG_SET_DIR/conf"
  elif [ $name == "rga" ]; then
    CONFIG_SET_NAME="opencga-$name-configset-$VERSION"
    cp -v "$OPENCGA_HOME/opencga-clinical/target/classes/rga/managed-schema" "$CONFIG_SET_DIR/conf"
  elif [ $name == "rga-aux" ]; then
    CONFIG_SET_NAME="opencga-$name-configset-$VERSION"
    cp -v "$OPENCGA_HOME/opencga-clinical/target/classes/rga/aux-managed-schema" "$CONFIG_SET_DIR/conf/managed-schema"
  else
    CONFIG_SET_NAME="opencga-$name-configset-$VERSION"
    cp -v "$OPENCGA_HOME/opencga-catalog/target/classes/solr/$name-managed-schema" "$CONFIG_SET_DIR/conf/managed-schema"
  fi

  mv "$CONFIG_SET_DIR" "$SOLR_DIR/$CONFIG_SET_NAME"

  echo '${SOLR_HOME}/bin/solr zk upconfig -n '$CONFIG_SET_NAME' -d ./'$CONFIG_SET_NAME' -z ${ZK_HOST}' >> $SOLR_DIR/install.sh

done

rm -rf $OPENCGA_BUILD_DIR/misc/solr/prepare_configsets.sh $OPENCGA_BUILD_DIR/misc/solr/conf
