#!/usr/bin/env bash

set -e

ZK_HOST=${1:-localhost:9983}
SOLR_HOME=${2:-/opt/solr}
OPENCGA_HOME=${3:-/opt/opencga}
VERSION=REPLACEME_OPENCGA_VERSION

echo "Install configsets from ${OPENCGA_HOME}/misc/solr"
echo "ZK_HOST=${ZK_HOST}"
echo "SOLR_HOME=${SOLR_HOME}"

cd ${OPENCGA_HOME}/misc/solr

${SOLR_HOME}/bin/solr zk upconfig -n opencga-variant-configset-${VERSION} -d ./opencga-variant-configset-${VERSION} -z ${ZK_HOST}
${SOLR_HOME}/bin/solr zk upconfig -n opencga-rga-configset-${VERSION} -d ./opencga-rga-configset-${VERSION} -z ${ZK_HOST}
${SOLR_HOME}/bin/solr zk upconfig -n opencga-rga-aux-configset-${VERSION} -d ./opencga-rga-aux-configset-${VERSION} -z ${ZK_HOST}