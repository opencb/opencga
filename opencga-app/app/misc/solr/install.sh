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
${SOLR_HOME}/bin/solr zk upconfig -n opencga-cohort-configset-${VERSION} -d ./opencga-cohort-configset-${VERSION} -z ${ZK_HOST}
${SOLR_HOME}/bin/solr zk upconfig -n opencga-family-configset-${VERSION} -d ./opencga-family-configset-${VERSION} -z ${ZK_HOST}
${SOLR_HOME}/bin/solr zk upconfig -n opencga-file-configset-${VERSION} -d ./opencga-file-configset-${VERSION} -z ${ZK_HOST}
${SOLR_HOME}/bin/solr zk upconfig -n opencga-individual-configset-${VERSION} -d ./opencga-individual-configset-${VERSION} -z ${ZK_HOST}
${SOLR_HOME}/bin/solr zk upconfig -n opencga-sample-configset-${VERSION} -d ./opencga-sample-configset-${VERSION} -z ${ZK_HOST}
${SOLR_HOME}/bin/solr zk upconfig -n opencga-job-configset-${VERSION} -d ./opencga-job-configset-${VERSION} -z ${ZK_HOST}