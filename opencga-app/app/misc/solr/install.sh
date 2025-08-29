#!/usr/bin/env bash

set -e

ZK_HOST=${1:-localhost:9983}
SOLR_HOME=${2:-/opt/solr}
OPENCGA_HOME=${3:-/opt/opencga}

echo "Install configsets from ${OPENCGA_HOME}/misc/solr"
echo "ZK_HOST=${ZK_HOST}"
echo "SOLR_HOME=${SOLR_HOME}"

cd ${OPENCGA_HOME}/misc/solr

## Install configsets
