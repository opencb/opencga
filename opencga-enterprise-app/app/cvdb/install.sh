

ENTERPRISE_VERSION=REPLACEME_ENTERPRISE_VERSION

echo "Install OpenCGA Enterprise configsets from ${OPENCGA_HOME}/misc/solr"

${SOLR_HOME}/bin/solr zk upconfig -n opencga-ca-configset-${ENTERPRISE_VERSION} -d ./opencga-ca-configset-${ENTERPRISE_VERSION} -z ${ZK_HOST}
${SOLR_HOME}/bin/solr zk upconfig -n opencga-ci-configset-${ENTERPRISE_VERSION} -d ./opencga-ci-configset-${ENTERPRISE_VERSION} -z ${ZK_HOST}
${SOLR_HOME}/bin/solr zk upconfig -n opencga-cv-configset-${ENTERPRISE_VERSION} -d ./opencga-cv-configset-${ENTERPRISE_VERSION} -z ${ZK_HOST}
${SOLR_HOME}/bin/solr zk upconfig -n opencga-cve-configset-${ENTERPRISE_VERSION} -d ./opencga-cve-configset-${ENTERPRISE_VERSION} -z ${ZK_HOST}

