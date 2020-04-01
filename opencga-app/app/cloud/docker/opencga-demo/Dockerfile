ARG TAG
ARG ORG=opencb

FROM $ORG/opencga-init:$TAG

ARG SOLR_VERSION="8.4.0"

# install mongodb, Solr, update config file
RUN apk update && \
    apk upgrade && \
    apk add --no-cache bash && \
    apk add --no-cache mongodb && \ 
    cd /opt/ && \
    wget http://archive.apache.org/dist/lucene/solr/${SOLR_VERSION}/solr-${SOLR_VERSION}.tgz && \ 
    tar -zxvf solr-${SOLR_VERSION}.tgz && \
    rm -r solr-${SOLR_VERSION}.tgz && \
    cp /opt/opencga/misc/solr/opencga-variant-configset-* /opt/solr-*/server/solr/configsets/ -a && \
    sed -i 's/<dataDir>\${solr.data.dir:}<\/dataDir>/<dataDir>\/data\/opencga\/solr\/\${solr.core.name}<\/dataDir>/g' /opt/solr-${SOLR_VERSION}/server/solr/configsets/opencga-variant-configset*/conf/solrconfig.xml

VOLUME ["/data/opencga/mongodb", "/data/opencga/solr", "/opt/opencga/variants", "/opt/opencga/sessions"]
EXPOSE 27017 28017

WORKDIR /opt/scripts
# Copy init.sh which perform initialization setps.
COPY ${BUILD_PATH}/cloud/docker/opencga-demo/init.sh init.sh
COPY ${BUILD_PATH}/cloud/docker/opencga-demo/mongo-cluster-init.js mongo-cluster-init.js

WORKDIR /opt/opencga/bin
ENTRYPOINT ["/bin/bash", "-c", "/opt/scripts/init.sh"]
