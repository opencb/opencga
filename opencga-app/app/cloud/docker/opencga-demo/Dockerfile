ARG TAG
ARG ORG=opencb

FROM $ORG/opencga-init:$TAG

ARG SOLR_VERSION="8.4.0"

# install mongodb, Solr
RUN apk update && \
    apk upgrade && \
    apk add --no-cache bash && \
    apk add --no-cache mongodb && \ 
    cd /opt/ && \
    wget http://archive.apache.org/dist/lucene/solr/${SOLR_VERSION}/solr-${SOLR_VERSION}.tgz && \ 
    tar -zxf solr-${SOLR_VERSION}.tgz && \
    rm -r solr-${SOLR_VERSION}.tgz && \
    ln -s solr-* solr

VOLUME ["/data/opencga/mongodb", "/data/opencga/solr", "/opt/opencga/variants", "/opt/opencga/sessions"]
EXPOSE 27017 28017

WORKDIR /opt/scripts
# Copy init.sh which perform initialization setps.
COPY ${BUILD_PATH}/cloud/docker/opencga-demo/init.sh init.sh
COPY ${BUILD_PATH}/cloud/docker/opencga-demo/mongo-cluster-init.js mongo-cluster-init.js

WORKDIR /opt/opencga/bin
ENTRYPOINT ["/bin/bash", "-c", "/opt/scripts/init.sh"]
