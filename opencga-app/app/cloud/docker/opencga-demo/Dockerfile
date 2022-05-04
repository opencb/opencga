ARG TAG
ARG ORG=opencb

FROM $ORG/opencga-init:$TAG

ARG SOLR_VERSION="8.4.0"

# install mongodb, Solr
# Upgrade twice, to ensure that openjdk8-jre-lib and openjdk8-jre-base are in sync
# See https://github.com/opencb/opencga/issues/1653
RUN apk update && \
    apk upgrade && \
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
EXPOSE 9090 9090

# Copy init.sh which perform initialization setps.
COPY ${BUILD_PATH}/cloud/docker/opencga-demo/init.sh ${BUILD_PATH}/cloud/docker/opencga-demo/mongo-cluster-init.js ${BUILD_PATH}/cloud/docker/opencga-demo/load-demo.sh /opt/opencga/demo/
#COPY ${BUILD_PATH}/cloud/docker/opencga-demo/mongo-cluster-init.js /opt/opencga/demo/mongo-cluster-init.js
RUN chmod +x /opt/opencga/demo/*

WORKDIR /opt/opencga/bin
ENTRYPOINT ["/bin/bash", "-c", "/opt/opencga/demo/init.sh"]
