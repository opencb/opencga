ARG TAG
ARG ORG=opencb

FROM $ORG/opencga-init:$TAG

ARG SOLR_VERSION="8.4.0"


VOLUME ["/data/opencga/mongodb", "/data/opencga/solr", "/opt/opencga/variants", "/opt/opencga/sessions"]
EXPOSE 27017 28017
EXPOSE 9090 9090

# Copy init.sh which perform initialization setps.
COPY ${BUILD_PATH}/cloud/docker/opencga-demo/init.sh /opt/scripts/init.sh
COPY ${BUILD_PATH}/cloud/docker/opencga-demo/mongo-cluster-init.js /opt/scripts/mongo-cluster-init.js
USER root
RUN chmod +x /opt/scripts/init.sh
USER opencga

WORKDIR /opt/opencga/bin
ENTRYPOINT ["/bin/bash", "-c", "/opt/opencga/demo/init.sh"]
