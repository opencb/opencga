FROM opencga-next

ARG SOLR_VERSION="6.6.0"

# install mongodb, Solr, update config file
RUN apk update && \
    apk upgrade && \
    apk add --no-cache bash && \
    apk add --no-cache mongodb && \ 
    cd /opt/ && \
    wget http://archive.apache.org/dist/lucene/solr/${SOLR_VERSION}/solr-${SOLR_VERSION}.tgz && \ 
    tar -zxvf solr-${SOLR_VERSION}.tgz && \
    rm -r solr-${SOLR_VERSION}.tgz && \
    sed -i 's/:8080/:9090/g' /opt/opencga/conf/client-configuration.yml  && \
    sed -i 's/cloud/standalone/g' /opt/opencga/conf/storage-configuration.yml  && \
    sed -i 's/insertBatchSize: 10000/insertBatchSize: 500/g' /opt/opencga/conf/storage-configuration.yml  && \
    wget http://docs.opencb.org/download/attachments/9240577/OpenCGAConfSet-1.4.0.tar.gz && \
    tar -zxvf OpenCGAConfSet-1.4.0.tar.gz && \
    rm -r OpenCGAConfSet-1.4.0.tar.gz && \
    cp OpenCGAConfSet-1.4.0 /opt/solr-*/server/solr/configsets/ -a && \
    sed -i 's/<dataDir>\${solr.data.dir:}<\/dataDir>/<dataDir>\/data\/opencga\/solr\/\${solr.core.name}<\/dataDir>/g' /opt/solr-6.6.0/server/solr/configsets/OpenCGAConfSet-1.4.0/conf/solrconfig.xml


VOLUME ["/data/opencga/mongodb", "/data/opencga/solr", "/opt/opencga/variants", "/opt/opencga/sessions"]
EXPOSE 27017 28017

WORKDIR /opt/opencga/bin

# Copy init.sh which perform initialization setps.
COPY opencga-app/app/scripts/docker/opencga-demo/init.sh init.sh
ENTRYPOINT ["/bin/bash", "-c", "./init.sh"]


