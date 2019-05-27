FROM opencb/opencga-next

ARG SOLR_VERSION="6.6.0"

# install mongodb, Solr, update config file
RUN apk update && apk upgrade && apk add --no-cache bash && \
    apk add --no-cache mongodb && \ 
    cd /opt/ && \
    wget http://archive.apache.org/dist/lucene/solr/${SOLR_VERSION}/solr-${SOLR_VERSION}.tgz && \ 
    tar -zxvf solr-${SOLR_VERSION}.tgz && rm -r solr-${SOLR_VERSION}.tgz && \
    sed -i 's/:8080/:9090/g' /opt/opencga/conf/client-configuration.yml  # change client config value as REST only runs on 9090 as of now.

VOLUME ["/data/db"]
EXPOSE 27017 28017

#TODO: copy Solr config files    
WORKDIR /opt/opencga/bin

# Copy init.sh which perform initialization setps.
COPY init.sh init.sh
ENTRYPOINT ["/bin/bash", "-c", "./init.sh"]

