FROM opencga

# Build args
ARG TOMCAT_VERSION="8.5.38"
ARG VERSION=""
ARG VCS_URL=""
ARG VCS_REF=""
ARG BUILD_DATE=""

# Metadata
LABEL org.label-schema.vendor="OpenCB" \
      org.label-schema.url="http://docs.opencb.org/" \
      org.label-schema.name="OpenCGA" \
      org.label-schema.description="An Open Computational Genomics Analysis platform for big data processing and analysis in genomics" \
      org.label-schema.version=${VERSION} \
      org.label-schema.vcs-url=${VCS_URL} \
      org.label-schema.vcs-ref=${VCS_REF} \
      org.label-schema.build-date=${BUILD_DATE} \
      org.label-schema.docker.schema-version="1.0"

# Install local dependencies
RUN apt-get update && apt-get install -y wget tar \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get clean

# Download and install Tomcat
RUN wget --quiet --no-cookies https://archive.apache.org/dist/tomcat/tomcat-8/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz -O /tmp/tomcat.tgz && \
tar xzvf /tmp/tomcat.tgz -C /opt && \
mv /opt/apache-tomcat-${TOMCAT_VERSION} /opt/tomcat && \
rm /tmp/tomcat.tgz && \
rm -rf /opt/tomcat/webapps/examples && \
rm -rf /opt/tomcat/webapps/docs && \
rm -rf /opt/tomcat/webapps/ROOT && \
chown -R 1001:1001 /opt/tomcat/

# Copy opencga build to Tomcat server
RUN cp /opt/opencga/*.war /opt/tomcat/webapps/opencga.war

# Copy OpenCGA config to Tomcat
COPY ./opencga-app/app/scripts/docker/opencga-app/opencga.xml /opt/tomcat/conf/Catalina/localhost/opencga.xml

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions

USER 1001:1001

HEALTHCHECK --interval=20m --timeout=3s \
    CMD  wget -q http://localhost:8080/opencga/webservices/rest/v1/meta/status || killall java

EXPOSE 8080
EXPOSE 8443

# Launch Tomcat
CMD ["/opt/tomcat/bin/catalina.sh","run"]