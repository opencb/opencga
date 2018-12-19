FROM opencga

ARG OPENCGA_VERSION="1.4.0-rc3-dev"

# Get Tomcat
ARG TOMCAT_VERSION="8.5.35"

RUN wget --quiet --no-cookies http://www-eu.apache.org/dist/tomcat/tomcat-8/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz -O /tmp/tomcat.tgz && \
tar xzvf /tmp/tomcat.tgz -C /opt && \
mv /opt/apache-tomcat-${TOMCAT_VERSION} /opt/tomcat && \
rm /tmp/tomcat.tgz && \
rm -rf /opt/tomcat/webapps/examples && \
rm -rf /opt/tomcat/webapps/docs && \
rm -rf /opt/tomcat/webapps/ROOT

# copy opencga build to tomcat server  - permissions??
RUN cp  /opt/opencga/opencga-${OPENCGA_VERSION}.war /opt/tomcat/webapps/opencga-${OPENCGA_VERSION}.war &&  chmod 777 /opt/opencga/

# copy tomcat config
COPY ./opencga.xml /var/lib/tomcat8/conf/Catalina/localhost/opencga-${OPENCGA_VERSION}.xml

# Mount the volume at arbitary location and then
# symlink expected location to the shared volume.
VOLUME /opt/volume
RUN mkdir -p /opt/opencga/ && \
    rm -r /opt/opencga/conf  && \
    ln -sfn /opt/volume/conf /opt/opencga/conf && \
    ln -sfn /opt/volume/sessions /opt/opencga/sessions

# Launch Tomcat
CMD ["sh", "-c", "/tmp/config.sh && /opt/tomcat/bin/catalina.sh run"]

EXPOSE 8080
