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
rm -rf /opt/tomcat/webapps/ROOT && \
chown -R 1001:1001 /opt/tomcat/

# copy opencga build to tomcat server  - permissions??
RUN cp /opt/opencga/opencga-${OPENCGA_VERSION}.war /opt/tomcat/webapps/opencga-${OPENCGA_VERSION}.war &&  chmod 777 /opt/opencga/

# copy tomcat config
COPY ./opencga.xml /opt/tomcat/conf/Catalina/localhost/opencga-${OPENCGA_VERSION}.xml

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions

USER 1001:1001

EXPOSE 8080
EXPOSE 8443

# Launch Tomcat
CMD ["/opt/tomcat/bin/catalina.sh", "run"]