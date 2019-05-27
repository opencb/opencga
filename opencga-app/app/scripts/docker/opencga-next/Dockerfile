FROM openjdk:8-jre-alpine 

ARG buildPath="./build/"

WORKDIR /opt/opencga
ENV BASEDIR=/opt/opencga
ENV OPENCGA_CONFIG_DIR=/opt/opencga/conf

RUN adduser -D -u 1001 opencga -h opencga 

COPY ${buildPath} /opt/opencga
RUN rm -fr /opt/opencga/test

