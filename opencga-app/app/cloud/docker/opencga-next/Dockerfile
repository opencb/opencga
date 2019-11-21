FROM openjdk:8-jre-alpine 

ARG buildPath="./build/"

ENV OPENCGA_HOME=/opt/opencga
ENV OPENCGA_CONFIG_DIR=i${OPENCGA_HOME}/conf

RUN apk --no-cache --update add openssh-client sshpass ca-certificates curl && \
    adduser -D -u 1001 opencga -h opencga 

COPY ${buildPath} /opt/opencga
RUN rm -fr /opt/opencga/opencga.war

WORKDIR /opt/opencga/bin
