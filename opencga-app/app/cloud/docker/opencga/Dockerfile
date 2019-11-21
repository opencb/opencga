FROM ubuntu:16.04

RUN apt-get update && apt-get install -y software-properties-common && \
    apt-get install -y openjdk-8-jre openssh-client sshpass && \
    java -version && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean;

RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

ARG buildPath="./build"

WORKDIR /opt/opencga
ENV BASEDIR=/opt/opencga
ENV OPENCGA_CONFIG_DIR=/opt/opencga/conf

RUN useradd -u 1001 -m opencga

COPY ${buildPath} /opt/opencga

ENTRYPOINT [ "/bin/bash" ]
