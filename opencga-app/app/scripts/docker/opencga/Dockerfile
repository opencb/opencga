FROM ubuntu:16.04

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections

RUN apt-get update && apt-get install -y software-properties-common && \
    add-apt-repository ppa:webupd8team/java && apt-get update && \
    apt-get install -y oracle-java8-installer openssh-client sshpass && \
    java -version && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

ARG buildPath="./build"

WORKDIR /opt/opencga
ENV BASEDIR=/opt/opencga

RUN adduser -u 1001 opencga

COPY ${buildPath} /opt/opencga

ENTRYPOINT [ "/bin/bash" ]
