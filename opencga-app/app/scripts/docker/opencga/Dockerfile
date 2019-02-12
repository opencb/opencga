FROM ubuntu:16.04

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections

RUN apt-get update && apt-get install -y software-properties-common && \
    add-apt-repository ppa:webupd8team/java && apt-get update && \
    apt-get install -y oracle-java8-installer && \
    java -version && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

ARG buildPath="./build"
ENV JAVA_HOME="/usr/lib/jvm/java-8-oracle"
ENV BASEDIR=/opt/opencga

WORKDIR /opt/opencga

RUN adduser --disabled-password --gecos "" -u 1001 opencga

COPY ${buildPath} /opt/opencga

ENTRYPOINT [ "/bin/bash" ]
