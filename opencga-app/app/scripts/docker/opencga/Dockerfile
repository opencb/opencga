FROM openjdk:11.0.1-slim-stretch

ARG buildPath="./build"

WORKDIR /opt/opencga
ENV BASEDIR=/opt/opencga

RUN adduser -u 1001 opencga

COPY ${buildPath} /opt/opencga

ENTRYPOINT [ "/bin/bash" ]