FROM openjdk:8-jre-alpine

ARG buildPath="./build"

WORKDIR /opt/opencga
ENV BASEDIR=/opt/opencga

RUN adduser -S -u 1001 opencga

RUN apk update && \
    apk add bash ca-certificates && \
    update-ca-certificates

COPY ${buildPath} /opt/opencga


ENTRYPOINT [ "/bin/bash" ]