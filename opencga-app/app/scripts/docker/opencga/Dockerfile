FROM openjdk:8-jre-alpine

ARG buildPath="./build"

WORKDIR /opt/opencga

RUN adduser -S -u 1001 opencga

RUN apk update && \
    apk add ca-certificates && \
    update-ca-certificates

COPY ${buildPath} /opt/opencga