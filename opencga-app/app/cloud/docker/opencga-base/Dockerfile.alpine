FROM openjdk:8-jre-alpine3.9

ARG BUILD_PATH="."

ENV OPENCGA_HOME=/opt/opencga
ENV OPENCGA_CONFIG_DIR=${OPENCGA_HOME}/conf

RUN echo 'http://dl-cdn.alpinelinux.org/alpine/v3.9/main' >> /etc/apk/repositories && echo 'http://dl-cdn.alpinelinux.org/alpine/v3.9/community' >> /etc/apk/repositories

RUN apk --no-cache --update add openssh-client sshpass ca-certificates curl docker jq ncurses vim && \
    adduser -D -u 1001 opencga -h /home/opencga

COPY ${BUILD_PATH} /opt/opencga
RUN chown -R 1001 /opt/opencga/

WORKDIR /opt/opencga/bin
