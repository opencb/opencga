## Based on Ubuntu 22.04 (jammy)
## We are now using OpenJDK 8u372 to support "cgroup v2", see https://developers.redhat.com/articles/2023/04/19/openjdk-8u372-feature-cgroup-v2-support#
FROM eclipse-temurin:8u372-b07-jre-jammy

ARG BUILD_PATH="."

ENV OPENCGA_HOME=/opt/opencga
ENV OPENCGA_CONFIG_DIR=${OPENCGA_HOME}/conf

RUN apt-get update && apt-get -y upgrade && apt-get install -y lsb-release sshpass ca-certificates curl gnupg jq ncurses-bin unzip && \
    ## Install Docker repository
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu "$(. /etc/os-release && echo "jammy")" stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    ## Install MongoDB 6.0 repository
    curl -fsSL https://pgp.mongodb.com/server-6.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-server-6.0.gpg && \
    echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-6.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/6.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list && \
    ## Install docker and mongodb packages
    apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io mongodb-mongosh && \
    ## Install yq
    wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 && \
    chmod +x /usr/local/bin/yq && \
    ## Clean downloaded packages to make images smaller
    rm -rf /var/lib/apt/lists/* && \
    adduser --disabled-password --uid 1001 opencga

## Run Docker images as non root
USER opencga

COPY --chown=opencga:opencga ${BUILD_PATH} /opt/opencga

## Declare the volume to be mounted later
VOLUME /opt/opencga/conf

WORKDIR /opt/opencga
