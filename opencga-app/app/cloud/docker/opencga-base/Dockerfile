## Based on Debian 11 (bullseye)
FROM openjdk:8-jre

ARG BUILD_PATH="."

ENV OPENCGA_HOME=/opt/opencga
ENV OPENCGA_CONFIG_DIR=${OPENCGA_HOME}/conf

RUN apt-get update && apt-get -y upgrade && apt-get install -y lsb-release sshpass ca-certificates curl gnupg jq ncurses-bin && \
    # Install Docker repository
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
      $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    # Install MongoDB repository
    wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | apt-key add - && \
    echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/4.2 main" | tee /etc/apt/sources.list.d/mongodb-org-4.2.list && \
    apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io mongodb-org-shell && \
    rm -rf /var/lib/apt/lists/* && \
    adduser --disabled-password --uid 1001 opencga

## Run Docker images as non root
USER opencga

COPY --chown=opencga:opencga ${BUILD_PATH} /opt/opencga

## Declare the volume to be mounted later
VOLUME /opt/opencga/conf

WORKDIR /opt/opencga
