## Based on Ubuntu 22.04 (jammy)
FROM eclipse-temurin:11.0.24_8-jre-jammy

ARG BUILD_PATH="."

RUN apt-get update && apt-get -y upgrade && apt-get install -y lsb-release sshpass ca-certificates curl gnupg jq unzip vim htop && \
    ## Install Docker repository
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu "$(. /etc/os-release && echo "jammy")" stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io && \
    ## Install yq
    wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 && \
    chmod +x /usr/local/bin/yq && \
    ## Install Nextflow \
    curl -s https://get.nextflow.io | bash && \
    chmod +xr nextflow && \
    mv nextflow /usr/local/bin/ && \
    ## Clean downloaded packages to make images smaller
    rm -rf /var/lib/apt/lists/* && \
    adduser --disabled-password --uid 1001 opencga && \
    # Add docker group to opencga user
    usermod -a -G docker opencga

## Run Docker images as opencga user
USER opencga

WORKDIR /home/opencga
