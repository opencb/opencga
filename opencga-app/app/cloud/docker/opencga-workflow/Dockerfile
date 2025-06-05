### Multi Stage building

## 1. Based on Ubuntu 24.04 (noble)
FROM eclipse-temurin:17.0.15_6-jre-noble AS base

ARG BUILD_PATH="."

ENV OPENCGA_HOME=/opt/opencga
ENV OPENCGA_CONFIG_DIR=${OPENCGA_HOME}/conf

RUN apt-get update && apt-get -y upgrade && apt-get install -y lsb-release sshpass ca-certificates curl gnupg jq unzip vim htop && \
    ## Install Docker repository
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu "$(. /etc/os-release && echo "noble")" stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io && \

    ## Install yq
    wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 && \
    chmod +x /usr/local/bin/yq && \

    ## Install Nextflow \
    wget -qO- https://github.com/nextflow-io/nextflow/releases/download/v25.04.2/nextflow-25.04.2-dist | bash && \
    chmod +xr nextflow && \
    mv nextflow /usr/local/bin/ && \

    ## Clean downloaded packages to make images smaller
    rm -rf /var/lib/apt/lists/* && \
    adduser --disabled-password --uid 1001 opencga && \

    # Add docker group to opencga user
    usermod -a -G docker opencga

COPY --chown=opencga:opencga ${BUILD_PATH} ${OPENCGA_HOME}


## 2. Based on base
FROM base
RUN apt-get update && apt-get -y upgrade && apt-get install -y python3 python3-pip python3-venv r-base samtools bcftools && \
    python3 -m venv /opt/venv

# Make the virtual environment's Python the default
# Add virtual environment bin directory to PATH
ENV PATH="/opt/venv/bin:$PATH"

RUN pip3 install ${OPENCGA_HOME}/clients/python && \
    ## Add .bashrc configuration to activate the virtual environment on login
    echo 'source /opt/venv/bin/activate' >> ~/.bashrc && \

    ## Clean downloaded packages to make images smaller
    rm -rf /var/lib/apt/lists/*

USER opencga
## Declare the volume to be mounted later
VOLUME ${OPENCGA_CONFIG_DIR}

WORKDIR ${OPENCGA_HOME}
