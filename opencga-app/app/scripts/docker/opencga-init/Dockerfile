FROM opencga

ENV HBASE_SSH_DNS=""
ENV HBASE_SSH_PASS=""
ENV HBASE_SSH_USER=""
ENV SEARCH_HOSTS=""
ENV CLINICAL_HOSTS=""
ENV CELLBASE_HOSTS=""
ENV CATALOG_DATABASE_HOSTS=""
ENV CATALOG_DATABASE_USER=""
ENV CATALOG_DATABASE_PASSWORD=""
ENV CATALOG_SEARCH_HOSTS=""
ENV CATALOG_SEARCH_USER=""
ENV CATALOG_SEARCH_PASSWORD=""
ENV REST_HOST=""
ENV GRPC_HOST=""
ENV OPENCGA_PASS=""
ENV BATCH_EXEC_MODE=""
ENV BATCH_ACCOUNT_NAME=""
ENV BATCH_ACCOUNT_KEY=""
ENV BATCH_ENDPOINT=""
ENV BATCH_POOL_ID=""
ENV BATCH_DOCKER_ARGS=""
ENV BATCH_DOCKER_IMAGE=""
ENV BATCH_MAX_CONCURRENT_JOBS=""

# Minimum compatible git commit, please only use later commits.
ARG IVA_COMMIT_SHA="dce421b6933d540d9c511c72e63c0e85dff00667"

# Install local dependencies
RUN apk add --no-cache python3 && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip setuptools && \
    if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi && \
    if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python3 /usr/bin/python; fi && \
    rm -r /root/.cache
RUN apk add --update --no-cache openssh \
    sshpass \
    git

# Download and extract the IVA configuration into our local configuration.
RUN git clone "https://github.com/opencb/iva.git" && \
    cd iva && \
    git reset "$IVA_COMMIT_SHA" --hard && \
    mkdir -p /opt/opencga/conf/iva && \
    cp -r src/conf/* /opt/opencga/conf/iva

# Mount volume to copy config into
VOLUME /opt/volume

RUN pip3 install oyaml
COPY ./opencga-app/app/scripts/docker/opencga-init /tmp/
RUN chmod +x /tmp/setup.sh

# It is the responsibility of the setup.sh
# script to initialise the volume correctly
# and apply any runtime config transforms.
ENTRYPOINT [ "/bin/bash","/tmp/setup.sh" ]
