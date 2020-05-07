FROM opencga

# Minimum compatible IVA release is v1.0.0-rc1, please only use this or later releases.
ARG IVA_TAG="v1.0.0-rc1"

# Install local dependencies
RUN apt-get update && apt-get install -y python3 python3-pip git && \
    pip3 install --upgrade pip setuptools && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean


# Download and extract the IVA configuration into our local configuration.
RUN git clone "https://github.com/opencb/iva.git" && \
    cd iva && \
    git checkout tags/"$IVA_TAG"  && \
    mkdir -p /opt/opencga/ivaconf && \
    cp -r src/conf/* /opt/opencga/ivaconf

# Mount volume to copy config into
VOLUME /opt/volume

COPY opencga-app/app/cloud/docker/opencga-init/ /tmp/
# Install python requirements
RUN pip3 install -r /tmp/requirements.txt
RUN chmod +x /tmp/setup.sh

# Run tests on config script
# If this line fails then a configuration change has a bug
# review override-yaml.py or override-js.py
RUN echo ">Running init container configuration tests" && cd /tmp/test && python3 test-override-yaml.py -v && python3 test-override-js.py -v && rm -r /tmp/test

# It is the responsibility of the setup.sh
# script to initialise the volume correctly
# and apply any runtime config transforms.
ENTRYPOINT [ "/bin/bash","/tmp/setup.sh" ]
