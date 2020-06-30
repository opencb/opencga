ARG TAG
ARG ORG=opencb

FROM $ORG/opencga-base:$TAG

ARG INIT_PATH=cloud/docker/opencga-init/

# Install local dependencies
RUN apk add python3 py3-pip git mongodb && \
    pip3 install --upgrade pip setuptools

# Mount volume to copy config into
VOLUME /opt/volume

COPY ${INIT_PATH} /opt/opencga/init/
COPY ${BUILD_PATH}/conf/* /opt/opencga/init/test/

# Install python requirements
RUN pip3 install -r /opt/opencga/init/requirements.txt
RUN chmod +x /opt/opencga/init/setup.sh

# Run tests on config script
# If this line fails then a configuration change has a bug
# review override-yaml.py or override-js.py
RUN echo ">Running init container configuration tests" && cd /opt/opencga/init/test && python3 test_override_yaml.py -v && rm -r /opt/opencga/init/test

# It is the responsibility of the setup.sh
# script to initialise the volume correctly
# and apply any runtime config transforms.
ENTRYPOINT [ "/bin/sh","/opt/opencga/init/setup.sh" ]
