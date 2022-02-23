ARG TAG
ARG ORG=opencb

FROM $ORG/opencga-base:$TAG

ARG INIT_PATH=cloud/docker/opencga-init/

COPY ${INIT_PATH} /opt/opencga/init/
COPY ${BUILD_PATH}/conf/* /opt/opencga/init/test/


# Mount volume to copy config into
VOLUME /opt/volume

USER root
# Install local dependencies
RUN apt install python3 && \
    echo "deb http://ftp.de.debian.org/debian bullseye main" | tee -a /etc/apt/sources.list.d/sources.list && \
    apt-get update && apt-get -y upgrade && \
    apt install -y python3-pip && \
    pip3 install --upgrade pip setuptools && \
    pip3 install -r /opt/opencga/init/requirements.txt && \
    chmod +x /opt/opencga/init/setup.sh /opt/opencga/init/setup-hadoop.sh && \
    echo ">Running init container configuration tests" && \
    cd /opt/opencga/init/test && python3 test_override_yaml.py -v && rm -r /opt/opencga/init/test


USER opencga
# It is the responsibility of the setup.sh
# script to initialise the volume correctly
# and apply any runtime config transforms.
ENTRYPOINT [ "/bin/sh","/opt/opencga/init/setup.sh" ]
