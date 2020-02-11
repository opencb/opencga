# TODO: Currently building from @j-coll's development image. Update once release available.
FROM jcoll/opencga-next:2.0.0-hdp3.1-dev   

# Install local dependencies
RUN echo 'http://dl-cdn.alpinelinux.org/alpine/v3.6/main' >> /etc/apk/repositories && echo 'http://dl-cdn.alpinelinux.org/alpine/v3.6/community' >> /etc/apk/repositories && apk update && apk add  python3 py3-pip git mongodb && \
    pip3 install --upgrade pip setuptools

# Mount volume to copy config into
VOLUME /opt/volume

COPY . /tmp/

# Install python requirements
RUN pip3 install -r /tmp/requirements.txt
RUN chmod +x /tmp/setup.sh

# Run tests on config script
# If this line fails then a configuration change has a bug
# review override-yaml.py or override-js.py
# TODO: why in base does this start with i
ENV OPENCGA_CONFIG_DIR=${OPENCGA_HOME}/conf
RUN echo ">Running init container configuration tests" && cd /tmp/test && python3 test_override_yaml.py -v 
#&& rm -r /tmp/test

# It is the responsibility of the setup.sh
# script to initialise the volume correctly
# and apply any runtime config transforms.
ENTRYPOINT [ "/bin/sh","/tmp/setup.sh" ]
