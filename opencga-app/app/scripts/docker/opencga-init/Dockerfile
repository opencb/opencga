FROM opencga

RUN apt-get update && apt-get install sshpass -y

ENV HD_INSIGHTS_SSH_DNS=""

ENV HD_INSIGHTS_SSH_PASS=""

ENV HD_INSIGHTS_SSH_USER=""

ARG IVA_VERSION="1.0.0-beta"

# Download and extract the IVA configuration
# into our local configuration.
RUN wget "https://github.com/opencb/iva/releases/download/v${IVA_VERSION}/iva-${IVA_VERSION}.tar.gz" && \
    mkdir -p iva /opt/opencga/conf/iva && \
    tar xvfz "iva-${IVA_VERSION}.tar.gz" -C iva --strip-components 1 && \
    cp -r iva/conf/* /opt/opencga/conf/iva

VOLUME /opt/volume

COPY ./setup.sh /tmp/

RUN chmod +x /tmp/setup.sh

# It is the responsibility of the setup.sh
# script to initialise the volume correctly
# and apply any runtime config transforms.
CMD ["sh", "-c", "/tmp/setup.sh"]
