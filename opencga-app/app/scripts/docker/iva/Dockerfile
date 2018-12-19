FROM httpd

# Install dependencies
RUN apt-get update && \
apt-get install -y wget

ARG IVA_VERSION="1.0.0-beta"

RUN wget --quiet "https://github.com/opencb/iva/releases/download/v${IVA_VERSION}/iva-${IVA_VERSION}.tar.gz" && tar xvf iva-${IVA_VERSION}.tar.gz --strip-components=1 -C /usr/local/apache2/htdocs/  && rm ./iva-${IVA_VERSION}.tar.gz

# Mount the volume at arbitary location and then
# symlink expected location to the shared volume.
VOLUME /opt/volume
RUN mkdir -p usr/local/apache2/htdocs/ && \
    ln -sf /opt/volume/conf usr/local/apache2/htdocs/conf