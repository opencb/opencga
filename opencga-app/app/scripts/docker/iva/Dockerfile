FROM httpd

# Install dependencies
RUN apt-get update && \
apt-get install -y wget

ARG IVA_VERSION="1.0.0-beta"

RUN wget --quiet "https://github.com/opencb/iva/releases/download/v${IVA_VERSION}/iva-${IVA_VERSION}.tar.gz" && tar xvf iva-${IVA_VERSION}.tar.gz --strip-components=1 -C /usr/local/apache2/htdocs/  && rm ./iva-${IVA_VERSION}.tar.gz

VOLUME usr/local/apache2/htdocs/conf
