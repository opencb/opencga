FROM debian:bullseye

LABEL org.label-schema.vendor="OpenCB" \
      org.label-schema.name="opencga-r-builder" \
      org.label-schema.url="http://docs.opencb.org/display/opencga" \
      org.label-schema.description="An Open Computational Genomics Analysis platform for big data processing and analysis in genomics" \
      maintainer="Joaquin Tarraga <joaquintarraga@gmail.com>" \
      org.label-schema.schema-version="1.0"

# run update and install necessary packages
RUN apt-get update -y && DEBIAN_FRONTEND="noninteractive" TZ="Europe/London" apt-get install -y \
    pandoc libcurl4 libssl-dev libcurl4-openssl-dev \
    r-base && \
    ## Installation dependencies using R install.packages() is slower than apt-get but final size is 400GB smaller.
    R -e "install.packages(c('BiocManager', 'nnls', 'ggplot2', 'jsonlite', 'optparse', 'knitr', 'configr', 'dplyr', 'rmarkdown', 'tidyr', 'httr'))" && \
    R -e "BiocManager::install('BiocStyle')" && \
    rm -rf /var/lib/apt/lists/* /tmp/*

WORKDIR /opt/opencga