FROM debian:bullseye

LABEL org.label-schema.vendor="OpenCB" \
      org.label-schema.name="opencga-ext-tools" \
      org.label-schema.url="http://docs.opencb.org/display/opencga" \
      org.label-schema.description="An Open Computational Genomics Analysis platform for big data processing and analysis in genomics" \
      maintainer="Joaquin Tarraga <joaquintarraga@gmail.com>" \
      org.label-schema.schema-version="1.0"

WORKDIR /opt/opencga/signature.tools.lib

# run update and install necessary packages
RUN apt-get update -y && DEBIAN_FRONTEND="noninteractive" TZ="Europe/London" apt-get install -y \
    libcurl4 git libgmp-dev libcurl4-openssl-dev libgit2-dev libssl-dev libssh-dev libxml2-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev \
    libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev \
    gnuplot pandoc samtools bcftools fastqc plink1.9 bwa r-base && \
    ## Installation dependencies using R install.packages() is slower than apt-get but final size is 400GB smaller.
    R -e "install.packages(c('BiocManager', 'RCircos', 'nnls', 'ggplot2', 'jsonlite', 'optparse', 'knitr', 'configr', 'dplyr', 'rmarkdown', 'tidyr', 'httr'))" && \
    R -e "BiocManager::install('BiocStyle')" && \
    ## signature.tools.lib installation
    R -e 'install.packages(c("devtools", "getopt"), repos="https://www.stats.bris.ac.uk/R/")' && \
    git clone https://github.com/Nik-Zainal-Group/signature.tools.lib.git /opt/opencga/signature.tools.lib && \
    git checkout d3d73db497b5b83abc55d6cd55840c34ed542628 && \
    sed -i '/BSgenome/d' DESCRIPTION && \
    R -e 'devtools::install(repos="https://www.stats.bris.ac.uk/R/")' && \
    ## Clean up
    rm -rf /var/lib/apt/lists/* /tmp/* /opt/opencga/signature.tools.lib/.git && \
    strip --remove-section=.note.ABI-tag /usr/lib/x86_64-linux-gnu/libQt5Core.so.5

WORKDIR /opt/opencga
