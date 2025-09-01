FROM ubuntu:24.04

LABEL org.label-schema.vendor="OpenCB" \
      org.label-schema.name="opencga-ext-tools" \
      org.label-schema.url="http://docs.opencb.org/display/opencga" \
      org.label-schema.description="An Open Computational Genomics Analysis platform for big data processing and analysis in genomics" \
      maintainer="Joaquin Tarraga <joaquintarraga@gmail.com>" \
      org.label-schema.schema-version="1.0"

## Run update and install necessary packages
RUN apt-get -y update && apt-get install -y --no-install-recommends && DEBIAN_FRONTEND="noninteractive" TZ="Europe/London" && \
    ## 1. Install system dependencies
    apt-get install -y git libgit2-dev curl libcurl4 libcurl4-openssl-dev libgmp-dev build-essential libssl-dev libssh-dev wget cmake \
      python3 python3-pip python3-venv libxml2-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev \
      gnuplot pandoc samtools fastqc r-base libopenblas0-openmp libcholmod5 libsuitesparse-dev && \
    apt-get remove libopenblas0-pthread && \
    pip3 install --no-cache-dir --break-system-packages pydantic multiqc && \

    ## 2. Install samtools and bcftools 1.21 and plugins
    wget https://github.com/samtools/samtools/releases/download/1.21/samtools-1.21.tar.bz2 && \
    tar xjvf samtools-1.21.tar.bz2 && \
    cd samtools-1.21 && \
    ./configure --prefix=/usr/local && \
    make && make install && \
    cd .. && \

    ## 3. Installation dependencies using R install.packages() is slower than apt-get but final size is 400MB smaller. \
    ## 3.1. 'tidyverse' installs: ggplot2, tibble, tidyr, readr, purrr, dplyr, stringr, forcats
    R -e "install.packages(c('BiocManager', 'RCircos', 'nnls', 'tidyverse', 'jsonlite', 'getopt', 'optparse', 'knitr', 'configr', 'dplyr', 'rmarkdown', 'httr', 'kinship2', 'limSolve'))" && \
    R -e "BiocManager::install('BiocStyle', ask = FALSE)" && \

    ## 4. Clean up
    rm -rf samtools-1.21 /var/lib/apt/lists/* /tmp/* && \
    strip --remove-section=.note.ABI-tag /usr/lib/x86_64-linux-gnu/libQt5Core.so.5

ENV BCFTOOLS_PLUGINS="/usr/local/bin"

WORKDIR /opt/opencga
