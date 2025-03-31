FROM ubuntu:22.04

## Run update and install necessary packages
RUN apt-get update -y && \
    ## 1. Install system dependencies
    DEBIAN_FRONTEND="noninteractive" TZ="Europe/London" apt-get install -y libcurl4 git libgmp-dev libcurl4-openssl-dev libgit2-dev build-essential \
    libssl-dev libssh-dev libxml2-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev \
    gnuplot pandoc samtools bcftools tabix fastqc plink1.9 bwa r-base wget libopenblas0-openmp libcholmod3 libsuitesparse-dev python3 python3-pip && \
    apt-get remove libopenblas0-pthread && \
    pip3 install pydantic && \

    ## 2. regenie
    wget -qO- https://github.com/rgcgithub/regenie/releases/download/v4.0/regenie_v4.0.gz_x86_64_Linux.zip | gunzip > /usr/local/bin/regenie && \
    chmod +x /usr/local/bin/regenie && \

    ## 3. Clean up
    rm -rf /var/lib/apt/lists/* /tmp/* && \
    strip --remove-section=.note.ABI-tag /usr/lib/x86_64-linux-gnu/libQt5Core.so.5

WORKDIR /opt/opencga
