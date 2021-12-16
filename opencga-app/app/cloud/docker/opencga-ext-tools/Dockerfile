FROM debian:11

LABEL org.label-schema.vendor="OpenCB" \
      org.label-schema.name="opencga-ext-tools" \
      org.label-schema.url="http://docs.opencb.org/display/opencga" \
      org.label-schema.description="An Open Computational Genomics Analysis platform for big data processing and analysis in genomics" \
      maintainer="Joaquin Tarraga <joaquintarraga@gmail.com>" \
      org.label-schema.schema-version="1.0"

# run update and install necessary packages
RUN apt-get update -y && DEBIAN_FRONTEND="noninteractive" TZ="Europe/London" apt-get install -y \
    gnuplot  \
    samtools \
    bcftools \
    fastqc \
    plink1.9 \
    bwa \
    picard \
    r-base && \
    R -e "install.packages(c('nnls', 'ggplot2', 'jsonlite', 'optparse', 'BiocManager', 'RCircos'))" && \
    rm -rf /var/lib/apt/lists/* /tmp/*