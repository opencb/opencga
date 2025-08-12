ARG TAG=5.0.0-SNAPSHOT
ARG ORG=opencb

FROM $ORG/opencga-ext-tools:$TAG

LABEL org.label-schema.vendor="OpenCB" \
      org.label-schema.name="opencga-transcriptomics" \
      org.label-schema.url="http://docs.opencb.org/display/opencga" \
      org.label-schema.description="An Open Computational Genomics Analysis platform for big data processing and analysis in genomics" \
      maintainer="Joaquin Tarraga <joaquintarraga@gmail.com>" \
      org.label-schema.schema-version="1.0"

# Install necessary system dependencies for MultiQC
# These are often needed for Python scientific packages
RUN apt-get update && apt-get install -y --no-install-recommends && \
    ## 1. apt-get and python installs

    ## 2. STAR
    wget https://github.com/alexdobin/STAR/archive/2.7.11b.tar.gz && \
    tar -xzf 2.7.11b.tar.gz && \
    cd STAR-2.7.11b/source && \
    make STAR && \

    ## 3. HISAT2

    ## 3. Installation dependencies using R install.packages() is slower than apt-get but final size is 400MB smaller.
#    R -e "install.packages(c('...'))" && \
#    R -e "BiocManager::install('BiocStyle')" && \

    && rm -rf STAR-2.7.11b /var/lib/apt/lists/*

# Set the PATH to include the virtual environment's bin directory
# This makes multiqc, pip, etc. available automatically
#ENV PATH="/app/multiqc_venv/bin:$PATH"

WORKDIR /opt/opencga

