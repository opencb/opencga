ARG ORG=opencb
ARG TAG=5.0.0-SNAPSHOT

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
    apt-get install -y xxd xorg-dev libxt-dev libcairo2-dev libpango1.0-dev cmake \
      perl libarchive-extract-perl libjson-perl liblwp-protocol-https-perl libtry-tiny-perl liburi-perl && \

    ## 2. Installation dependencies using R install.packages() is slower than apt-get but final size is 400MB smaller.
#    R -e "BiocManager::install('BiocStyle')" && \
    R -e "install.packages(c('ranger'))" && \

    ## 3. STAR
    wget https://github.com/alexdobin/STAR/archive/2.7.11b.tar.gz && \
    tar -xzf 2.7.11b.tar.gz && \
    cd STAR-2.7.11b/source && \
    make STAR && \
    cp -v STAR /usr/local/bin/ && \
    # Go back two levels, since we were in 'source' dir.
    cd ../..  && \

    ## STAR Fusion \
    wget https://github.com/STAR-Fusion/STAR-Fusion/releases/download/STAR-Fusion-v1.15.1/STAR-Fusion-v1.15.1.FULL.tar.gz && \
    tar -xzf STAR-Fusion-v1.15.1.FULL.tar.gz && \
	cd STAR-Fusion-v1.15.1 && \
	make && \
    cp -v /opt/opencga/STAR-Fusion-v1.15.1/STAR-Fusion /usr/local/bin/ && \
    cp -r /opt/opencga/STAR-Fusion-v1.15.1/FusionInspector /usr/local/bin && \
    cp -r /opt/opencga/STAR-Fusion-v1.15.1/util /usr/local/bin && \
    cp -r /opt/opencga/STAR-Fusion-v1.15.1/PerlLib /usr/local/bin && \
    cd .. && \

    ## 4. HISAT2
    wget -O hisat2-source.zip https://github.com/DaehwanKimLab/hisat2/archive/v2.2.1.zip && \
    unzip hisat2-source.zip && \
    cd hisat2-2.2.1 && \
    make && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    cd .. && \

    ## 5. edgeR and DESeq2 (using Bioconductor 3.18 for R 4.3.1)
    R -e "BiocManager::install('DESeq2', ask = FALSE)" && \
    R -e "BiocManager::install('edgeR', ask = FALSE)" && \
#    Rscript -e " \
#      if (!requireNamespace('BiocManager', quietly = TRUE)) \
#        install.packages('BiocManager', repos = 'https://cloud.r-project.org'); \
#      BiocManager::install(version = '3.18', ask = FALSE, update = TRUE); \
#      BiocManager::install('DESeq2', ask = FALSE); \
#      print(packageVersion('DESeq2')); \
#      BiocManager::install('edgeR', ask = FALSE); \
#      print(packageVersion('edgeR')) \
#    " && \

    ## 6. Salmon \
#    wget https://github.com/COMBINE-lab/salmon/archive/refs/tags/v1.10.1.tar.gz && \
#    tar -xzf v1.10.1.tar.gz && \
#    cd salmon-1.10.1 && \
#    mkdir build && \
#    cd build && \
#    cmake ..  && \
#    make && \
#    make install && \
#    cp -v /opt/opencga/salmon-1.10.1/bin/salmon /usr/local/bin/ && \
#    cp -r -v /opt/opencga/salmon-1.10.1/lib/* /usr/local/lib/ && \
#    cd ../.. && \

    ## 7. Kallisto \
    wget https://github.com/pachterlab/kallisto/archive/refs/tags/v0.51.1.tar.gz && \
    tar -xzf v0.51.1.tar.gz && \
    cd kallisto-0.51.1 && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make && \
    cp -v /opt/opencga/kallisto-0.51.1/build/src/kallisto /usr/local/bin/ && \
    cp -r -v /opt/opencga/kallisto-0.51.1/build/src/libkallisto_core.a /usr/local/lib/ && \
    cd ../.. && \

    rm -rf STAR-2.7.11b STAR-Fusion-v1.15.1 hisat2-2.2.1 salmon-1.10.1 kallisto-0.51.1 /var/lib/apt/lists/* /tmp/*

# Set the PATH to include the virtual environment's bin directory
# This makes multiqc, pip, etc. available automatically
#ENV PATH="/app/multiqc_venv/bin:$PATH"
ENV PERL5LIB /usr/local/bin/PerlLib:$PERL5LIB
ENV PATH="/opt/opencga/hisat2-2.2.1:${PATH}"

#RUN STAR-Fusion --help

WORKDIR /opt/opencga
