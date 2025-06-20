FROM ubuntu:22.04

LABEL org.label-schema.vendor="OpenCB" \
      org.label-schema.name="opencga-ext-tools" \
      org.label-schema.url="http://docs.opencb.org/display/opencga" \
      org.label-schema.description="An Open Computational Genomics Analysis platform for big data processing and analysis in genomics" \
      maintainer="Joaquin Tarraga <joaquintarraga@gmail.com>" \
      org.label-schema.schema-version="1.0"

## 1. Run update and install necessary packages
RUN apt-get update -y && \
    ## 1.1 Install system dependencies
    DEBIAN_FRONTEND="noninteractive" TZ="Europe/London" apt-get install -y libcurl4 git libgmp-dev libcurl4-openssl-dev libgit2-dev build-essential \
    libssl-dev libssh-dev libxml2-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev \
    gnuplot pandoc samtools bcftools tabix fastqc plink1.9 bwa r-base wget libopenblas0-openmp libcholmod3 libsuitesparse-dev python3 python3-pip && \
    apt-get remove libopenblas0-pthread && \
    pip3 install pydantic && \

    ## 1.2. Install samtools and bcftools 1.21 and plugins
    wget https://github.com/samtools/samtools/releases/download/1.21/samtools-1.21.tar.bz2 && \
    tar xjvf samtools-1.21.tar.bz2 && \
    cd samtools-1.21 && \
    ./configure --prefix=/usr/local && \
    make && make install && \
    cd .. && \

    wget http://github.com/samtools/bcftools/releases/download/1.21/bcftools-1.21.tar.bz2 && \
    tar xjvf bcftools-1.21.tar.bz2 && \
    bash -c 'wget -P bcftools-1.21 http://raw.githubusercontent.com/DrTimothyAldenDavis/SuiteSparse/stable/{SuiteSparse_config/SuiteSparse_config,CHOLMOD/Include/cholmod}.h' && \
    cd bcftools-1.21/ && \
    bash -c '/bin/rm -f plugins/{score.{c,h},{munge,liftover,metal,blup}.c,pgs.{c,mk}}' && \
    bash -c 'wget -P plugins http://raw.githubusercontent.com/freeseek/score/master/{score.{c,h},{munge,liftover,metal,blup}.c,pgs.{c,mk}}' && \
    make && \
    bash -c '/bin/cp bcftools plugins/{munge,liftover,score,metal,pgs,blup}.so /usr/local/bin/' && \
    wget -P /usr/local/bin http://raw.githubusercontent.com/freeseek/score/master/assoc_plot.R && \
    chmod a+x /usr/local/bin/assoc_plot.R && \
    cd .. && true

    # 2. Clone the signature.tools.lib repository
RUN git clone https://github.com/Nik-Zainal-Group/signature.tools.lib.git /opt/opencga/signature.tools.lib && \
    cd /opt/opencga/signature.tools.lib && \
    git fetch origin --tags && \
    git checkout tags/v2.4.4 && \
    sed -i '/Mmusculus/d' DESCRIPTION && \
    sed -i '/Cfamiliaris/d' DESCRIPTION && \
    sed -i '/1000genomes/d' DESCRIPTION

    # 3. Install R package managers and general dependencies
RUN R -e "install.packages('BiocManager', repos='https://cloud.r-project.org')" && \
    R -e "install.packages(c('remotes','devtools', 'getopt', 'RCircos', 'nnls', 'ggplot2', 'jsonlite', 'optparse', 'knitr', 'configr', 'dplyr', 'rmarkdown', 'tidyr', 'httr', 'kinship2', 'gh', 'usethis'), repos='https://cloud.r-project.org')"

    # 4. Install Bioconductor packages required by signature.tools.lib
RUN R -e "BiocManager::install(c('BiocStyle', 'BSgenome.Hsapiens.UCSC.hg38', 'limSolve'))"

    # 5. Install signature.tools.lib from local clone (ignore warnings about long file paths)
RUN Rscript -e "options(timeout = 3600); remotes::install_local('/opt/opencga/signature.tools.lib', upgrade='never', dependencies=TRUE)"

    ## 6. Clean up
RUN rm -rf samtools-1.21 bcftools-1.21 /var/lib/apt/lists/* /tmp/* /opt/opencga/signature.tools.lib/.git && \
    strip --remove-section=.note.ABI-tag /usr/lib/x86_64-linux-gnu/libQt5Core.so.5

ENV BCFTOOLS_PLUGINS="/usr/local/bin"

WORKDIR /opt/opencga
