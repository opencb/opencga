ARG ORG=opencb
ARG TAG=5.0.0-SNAPSHOT

FROM $ORG/opencga-ext-tools:$TAG

LABEL org.label-schema.vendor="OpenCB" \
      org.label-schema.name="opencga-genomics" \
      org.label-schema.url="http://docs.opencb.org/display/opencga" \
      org.label-schema.description="An Open Computational Genomics Analysis platform for big data processing and analysis in genomics" \
      maintainer="Joaquin Tarraga <joaquintarraga@gmail.com>" \
      org.label-schema.schema-version="1.0"

## Run update and install necessary packages
RUN apt-get update && apt-get install -y --no-install-recommends && \
    ## 1. Install system dependencies
    apt-get install -y bwa bcftools plink1.9 plink2 tabix && \

    ## 2. Install bcftools 1.21 and plugins
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
    cd .. && \

    ## 3. Installation dependencies using R install.packages() is slower than apt-get but final size is smaller.
#    R -e "install.packages(c('BiocManager', 'RCircos', 'nnls', 'ggplot2', 'jsonlite', 'optparse', 'knitr', 'configr', 'dplyr', 'rmarkdown', 'tidyr', 'httr', 'kinship2', 'limSolve'))" && \
#    R -e "BiocManager::install('BiocStyle')" && \
    R -e "BiocManager::install('BSgenome.Hsapiens.UCSC.hg38', ask = FALSE)" && \

    ## 4. Install signature.tools.lib installation \
    R -e "install.packages(c('devtools'), repos=c('http://cran.rstudio.com/', 'https://www.stats.bris.ac.uk/R/'))" && \
    git clone https://github.com/Nik-Zainal-Group/signature.tools.lib.git /opt/opencga/signature.tools.lib && \
    cd /opt/opencga/signature.tools.lib && \
    git fetch origin --tags && \
    git checkout tags/v2.4.4 && \
    sed -i '/Mmusculus/d' DESCRIPTION && \
    sed -i '/Cfamiliaris/d' DESCRIPTION && \
    sed -i '/1000genomes/d' DESCRIPTION && \
    R -e "options(timeout = 3600);devtools::install(repos='https://www.stats.bris.ac.uk/R/')" && \
    cd .. && \

    ## 5. Clean up
    rm -rf bcftools-1.21 /var/lib/apt/lists/* /tmp/* /opt/opencga/signature.tools.lib/.git

ENV BCFTOOLS_PLUGINS="/usr/local/bin"

WORKDIR /opt/opencga
