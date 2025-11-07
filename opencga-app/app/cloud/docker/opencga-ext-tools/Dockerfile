FROM ubuntu:24.04

LABEL org.label-schema.vendor="OpenCB" \
      org.label-schema.name="opencga-ext-tools" \
      org.label-schema.url="http://docs.opencb.org/display/opencga" \
      org.label-schema.description="An Open Computational Genomics Analysis platform for big data processing and analysis in genomics" \
      maintainer="Joaquin Tarraga <joaquintarraga@gmail.com>" \
      org.label-schema.schema-version="1.0"

## Run update and install necessary packages
RUN apt-get update -y && \
    ## 0. Install system dependencies
    DEBIAN_FRONTEND="noninteractive" TZ="Europe/London" apt-get install -y curl libcurl4 git libgmp-dev libcurl4-openssl-dev libgit2-dev build-essential \
    libssl-dev libssh-dev libxml2-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev \
    gnuplot pandoc samtools bcftools tabix fastqc plink1.9 plink2 bwa r-base wget libopenblas0-openmp libcholmod5 libsuitesparse-dev python3 python3-pip && \
    apt-get remove libopenblas0-pthread && \
    pip3 install --no-cache-dir --user multiqc --break-system-packages && \
    pip3 install --no-cache-dir pydantic --break-system-packages && \

    ## 1. Install Docker repository
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu "$(. /etc/os-release && echo "noble")" stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io && \

    ## 2. Install samtools
    wget https://github.com/samtools/samtools/releases/download/1.22.1/samtools-1.22.1.tar.bz2 && \
    tar xjvf samtools-1.22.1.tar.bz2 && \
    cd samtools-1.22.1 && \
    ./configure --prefix=/usr/local && \
    make && make install && \
    cd .. && \

    ## 3. Install bcftools with liftover and affy2vcf plugins
    wget http://github.com/samtools/bcftools/releases/download/1.22/bcftools-1.22.tar.bz2 && \
    tar xjvf bcftools-1.22.tar.bz2 && \
    bash -c 'wget -P bcftools-1.22 http://raw.githubusercontent.com/DrTimothyAldenDavis/SuiteSparse/stable/{SuiteSparse_config/SuiteSparse_config,CHOLMOD/Include/cholmod}.h' && \
    cd bcftools-1.22 && \
    ## liftover plugin
    bash -c '/bin/rm -f plugins/{score.{c,h},{munge,liftover,metal,blup}.c,pgs.{c,mk}}' && \
    bash -c 'wget -P plugins http://raw.githubusercontent.com/freeseek/score/master/{score.{c,h},{munge,liftover,metal,blup}.c,pgs.{c,mk}}' && \
    ## affy2vcf plugin
    bash -c '/bin/rm -f plugins/{idat2gtc.c,gtc2vcf.{c,h},affy2vcf.c}' && \
    bash -c 'wget -P plugins http://raw.githubusercontent.com/freeseek/gtc2vcf/master/{idat2gtc.c,gtc2vcf.{c,h},affy2vcf.c,BAFregress.c}' && \
    make && \
    bash -c '/bin/cp bcftools plugins/{munge,liftover,score,metal,pgs,blup,idat2gtc,gtc2vcf,affy2vcf,BAFregress}.so /usr/local/bin/' && \
    wget -P /usr/local/bin http://raw.githubusercontent.com/freeseek/score/master/assoc_plot.R && \
    chmod a+x /usr/local/bin/assoc_plot.R && \
    cd .. && \

    ## 4. Install bwa-mem2
    git clone --recursive https://github.com/bwa-mem2/bwa-mem2 && \
    cd bwa-mem2 && \
    make && \
    cp bwa-mem2* /usr/local/bin/ && \
    cd .. && \

    ## 5. Install Thermofisher APT tools
    mkdir affy && \
    cd affy && \
    wget https://downloads.thermofisher.com/APT/APT_2.12.0/apt_2.12.0_linux_64_x86_binaries.zip && \
    unzip apt_2.12.0_linux_64_x86_binaries.zip && \
    chmod +x bin/* && \
    cp bin/*-axiom bin/apt-format-result bin/apt-copynumber-axiom-cnvmix /bin/apt2-summary-plate-norm bin/ps-metrics bin/ps-classification /usr/local/bin/ && \
    cd .. && \

    ## 6. Installation dependencies using R install.packages() is slower than apt-get but final size is 400MB smaller.
    R -e "install.packages(c('BiocManager', 'RCircos', 'nnls', 'ggplot2', 'jsonlite', 'optparse', 'knitr', 'configr', 'dplyr', 'rmarkdown', 'tidyr', 'httr', 'kinship2', 'limSolve'))" && \
    R -e "BiocManager::install('BiocStyle')" && \
    R -e "BiocManager::install('BSgenome.Hsapiens.UCSC.hg38')" && \

    ## 7. Install signature.tools.lib installation \
    R -e "install.packages(c('devtools', 'getopt'), repos=c('http://cran.rstudio.com/', 'https://www.stats.bris.ac.uk/R/'))" && \
    git clone https://github.com/Nik-Zainal-Group/signature.tools.lib.git /opt/opencga/signature.tools.lib && \
    cd /opt/opencga/signature.tools.lib && \
    git fetch origin --tags && \
    git checkout tags/v2.4.4 && \
    sed -i '/Mmusculus/d' DESCRIPTION && \
    sed -i '/Cfamiliaris/d' DESCRIPTION && \
    sed -i '/1000genomes/d' DESCRIPTION && \
    R -e "options(timeout = 3600);devtools::install(repos='https://www.stats.bris.ac.uk/R/')" && \
    cd .. && \

    ## 8. Clean up
    rm -rf samtools-1.22.1 bcftools-1.22 bwa-mem2 affy /var/lib/apt/lists/* /tmp/* /opt/opencga/signature.tools.lib/.git && \
    strip --remove-section=.note.ABI-tag /usr/lib/x86_64-linux-gnu/libQt5Core.so.5


ENV BCFTOOLS_PLUGINS="/usr/local/bin"

WORKDIR /opt/opencga
