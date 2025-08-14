FROM opencb/opencga-ext-tools:5.0.0-SNAPSHOT

# Install necessary system dependencies for MultiQC
# These are often needed for Python scientific packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    xxd  \
    python3 \
    python3-pip \
    python3-venv \
    build-essential \
    xorg-dev \
    libxt-dev \
    libcairo2-dev \
    libpango1.0-dev && \
    rm -rf /var/lib/apt/lists/* \


## Install local dependencies
# --- MultiQC virtual environment setup ---
WORKDIR /app

# Create a virtual environment for MultiQC
RUN python3 -m venv multiqc_venv

# Set the PATH to include the virtual environment's bin directory
# This makes multiqc, pip, etc. available automatically
ENV PATH="/app/multiqc_venv/bin:$PATH"

# Install MultiQC into the virtual environment
RUN pip install --no-cache-dir multiqc

# Install HISAT2 source code and make
RUN wget -O hisat2-source.zip https://github.com/DaehwanKimLab/hisat2/archive/v2.2.1.zip && \
    unzip hisat2-source.zip  && \
    cd hisat2-2.2.1 && \
    make && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    cd ..

# Download and install R 4.3.1
RUN wget https://cran.r-project.org/src/base/R-4/R-4.3.1.tar.gz && \
    tar -xzf R-4.3.1.tar.gz && \
    cd R-4.3.1 && \
    ./configure --enable-R-shlib && \
    make && \
    make install && \
    cd .. && \
    rm -rf R-4.3.1*

# Install BiocManager and DESeq2 (using Bioconductor 3.18 for R 4.3.1)
RUN Rscript -e " \
  if (!requireNamespace('BiocManager', quietly = TRUE)) \
    install.packages('BiocManager', repos = 'https://cloud.r-project.org'); \
  BiocManager::install(version = '3.18', ask = FALSE, update = TRUE); \
  BiocManager::install('DESeq2', ask = FALSE); \
  print(packageVersion('DESeq2')); \
  BiocManager::install('edgeR', ask = FALSE); \
  print(packageVersion('edgeR')) \
"

## Install Salmon
#RUN wget https://github.com/COMBINE-lab/salmon/archive/refs/tags/v1.10.1.tar.gz && \
#    tar xzf v1.10.1.tar.gz && \
#    mv salmon-1.10.1/bin/salmon /usr/local/bin/ && \
#    rm -rf salmon-*
#
## Install Kallisto
#RUN wget https://github.com/pachterlab/kallisto/releases/download/v0.48.0/kallisto_linux-v0.48.0.tar.gz && \
#    tar xzf kallisto_linux-v0.48.0.tar.gz && \
#    mv kallisto_linux-v0.48.0/kallisto /usr/local/bin/ && \
#    rm -rf kallisto_*

# Install STAR from source code and make
RUN wget https://github.com/alexdobin/STAR/archive/2.7.11b.tar.gz && \
	tar -xzf 2.7.11b.tar.gz  && \
	cd STAR-2.7.11b/source  && \
	make STAR && \
    cp -v STAR /usr/local/bin/ && \
    cd ../..  && \
    rm -rf STAR-2.7.11b


# Install required R packages for FusionInspector
RUN Rscript -e 'install.packages(c("ranger", "getopt", "jsonlite", "tidyverse"), repos="http://cran.us.r-project.org")'

#
## Install STAR-Fusion
#RUN git clone --recursive https://github.com/STAR-Fusion/STAR-Fusion.git \
#    && cd STAR-Fusion \
#    && make \
#    && cp STAR-Fusion /usr/local/bin/ \
#    && cp FusionInspector/FusionInspector /usr/local/bin/ \
#    && cp FusionFilter/FusionFilter /usr/local/bin/ \
#    && cp Prep_Reference_for_STAR_Fusion.pl /usr/local/bin/ \
#    && rm -rf /var/cache/git/* \
#    && cd .. \
#    && rm -rf STAR-Fusion
#
## Install Perl modules for STAR-Fusion
#RUN apt-get update && apt-get install -y \
#    perl \
#    cpanminus \
#    && rm -rf /var/lib/apt/lists/*
#
#RUN cpanm --notest File::Basename \
#    && cpanm --notest Getopt::Long \
#    && cpanm --notest FindBin \
#    && cpanm --notest strict \
#    && cpanm --notest warnings \
#    && cpanm --notest Carp \
#    && cpanm --notest Cwd \
#    && cpanm --notest IPC::Open3

# Install salmon and its dependencies
RUN apt-get update && apt-get install -y \
    curl cmake libboost-all-dev && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://github.com/COMBINE-lab/salmon/archive/refs/tags/v1.10.1.tar.gz && \
    tar -xzf v1.10.1.tar.gz && \
	cd salmon-1.10.1 && \
	mkdir build && cd build && \
	cmake ..  && \
	make && \
	make install

RUN cp -v /opt/opencga/salmon-1.10.1/bin/salmon /usr/local/bin/ && \
    cp -r -v /opt/opencga/salmon-1.10.1/lib/* /usr/local/lib/ && \
    rm -rf /opt/opencga/salmon-1.10.1

# Install kallisto
RUN wget https://github.com/pachterlab/kallisto/archive/refs/tags/v0.51.1.tar.gz && \
	tar -xzf v0.51.1.tar.gz && \
	cd kallisto-0.51.1 && \
	mkdir build && cd build && \
	cmake .. && \
	make

RUN cp -v /opt/opencga/kallisto-0.51.1/build/src/kallisto /usr/local/bin/ && \
    cp -r -v /opt/opencga/kallisto-0.51.1/build/src/libkallisto_core.a /usr/local/lib/ && \
    rm -rf /opt/opencga/kallisto-0.51.1



ENV PATH="/opt/opencga/hisat2-2.2.1:${PATH}"

