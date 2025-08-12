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

# Install STAR from source code and make
RUN wget https://github.com/alexdobin/STAR/archive/2.7.11b.tar.gz && \
	tar -xzf 2.7.11b.tar.gz  && \
	cd STAR-2.7.11b/source  && \
	make STAR && \
    cp -v STAR /usr/local/bin/ && \
    cd ../..  && \
    rm -rf STAR-2.7.11b

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
  print(packageVersion('DESeq2')) \
"

ENV PATH="/opt/opencga/hisat2-2.2.1:${PATH}"

