FROM opencb/opencga-ext-tools:5.0.0-SNAPSHOT

# Install necessary system dependencies for MultiQC
# These are often needed for Python scientific packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    xxd  \
    python3 \
    python3-pip \
    python3-venv \
    build-essential \
    && rm -rf /var/lib/apt/lists/* \

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

ENV PATH="/opt/opencga/hisat2-2.2.1:${PATH}"

