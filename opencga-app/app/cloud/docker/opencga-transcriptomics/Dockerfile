FROM opencb/opencga-ext-tools:5.0.0-SNAPSHOT

# Install necessary system dependencies for MultiQC
# These are often needed for Python scientific packages
RUN apt-get update && apt-get install -y --no-install-recommends \
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



