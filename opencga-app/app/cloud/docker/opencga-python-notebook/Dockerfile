# Start from the Jupyter scipy-notebook image
FROM quay.io/jupyter/scipy-notebook:ubuntu-24.04
ARG VERSION
USER root

RUN apt-get update && apt-get -y upgrade && \
    ## Install pyopencga ${VERSION}
    pip install pyopencga==${VERSION} && \
    ## Clean downloaded packages to make images smaller
    apt-get clean && rm -rf /var/lib/apt/lists/*

## Run Docker images as non root
USER ${NB_UID}
