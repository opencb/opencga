FROM r-base:3.6.2

LABEL org.label-schema.vendor="OpenCB" \
      org.label-schema.name="opencga-r" \
      org.label-schema.url="http://docs.opencb.org/display/opencga" \
      org.label-schema.description="An Open Computational Genomics Analysis platform for big data processing and analysis in genomics" \
      maintainer="Joaquin Tarraga <joaquintarraga@gmail.com>" \
      org.label-schema.schema-version="1.0"

RUN apt-get update && apt-get install -y curl libssl-dev libcurl4-openssl-dev libxml2-dev

RUN R -e "install.packages(c('nnls', 'ggplot2', 'jsonlite', 'optparse', 'BiocManager', 'RCircos'))"
RUN R -e "BiocManager::install('BSgenome.Hsapiens.1000genomes.hs37d5')"
