FROM opencga-java-base

# Get opencga
ARG OPENCGA_VERSION="1.4.0-rc2"

RUN mkdir /install

WORKDIR /install

# RUN wget --quiet https://github.com/opencb/opencga/releases/download/v${OPENCGA_VERSION}/opencga-${OPENCGA_VERSION}.tar.gz 
COPY ./opencga-${OPENCGA_VERSION}.tar.gz ./opencga.tar.gz

# extract
RUN  mkdir /opt/opencga && tar xvf opencga.tar.gz  -C /opt/opencga 

COPY ./conf /opt/opencga/conf

RUN  apt-get update && apt-get install sshpass -y

COPY ./config_and_run.sh /install

ENV HD_INSIGHTS_SSH_DNS=""

ENV HD_INSIGHTS_SSH_USER=""

ENV HD_INSIGHTS_SSH_PASS=""
 
CMD ["sh", "-c", "/install/config_and_run.sh &&  /opt/opencga/bin/opencga-admin.sh catalog daemon --start -p $HD_INSIGHTS_SSH_PASS"] 
