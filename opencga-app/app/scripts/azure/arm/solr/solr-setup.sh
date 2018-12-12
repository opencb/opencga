#!/bin/bash

set -x
set -e

## Nacho (6/12/2018)
## Install Docker following: https://docs.docker.com/install/linux/docker-ce/ubuntu/#extra-steps-for-aufs

## Set up Docker repository for Ubuntu
apt-get update
apt-get install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
apt-key fingerprint 0EBFCD88
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

## Install Docker CE
apt-get update
apt-get install -y docker-ce


## Install Solr 6.6 in Docker
docker pull solr:6.6

MY_ID=$(($1+1))
SUBNET_PREFIX=$2
IP_FIRST=$3
ZK_HOSTS_NUM=$4

DOCKER_NAME=opencga-solr-6.6

# Create docker
docker create --name ${DOCKER_NAME} --restart always -p 8983:8983 -t solr:6.6

# Add OpenCGA Configuration Set 
wget 'http://docs.opencb.org/download/attachments/9240577/OpenCGAConfSet_1.4.x.tar.gz?version=1&modificationDate=1523459489793&api=v2' -O OpenCGAConfSet_1.4.x.tar.gz
tar zxfv OpenCGAConfSet_1.4.x.tar.gz

docker cp OpenCGAConfSet opencga-solr-6.6:/opt/solr/server/solr/configsets/OpenCGAConfSet-1.4.x

ZK_CLI=
if [[ $ZK_HOSTS_NUM -gt 0 ]]; then

    i=0
    while [ $i -lt $ZK_HOSTS_NUM ]
    do
        ZK_HOST=${ZK_HOST},${SUBNET_PREFIX}$(($i+$IP_FIRST))
        i=$(($i+1))
    done

    # Remove leading comma
    ZK_HOST=`echo $ZK_HOST | cut -c 2-`

    docker cp ${DOCKER_NAME}:/opt/solr/bin/solr.in.sh .
    sed -i -e 's/#ZK_HOST=.*/ZK_HOST='$ZK_HOST'/' solr.in.sh

    docker cp solr.in.sh ${DOCKER_NAME}:/opt/solr/bin/solr.in.sh
else
    ZK_CLI="-z localhost:9983"
fi

docker start ${DOCKER_NAME}

# Wait solr to be started
# FIXME: Improve this wait
sleep 60

# Register opencga configuration set
docker exec opencga-solr-6.6 /opt/solr/bin/solr zk upconfig -n OpenCGAConfSet-1.4.x -d /opt/solr/server/solr/configsets/OpenCGAConfSet-1.4.x $ZK_CLI


