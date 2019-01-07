#!/bin/bash

set -x
set -e
export DEBIAN_FRONTEND='noninteractive'
# Wait for network
sleep 5

## Nacho (6/12/2018)
## Install Docker following: https://docs.docker.com/install/linux/docker-ce/ubuntu/#extra-steps-for-aufs

## Set up Docker repository for Ubuntu
apt-get update -y
apt-get install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
apt-key fingerprint 0EBFCD88
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

## Install Docker CE
apt-get update -y
apt-get install -y docker-ce


MY_ID=$(($1+1))
SUBNET_PREFIX=$2
IP_FIRST=$3
ZK_HOSTS_NUM=$4
SOLR_VERSION=$5

DOCKER_NAME=opencga-solr-${SOLR_VERSION}

## Install Solr in Docker
docker pull solr:${SOLR_VERSION}

# Create docker
docker create --name ${DOCKER_NAME} --restart always -p 8983:8983 -t solr:${SOLR_VERSION}

# Add OpenCGA Configuration Set 
tar zxfv OpenCGAConfSet_1.4.x.tar.gz

docker cp OpenCGAConfSet ${DOCKER_NAME}:/opt/solr/server/solr/configsets/OpenCGAConfSet-1.4.x

# Configure solr.in.sh
docker cp ${DOCKER_NAME}:/opt/solr/bin/solr.in.sh .
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

    sed -i -e 's/#ZK_HOST=.*/ZK_HOST='$ZK_HOST'/' solr.in.sh
else
    ZK_CLI="-z localhost:9983"
fi

## Ensure always using cloud mode, even for the single server configurations.
echo 'SOLR_MODE="solrcloud"' >> solr.in.sh

docker cp solr.in.sh ${DOCKER_NAME}:/opt/solr/bin/solr.in.sh

docker start ${DOCKER_NAME}

# Wait solr to be started
# FIXME: Improve this wait
sleep 60

# Register opencga configuration set
docker exec ${DOCKER_NAME} /opt/solr/bin/solr zk upconfig -n OpenCGAConfSet-1.4.x -d /opt/solr/server/solr/configsets/OpenCGAConfSet-1.4.x $ZK_CLI


