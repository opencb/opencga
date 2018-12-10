#!/usr/bin/env bash

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

ZK_HOSTS_NUM=$1
DOCKER_NAME=opencga-solr-6.6

if [[ $ZK_HOSTS_NUM -gt 0 ]]; then
    ZK_HOSTS=$2

    for i in "${@:3}"; do
        ZK_HOSTS=${ZK_HOSTS},$i
    done

    docker create --name ${DOCKER_NAME} --restart always -p 8983:8983 -t solr:6.6
    docker cp ${DOCKER_NAME}:/opt/solr/bin/solr.in.sh .
    sed -i -e 's/#ZK_HOST=.*/ZK_HOST='$ZK_HOST'/' solr.in.sh

    docker cp solr.in.sh ${DOCKER_NAME}:/opt/solr/bin/solr.in.sh

fi

docker start ${DOCKER_NAME}
