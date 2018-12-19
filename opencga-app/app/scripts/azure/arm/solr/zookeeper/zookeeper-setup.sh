#!/usr/bin/env bash

set -x
set -e

## Install Docker following: https://docs.docker.com/install/linux/docker-ce/ubuntu/#extra-steps-for-aufs
## Set up Docker repository for Ubuntu
apt-get update
apt-get install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
apt-key fingerprint 0EBFCD88
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

## Configure zookeeper docker container
## See https://hub.docker.com/_/zookeeper/

# Zookeeper IDs should have a value between 1 and 255.
ZOO_MY_ID=$(($1+1))
SUBNET_PREFIX=$2
IP_FIRST=$3
NUM_NODES=$4
ZOOKEEPER_VERSION=$5

DOCKER_NAME=opencga-zookeeper-${ZOOKEEPER_VERSION}

ZOO_SERVERS=
i=1
while [ $i -le $NUM_NODES ]
do
    if [ $i -eq $ZOO_MY_ID ] ; then
        ## Instead of having its own IP, should have 0.0.0.0
        ZOO_SERVERS="${ZOO_SERVERS} server.${i}=0.0.0.0:2888:3888"
    else
        ZOO_SERVERS="${ZOO_SERVERS} server.${i}=${SUBNET_PREFIX}$(($i+$IP_FIRST-1)):2888:3888"
    fi
    i=$(($i+1))
done

## Install Docker CE
apt-get update
apt-get install -y docker-ce

## Create docker container
docker run --name ${DOCKER_NAME} --restart always -d \
        -e ZOO_MY_ID=$ZOO_MY_ID -e "ZOO_SERVERS=$ZOO_SERVERS" -e ZOO_LOG4J_PROP="INFO,ROLLINGFILE" \
        -p 2888:2888 -p 2181:2181 -p 3888:3888 zookeeper:${ZOOKEEPER_VERSION}
