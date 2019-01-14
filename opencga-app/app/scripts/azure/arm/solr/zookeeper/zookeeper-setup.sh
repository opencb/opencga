#!/usr/bin/env bash

set -x
set -e
export DEBIAN_FRONTEND='noninteractive'

# Zookeeper IDs should have a value between 1 and 255.
ZOO_MY_ID=$1
VM_NAME_PREFIX=$2
NUM_NODES=$3
ZOOKEEPER_VERSION=$4

DOCKER_NAME=opencga-zookeeper-${ZOOKEEPER_VERSION}

ZOO_SERVERS=
i=0
while [ $i -le $NUM_NODES ]
do
    if [ $i -eq $ZOO_MY_ID ] ; then
        ## Instead of having its own IP, should have 0.0.0.0
        ZOO_SERVERS="${ZOO_SERVERS} server.${i}=0.0.0.0:2888:3888"
    else
        ZOO_SERVERS="${ZOO_SERVERS} server.${i}=${VM_NAME_PREFIX}${i}:2888:3888"
    fi
    i=$(($i+1))
done

## Create docker container
docker run --name ${DOCKER_NAME} --restart always -d \
        -e ZOO_MY_ID=$ZOO_MY_ID -e "ZOO_SERVERS=$ZOO_SERVERS" -e ZOO_LOG4J_PROP="INFO,ROLLINGFILE" \
        -p 2888:2888 -p 2181:2181 -p 3888:3888 zookeeper:${ZOOKEEPER_VERSION}