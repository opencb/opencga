#!/usr/bin/env bash

set -x
set -e

ZOO_MY_ID=$1
SUBNET_PREFIX=$2
IP_FIRST=$3
NUM_NODES=$4

DOCKER_NAME=opencga-zookeeper-3.4

ZOO_SERVERS=
i=0
while [ $i -lt $NUM_NODES ]
do
    ZOO_SERVERS="${ZOO_SERVERS} server.${i}=${SUBNET_PREFIX}$(($i+$IP_FIRST)):2888:3888"
    i=$(($i+1))
done

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

## Create docker container
docker run --name ${DOCKER_NAME} --restart always -d -e ZOO_MY_ID=$ZOO_MY_ID -e "ZOO_SERVERS=$ZOO_SERVERS" zookeeper:3.4
