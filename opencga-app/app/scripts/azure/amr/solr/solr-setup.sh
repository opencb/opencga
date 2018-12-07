#!/usr/bin/env bash

## Nacho (6/12/2018)
## Install Docker following: https://docs.docker.com/install/linux/docker-ce/ubuntu/#extra-steps-for-aufs

## Set up Docker repository for Ubuntu
apt-get update
apt-get install apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
apt-key fingerprint 0EBFCD88
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

## Install Docker CE
apt-get update
apt-get install docker-ce


## Install Solr 6.6 in Docker
docker pull solr:6.6
docker run --name opencga-solr-6.6 -d --restart always -p 8983:8983 -t solr:6.6

