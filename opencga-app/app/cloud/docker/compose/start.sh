#!/bin/bash

## Create folder structure
if [ ! -d ~/opencga-docker-data/ ]; then
    mkdir ~/opencga-docker-data
fi

if [ ! -d ~/opencga-docker-data/sessions/ ]; then
    mkdir ~/opencga-docker-data/sessions
    chmod 777 ~/opencga-docker-data/sessions
fi

if [ ! -d ~/opencga-docker-data/mongodb/ ]; then
    mkdir ~/opencga-docker-data/mongodb
fi

if [ ! -d ~/opencga-docker-data/solr/ ]; then
    mkdir ~/opencga-docker-data/solr
    chmod 777 ~/opencga-docker-data/solr
fi

## Launch Docker compose
docker-compose -f `dirname $0`/docker-compose.yml up -d


