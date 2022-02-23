#!/bin/bash


if [ ! -d ~/opencga-docker-data/ ]; then
    mkdir ~/opencga-docker-data
fi

if [ ! -d ~/opencga-docker-data/mongodb/ ]; then
    mkdir ~/opencga-docker-data/mongodb
fi

if [ ! -d ~/opencga-docker-data/solr/ ]; then
    mkdir ~/opencga-docker-data/solr
    chmod 777 ~/opencga-docker-data/solr
    chown -R 1001:1001 ~/opencga-docker-data/solr
fi

docker-compose up


