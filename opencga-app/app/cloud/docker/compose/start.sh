#!/bin/bash

mkdir ~/opencga-docker-data ~/opencga-docker-data/mongodb ~/opencga-docker-data/solr

chown -R 1001:1001 ~/opencga-docker-data/solr
#chown solr:solr ~/opencga-docker-data/solr
#chmod 777 ~/opencga-docker-data/solr
#export SOLR_USER="$(id -u -n)"
#export SOLR_UID="$(id -u)"

#echo $SOLR_USER
#echo $SOLR_UID
#docker-compose config
docker-compose up
