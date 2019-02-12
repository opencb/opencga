#!/bin/bash

set -x
set -e
export DEBIAN_FRONTEND='noninteractive'
# Wait for network
sleep 5

## Nacho (6/12/2018)

VM_NAME_PREFIX=$1
ZK_HOSTS_NUM=$2
SOLR_VERSION=$3

DOCKER_NAME=opencga-solr-${SOLR_VERSION}

# Create docker

# Add OpenCGA Configuration Set 
tar zxfv OpenCGAConfSet-1.4.0.tar.gz

# create a directory to store the server/solr directory
mkdir /opt/solr-volume

# make sure its host owner matches the container's solr user
sudo chown 8983:8983 /opt/solr-volume

# copy the solr directory from a temporary container to the volume
docker run --rm -v /opt/solr-volume:/target solr:${SOLR_VERSION} cp -r server/solr /target/

# copy configset to volume ready to mount
cp -r OpenCGAConfSet-1.4.0 /opt/solr-volume/solr/configsets/OpenCGAConfSet-1.4.0

# get script
docker run  --rm  solr:${SOLR_VERSION}  cat /opt/solr/bin/solr.in.sh > /opt/solr.in.sh

# botch - give networking a chance!

ZK_CLI=
if [[ $ZK_HOSTS_NUM -gt 0 ]]; then

    i=0
    while [ $i -lt $ZK_HOSTS_NUM ]
    do
        ZK_HOST=${ZK_HOST},${VM_NAME_PREFIX}${i}
       
       
       # check zookeeper node status
     
        until ( echo stat | (exec 3<>/dev/tcp/${VM_NAME_PREFIX}${i}/2181; cat >&3; cat <&3;) > /dev/null);
        do 
            echo "Waiting for Zookeeper node ${VM_NAME_PREFIX}${i} \n"
            sleep 10
        done     
    
       
        i=$(($i+1))

        
    done

    # Remove leading comma
    ZK_HOST=`echo $ZK_HOST | cut -c 2-`

    sed -i -e 's/#ZK_HOST=.*/ZK_HOST='$ZK_HOST'/' /opt/solr.in.sh
else
    ZK_CLI="-z localhost:9983"
fi

## Ensure always using cloud mode, even for the single server configurations.
echo 'SOLR_MODE="solrcloud"' >> /opt/solr.in.sh


docker run --name ${DOCKER_NAME} --restart always -p 8983:8983 -d -v /opt/solr-volume/solr:/opt/solr/server/solr -v /opt/solr.in.sh:/opt/solr/bin/solr.in.sh   solr:${SOLR_VERSION} docker-entrypoint.sh solr-foreground -h $(hostname) 


until $(curl --output /dev/null --silent --head --fail  "http://$(hostname):8983/solr/#/offers"); do
    printf 'waiting for solr...\n'
    sleep 5
done

docker exec ${DOCKER_NAME} /opt/solr/bin/solr zk upconfig -n OpenCGAConfSet-1.4.0 -d /opt/solr/server/solr/configsets/OpenCGAConfSet-1.4.0 $ZK_CLI
