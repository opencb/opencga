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


scanForNewDisks() {
    # Looks for unpartitioned disks
    DEVS=($(fdisk -l 2>/dev/null | egrep -o '(/dev/[^:]*):' | egrep -o '([^:]*)'))

    for DEV in "${DEVS[@]}";
    do
        echo "Found unpartitioned disk $DEV"
        isPartitioned ${DEV}
    done
}

isPartitioned() {
    OUTPUT=$(partx -s ${1} 2>&1)
    if [[ "$OUTPUT" == *"failed to read partition table"* ]]; then
        # found unpartitioned disk
        formatAndMountDisk "${DEV}"
    fi
}

formatAndMountDisk() {
    #partitions primary linux partition on new disk
    echo "Partitioning disk"
    echo 'type=83' | sfdisk ${1}
    until blkid ${1}1
    do
        echo "Waiting for drive to be partitioned"
        sleep 2
    done
    echo "Formatting Partition"
    mkfs -t ext4 ${1}1
    mkdir /datadrive
    mount -o acl ${1}1 /datadrive
    fs_uuid=$(blkid -o value -s UUID ${1}1)
    echo "UUID=${fs_uuid}   /datadrive   ext4   defaults,nofail,acl   1   2" >> /etc/fstab
}

scanForNewDisks

# Add OpenCGA Configuration Set 
tar zxfv OpenCGAConfSet-1.4.0.tar.gz

# create a directory to store the server/solr directory
mkdir /datadrive/solr-volume

# make sure its host owner matches the container's solr user
sudo chown 8983:8983 /datadrive/solr-volume

# copy the solr directory from a temporary container to the volume
docker run --rm -v /datadrive/solr-volume:/target solr:${SOLR_VERSION} cp -r server/solr /target/

# copy configset to volume ready to mount
cp -r OpenCGAConfSet-1.4.0 /datadrive/solr-volume/solr/configsets/OpenCGAConfSet-1.4.0

# get script
docker run  --rm  solr:${SOLR_VERSION}  cat /opt/solr/bin/solr.in.sh > /opt/solr.in.sh


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

TOTAL_RAM=$(sed 's/ kB//g'  <<< $(grep -oP '^MemTotal:\s+\K.*' /proc/meminfo))
SOLR_HEAP=$(echo "$TOTAL_RAM/1024/1024/2" | bc )

docker run --name ${DOCKER_NAME} --restart always -p 8983:8983 -d -v /datadrive/solr-volume/solr:/opt/solr/server/solr -v /opt/solr.in.sh:/opt/solr/bin/solr.in.sh -e SOLR_HEAP=${SOLR_HEAP}g  solr:${SOLR_VERSION} docker-entrypoint.sh solr-foreground -h $(hostname) 


until $(curl --output /dev/null --silent --head --fail  "http://$(hostname):8983/solr/#/offers"); do
    printf 'waiting for solr...\n'
    sleep 5
done

docker exec ${DOCKER_NAME} /opt/solr/bin/solr zk upconfig -n OpenCGAConfSet-1.4.0 -d /opt/solr/server/solr/configsets/OpenCGAConfSet-1.4.0 $ZK_CLI
