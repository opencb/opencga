#!/bin/bash

#this script is wrapped into the mongodb template in base64
#just here for reference

set -e

# echo $APP_DNS_NAME
# echo $MONGODB_USERNAME
# echo $MONGODB_PASSWORD
# echo $CERT_EMAIL
# echo $VM_INDEX
# echo $CLUSTER_SIZE

configureTCPTimeout() {
    # Lower TCP keeplive interval
    # "For MongoDB, you will have better results with shorter keepalive periods, on the order of 120 seconds (two minutes)."
    # See: https://docs.mongodb.com/manual/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments
    sysctl -w net.ipv4.tcp_keepalive_time=120
}

installDeps () {
    echo "installing deps"
    apt-mark hold walinuxagent
    apt-get update
    apt-get upgrade -y
    apt-get install software-properties-common -y
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5
    wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | sudo apt-key add -

    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/4.2 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.2.list
    apt-get update
    apt-get install -y mongodb-org
    systemctl enable mongod.service
    add-apt-repository -y universe
    #add-apt-repository -y ppa:certbot/certbot
    apt-get update
    apt-get install -y nginx
    #apt-get install -y certbot python-certbot-nginx

    #wait until nginx is available
    sleep 15
}


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
    echo "Formatting Partition as XFS"
    mkfs -t xfs ${1}1
    mkdir /datadrive
    mount ${1}1 /datadrive
    fs_uuid=$(blkid -o value -s UUID ${1}1)
    echo "UUID=${fs_uuid}   /datadrive   xfs   defaults,nofail   1   2" >> /etc/fstab
}

#generateCertificate() {
#    echo "generating certs"
#    sed -i -e 's/# server_names_hash_bucket_size 64/server_names_hash_bucket_size 128/g' /etc/nginx/nginx.conf
#    certbot --nginx -d ${APP_DNS_NAME} -m ${CERT_EMAIL} --agree-tos -q
#    nginx -t && nginx -s reload
#    cat /etc/letsencrypt/live/${APP_DNS_NAME}/privkey.pem /etc/letsencrypt/live/${APP_DNS_NAME}/cert.pem > /etc/ssl/mongo.pem
#    cat /etc/letsencrypt/live/${APP_DNS_NAME}/chain.pem >> /etc/ssl/ca.pem
#
#
#    #add cronjob to renew certificate
#    crontab -l | { cat; echo "0 0 1 * * /opt/renew_mongo_cert.sh ${APP_DNS_NAME}"; } | crontab -
#}

generateCertificate() {
    echo "generating certs"
    /opt/renew_mongo_cert.sh ${APP_DNS_NAME}
    sleep 10

    #add cronjob to renew certificate
    crontab -l | { cat; echo "0 0 1 * * /opt/renew_mongo_cert.sh ${APP_DNS_NAME}"; } | crontab -
}

configureDataDrive() {}
    #configure mongodb to use /datadrive for storage
    mkdir /datadrive/mongodb
    setfacl -R -m u:mongodb:rwx /datadrive/mongodb
    sed -i -e '/dbPath/ s/: .*/: \/datadrive\/mongodb /' /etc/mongod.conf

    systemctl restart mongod
    sleep 10
}

configureMongoDB() {
    echo "configuring mongo"
  
    until mongo admin --eval 'db.createUser({user: "'${MONGODB_USERNAME}'",pwd: "'${MONGODB_PASSWORD}'",roles: ["root"]})' --quiet 
    do
        echo "Waiting for Mongo DB"
        sleep 5
    done

    sed -i '/#security/csecurity:\n  authorization: "enabled"\n' /etc/mongod.conf
    sed -i -e '/bindIp/ s/: .*/: ::,0.0.0.0\n  ssl:\n    mode: requireTLS\n    PEMKeyFile: \/etc\/ssl\/mongo.pem\n    allowConnectionsWithoutCertificates: true\n    allowInvalidCertificates: true /' /etc/mongod.conf

    systemctl restart mongod
}

configureMongoReplication() {
    echo "configure replication"
    sed -i '/authorization: "enabled"/!b;n;c\  keyFile: \/opt\/mongodb.key\n' /etc/mongod.conf
    sed -i '/#replication/creplication:\n  replSetName: rs0\n' /etc/mongod.conf

    systemctl restart mongod
    sleep 10
}


generateMongoKeyFile() {
    echo "generating keyfile on primary node"
    openssl rand -base64 741 > /opt/mongodb.key
    gpg --yes --batch --passphrase=${MONGODB_PASSWORD} -c /opt/mongodb.key
    cp /opt/mongodb.key.gpg /var/www/html/mongodb.key.gpg
    chmod 600 /opt/mongodb.key
    chown mongodb /opt/mongodb.key
    systemctl restart mongod
}

configureSlaveNodes() {
    sleep 60
    #get webserver url of primary instance
    VM_DNS=$(sed -e "s/$VM_INDEX\./0\./g" <<< $APP_DNS_NAME)

    echo "configuring slave nodes"
    echo $VM_DNS

    #loop till mongodb key is available
    while true;do
        wget -T 15 ${VM_DNS}/mongodb.key.gpg -O /opt/mongodb.key.gpg && break
    done

    gpg --yes --batch --passphrase=${MONGODB_PASSWORD} /opt/mongodb.key.gpg
    chmod 600 /opt/mongodb.key
    chown mongodb /opt/mongodb.key
    systemctl restart mongod
}

createReplicaSet() {
    echo "initiating replicaset"
    mongo admin -u ${MONGODB_USERNAME} -p ${MONGODB_PASSWORD} --eval 'rs.initiate({_id: "rs0",members: [{_id: 0,host:"'${APP_DNS_NAME}':27017"}]})'

    sleep 10

    #for vmindex +1
    for (( c=1; c<$CLUSTER_SIZE; c++ ))
    do
        #replacing 0. with 1., 2. etc
        VM_DNS=$(sed -e "s/0\./$c\./g" <<< $APP_DNS_NAME)

        mongo admin -u ${MONGODB_USERNAME} -p ${MONGODB_PASSWORD} --eval 'rs.add("'${VM_DNS}':27017")'
        sleep 5
    done

    #remove mongodb keyfile
    rm /var/www/html/mongodb.key.gpg
}

restoreMongoDBDump() {
    echo "restoring mongoDB dump"
    systemctl stop mongod
    
    cd /datadrive

    echo "Get azcopy to speed up download"
    wget -O azcopy.tar https://azcopyvnext.azureedge.net/release20190301/azcopy_linux_amd64_10.0.8.tar.gz
    tar -xf azcopy.tar --strip-components=1

    echo "Restoring MongoDB data dump"
    echo "Downloading dump file"
    ./azcopy copy --recursive $MONGODB_DUMP_URL /datadrive/

    echo "Set file ownership to mongo"
    chown -R mongodb /datadrive/mongodb/**
    chmod -R 777 /datadrive/mongodb/** #Todo: maybe not needed

    echo "Restarting mongo"
    systemctl start mongod
    sleep 120 # To ensure time for mongo to start up again

    echo "Mongo Logs:"
    tail /var/log/mongodb/mongod.log
}

#install flow
configureTCPTimeout
installDeps
scanForNewDisks
generateCertificate
configureDataDrive

# Generate a key for use by the cluster. We want to do this before the restore so the slave nodes can be configured correctly
generateMongoKeyFile

# restore a mongodb dump is one has been specificed, and this is the primary node in the cluster
if [ -n "$MONGODB_DUMP_URL" ] && [ "$VM_INDEX" -eq 0 ]
then
    restoreMongoDBDump
fi

configureMongoDB

# allways make replicaset 
configureMongoReplication

# configure mongo replication if the cluster size is greater than 1, and this is the primary node in the cluster

if [ "$VM_INDEX" -eq 0 ]
then
    sleep 120
    createReplicaSet
else
    configureSlaveNodes
fi



