#!/bin/bash

#this script is wrapped into the mongodb template in base64
#just here for reference

set -e

# echo $APP_DNS_NAME
# echo $MONGODB_USERNAME
# echo $MONGODB_PASSWORD
# echo $CERT_EMAIL
# echo $VM_INDEX
# echo $VM_SIZE
apt-get update
apt-get upgrade -y
apt-get install software-properties-common -y
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.6 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.6.list
apt-get update
apt-get install -y mongodb-org
systemctl enable mongod.service
add-apt-repository -y universe
add-apt-repository -y ppa:certbot/certbot
apt-get update
apt-get install -y nginx certbot python-certbot-nginx
sleep 30
sed -i -e 's/# server_names_hash_bucket_size 64/server_names_hash_bucket_size 128/g' /etc/nginx/nginx.conf
certbot --nginx -d ${APP_DNS_NAME} -m ${CERT_EMAIL} --agree-tos -q
nginx -t && nginx -s reload
cat /etc/letsencrypt/live/${APP_DNS_NAME}/privkey.pem /etc/letsencrypt/live/${APP_DNS_NAME}/cert.pem > /etc/ssl/mongo.pem
cat /etc/letsencrypt/live/${APP_DNS_NAME}/chain.pem >> /etc/ssl/ca.pem
systemctl restart mongod
sleep 10
mongo admin --eval 'db.createUser({user: "'${MONGODB_USERNAME}'",pwd: "'${MONGODB_PASSWORD}'",roles: ["root"]})'
sed -i '/#security/csecurity:\n  authorization: "enabled"\n' /etc/mongod.conf
sed -i '/#replication/creplication:\n  replSetName: rs0\n' /etc/mongod.conf
sed -i -e '/bindIp/ s/: .*/: ::,0.0.0.0\n  ssl:\n    mode: allowSSL\n    PEMKeyFile: \/etc\/ssl\/mongo.pem\n    CAFile: \/etc\/ssl\/ca.pem\n    allowConnectionsWithoutCertificates: true /' /etc/mongod.conf

#add cronjob to renew certificate
crontab -l | { cat; echo "0 0 1 * * /opt/renew_mongo_cert.sh ${APP_DNS_NAME}"; } | crontab -

systemctl restart mongod

# only for primary mongodb instance
if [$VM_INDEX == 0]
then
    sleep 60 # waiting for other mongodb instances to be successfully configured
    openssl rand -base64 741 > /opt/mongodb.key
    chmod 600 /opt/mongodb.key
    chown mongodb /opt/mongodb.key
    systemctl restart mongod

    #copy keyfile to other mongodb instances, chmod, chown & restart mongodb there as well

    #sleep some time

    mongo admin -u ${MONGODB_USERNAME} -p ${MONGODB_PASSWORD} --eval 'rs.initiate()'

    #for vmindex +1
    for (( c=1; c<$VM_SIZE; c++ ))
    do
        #replace 0. with 1., 2. etc
        VM_DNS=$(sed -e "s/0./$c./g" <<< $APP_DNS_NAME)
        echo $VM_DNS
        mongo admin -u ${MONGODB_USERNAME} -p ${MONGODB_PASSWORD} --eval 'rs.add("'${VM_DNS}':27017")'
        sleep 5
    done
fi

