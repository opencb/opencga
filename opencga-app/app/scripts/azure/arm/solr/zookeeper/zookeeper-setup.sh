#!/usr/bin/env bash

set -x
set -e

MY_ID=$1
SUBNET_PREFIX=$2
IP_FIRST=$3
NUM_NODES=$4


wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie"  "https://download.oracle.com/otn-pub/java/jdk/8u191-b12/2787e4a523244c269598db4e85c51e0c/jdk-8u191-linux-x64.tar.gz"
tar -xvf jdk-8*
mkdir /usr/lib/jvm
mv ./jdk1.8* /usr/lib/jvm/jdk1.8.0
update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0/bin/java" 1
update-alternatives --install "/usr/bin/javac" "javac" "/usr/lib/jvm/jdk1.8.0/bin/javac" 1
update-alternatives --install "/usr/bin/javaws" "javaws" "/usr/lib/jvm/jdk1.8.0/bin/javaws" 1
chmod a+x /usr/bin/java
chmod a+x /usr/bin/javac
chmod a+x /usr/bin/javaws

cd /usr/local

wget "http://mirrors.ukfast.co.uk/sites/ftp.apache.org/zookeeper/stable/zookeeper-3.4.12.tar.gz"
tar -xvf "zookeeper-3.4.12.tar.gz"

touch zookeeper-3.4.12/conf/zoo.cfg

echo "tickTime=2000" >> zookeeper-3.4.12/conf/zoo.cfg
echo "dataDir=/var/lib/zookeeper" >> zookeeper-3.4.12/conf/zoo.cfg
echo "clientPort=2181" >> zookeeper-3.4.12/conf/zoo.cfg
echo "initLimit=5" >> zookeeper-3.4.12/conf/zoo.cfg
echo "syncLimit=2" >> zookeeper-3.4.12/conf/zoo.cfg

i=0
while [ $i -lt $NUM_NODES ]
do
    echo "server.$i=${SUBNET_PREFIX}$(($i+$IP_FIRST)):2888:3888" >> zookeeper-3.4.12/conf/zoo.cfg
    i=$(($i+1))
done

mkdir -p /var/lib/zookeeper

echo ${MY_ID} >> /var/lib/zookeeper/myid

zookeeper-3.4.12/bin/zkServer.sh start
