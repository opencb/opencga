FROM opencga

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions
VOLUME /opt/opencga/variants

# Install all the Hortonworks Data Platform stuff we need
# hadoop etc
COPY ./opencga-app/app/scripts/docker/opencga-batch/hdp-azure.pub /tmp/
RUN apt-key add /tmp/hdp-azure.pub
RUN apt-get update && apt-get install apt-transport-https ca-certificates -y
RUN wget https://private-repo-1.hortonworks.com/HDP/ubuntu16/2.x/updates/2.6.5.3007-3/hdp.list -O /etc/apt/sources.list.d/hdp.list && apt-get update
RUN apt-get install zookeeper hadoop hadoop-hdfs libhdfs0 hadoop-yarn hadoop-mapreduce hadoop-client openssl hbase \
 libsnappy1 liblzo2-2 liblzo2-dev phoenix -y

RUN apt-get install openjdk-8-jdk -y

 # Node removed - libsnappy-dev as causes an issue

# Check hadoop is now on path
RUN hadoop

USER 1001:1001