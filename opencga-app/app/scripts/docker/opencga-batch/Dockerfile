FROM opencga

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions
VOLUME /opt/opencga/variants

# Install all the Hortonworks Data Platform stuff we need
# hadoop etc
RUN wget https://private-repo-1.hortonworks.com/HDP/ubuntu16/2.x/updates/2.6.5.3007-3/hdp.list -O /etc/apt/sources.list.d/hdp.list && apt-get updates
RUN apt-get install zookeeper hadoop hadoop-hdfs libhdfs0 hadoop-yarn hadoop-mapreduce hadoop-client openssl \
 hbase \
 libsnappy1 libsnappy-dev liblzo2-2 liblzo2-dev hadooplzo phoenix \

# # Copy hadoop setup script into the container
# COPY ./opencga-app/app/scripts/docker/opencga-batch /tmp/
# # This ensures that the script will run every time a new bash session is started
# RUN chmod +x /tmp/setup-hadoop.sh && echo ". /tmp/setup-hadoop.sh" >> /etc/bash.bashrc

USER 1001:1001