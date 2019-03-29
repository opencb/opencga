# Setup OpenCGA on Azure
The following guide shows how to successfully configure OpenCGA on Azure using different storage engines. It sets up a single virtual machine with OpenCGA, Tomcat, MongoDB, Solr and Redis Cache. Optionally it also sets up an HDInsight HBase cluster with either Azure Storage or Azure DataLake.

## Deploy resources
Depending on what storage engine you want to use, several resources have to be created in Azure. Remember the username and password for each resources you create, as we have to ssh into the virtual machines to configure them later.

### OpenCGA Virtual Machine
The only required resource for a minimum working product is a virtual machine in Azure.

- Create a new resource
- Search for **Ubuntu Server 16.04 LTS** (18.04 is incompatible with MongoDB 3.x)
- For better performance, select a storage optimized VM (e.g. L8s or L16s)

### Azure DataLake (optional)
If you want to leverage Hadoop as a backend with DataLake instead of Azure Storage, DataLake needs to be deployed. DataLake is a bit more expensive, but more performant and required for large loads (+500TB)

- Create a new resource
- Search for **Data Lake Storage Gen1** (Gen2 might be available in the future)
- Make sure to deploy it in the same region as your OpenCGA VM

### Azure HDInsights - HBase (optional)
If you want to leverage OpenCGA's more performant Hadoop backend instead of MongoDB, you have to create an Azure HDInsight instance with HBase.

- Create a new resource
- Search for **HDInsight**
- Step 1: Click on the Custom tab (instead of Quick Create)
- Step 1: Select **HBase 1.1.2 (HDI 3.6)** as the Cluster type
- Step 1: Deploy it in the same region as your OpenCGA VM
- Step 2: Place it in the same Virtual Network as your OpenCGA VM (select the virtual network that the VM has created when it was deployed)
- Step 3: Configure Storage with either _Azure Storage_ or (optionally) the newly created _Data Lake Storage Gen1_ instance
- Step 5: Depending on the required performance any cluster size is possible from 2 region nodes with minimum specs to more nodes with higher specs


## Setup resources
After successfully deploying the resources, they have to be configured

### OpenCGA Virtual Machine
SSH into your machine (replace username and IP address)

```
ssh azure@139.349.12.49
````

Install the latest updates
```
sudo apt-get update
sudo apt-get upgrade -y
```

Install Java SDK

```
sudo -- sh -c 'echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections &&  add-apt-repository -y ppa:webupd8team/java &&  apt-get update &&  apt-get install -y oracle-java8-installer &&  rm -rf /var/lib/apt/lists/* &&  rm -rf /var/cache/oracle-jdk8-installer'
```

Install Redis, Tomcat, Maven and Git

```
sudo add-apt-repository universe && sudo apt-get update && sudo apt-get install redis-server redis-tools tomcat8 maven git-core -y
```

Install MongoDB 3.6

```
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.6 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.6.list
sudo apt-get update
sudo apt-get install -y mongodb-org
sudo systemctl enable mongod.service # start on boot
```

Install Solr

```
wget https://www-eu.apache.org/dist/lucene/solr/6.6.5/solr-6.6.5.tgz
tar xvfz solr-6.6.5.tgz
sudo ./solr-6.6.5/bin/install_solr_service.sh ./solr-6.6.5.tgz
```

Clone OpenCGA
```
git clone -b azure https://github.com/opencb/opencga.git
cd opencga/
```

**Option 1: Build OpenCGA with MongoDB**
```
mvn clean install -DskipTests
```

**Option 2: Build OpenCGA with Hadoop**
```
mvn clean install -DskipTests -Dstorage-hadoop -Popencga-storage-hadoop-deps -Phdp-2.6.5 -DOPENCGA.STORAGE.DEFAULT_ENGINE=hadoop
```

Move OpenCGA to `/opt/opencga`
```
sudo mkdir /opt/opencga
sudo cp -r ~/opencga/build/* /opt/opencga
```

Increase memory heap space for tomcat to avoid running out of memory
```
printf '#!/bin/sh\nexport CATALINA_OPTS="$CATALINA_OPTS -Xmx1024m"\nexport CATALINA_OPTS="$CATALINA_OPTS -Xms512m"' | sudo tee -a /usr/share/tomcat8/bin/setenv.sh
```


Create OpenCGA user and group to run Tomcat server
```
sudo service tomcat8 stop #stop tomcat to make changes
sudo addgroup opencga
sudo adduser -ingroup opencga --disabled-password --gecos "" opencga #create user opencga
sudo sed -i 's/=tomcat8/=opencga/' /etc/default/tomcat8 # change both user and group
sudo chown -R opencga:adm /var/log/tomcat8
sudo chown -R opencga:opencga /var/lib/tomcat8/webapps
sudo chown opencga:adm /var/cache/tomcat8
sudo chown -R opencga:opencga /var/cache/tomcat8/Catalina
sudo usermod -a -G tomcat8 opencga
sudo service tomcat8 start #start tomcat
```

Copy OpenCGA build to Tomcat server
```
sudo cp /opt/opencga/opencga*.war /var/lib/tomcat8/webapps/opencga.war
sudo chown -R tomcat8 /opt/opencga/ #give tomcat8 user ownership of opencga folder
sudo chmod -R 777 /opt/opencga/ #necessary to write in sessions folder
```

If you chose Hadoop as a backend, configure Tomcat to use this configuration:
```
sudo -- sh -c 'echo "<Context>\n\t<Resources>\n\t\t<PostResources className=\"org.apache.catalina.webresources.DirResourceSet\" webAppMount=\"/WEB-INF/classes\" base=\"/opt/opencga/conf/hadoop\" />\n\t</Resources>\n</Context>" >> /var/lib/tomcat8/conf/Catalina/localhost/opencga.xml'
```

Reboot VM to load your configuration
```
sudo reboot
```

### Azure HDInsight cluster (optional)
SSH into the HDInsight cluster (change domain name)
```
ssh sshuser@opencga-ssh.azurehdinsight.net
```

Change hbase.client.keyvalue.maxsize to 0 in `/etc/hbase/conf/hbase-site.xml` on hadoop cluster to avoid 'KeyValue size too large' error
```
sudo sed -i '/<name>hbase.client.keyvalue.maxsize<\/name>/!b;n;c<value>0</value>' /etc/hbase/conf/hbase-site.xml
```

Copy required Hadoop and HBase configuration files to the OpenCGA Virtual Machine (change username & IP address)
```
sudo scp -r /etc/hadoop/conf/* azure@139.349.12.49:/opt/opencga/conf/hadoop
sudo scp -r /etc/hbase/conf/* azure@139.349.12.49:/opt/opencga/conf/hadoop
```
