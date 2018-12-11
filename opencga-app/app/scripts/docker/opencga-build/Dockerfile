FROM opencga-java-base

# Install dependencies
RUN apt-get update && \
apt-get install -y git build-essential curl wget unzip tar software-properties-common vim maven git curl

WORKDIR /build

# clone git master branch
RUN git clone -b release-1.4.0 https://github.com/opencb/opencga.git .

COPY settings.xml .

RUN mvn dependency:go-offline  --settings settings.xml -Dstorage-hadoop -Popencga-storage-hadoop-deps -Phdp-2.6.0 -DOPENCGA.STORAGE.DEFAULT_ENGINE=hadoop

# compile 
RUN mvn clean install --offline --settings settings.xml -DskipTests - -Dstorage-hadoop -Popencga-storage-hadoop-deps -Phdp-2.6.0 -DOPENCGA.STORAGE.DEFAULT_ENGINE=hadoop

RUN rm -fr ~/.m2