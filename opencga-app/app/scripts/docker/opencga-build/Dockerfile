FROM openjdk:11

RUN apt-get update && \
apt-get install -y \
git \
build-essential \
curl \
wget \
unzip \
tar \
software-properties-common \
vim \
maven

WORKDIR /build

COPY . .

RUN mvn clean install --settings ./opencga-app/app/scripts/docker/opencga-build/settings.xml -DskipTests -Dstorage-hadoop -Popencga-storage-hadoop-deps -Phdp-2.6.0 -DOPENCGA.STORAGE.DEFAULT_ENGINE=hadoop -Dopencga.war.name=opencga

RUN rm -fr ~/.m2