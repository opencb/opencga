FROM opencga-java-base

ARG BRANCH="1.4.0"

# Install dependencies
RUN apt-get update && \
apt-get install -y git build-essential curl wget unzip tar software-properties-common vim maven git curl

WORKDIR /build

# clone git master branch
RUN git clone -b ${BRANCH} https://github.com/opencb/opencga.git .

COPY settings.xml .

RUN mvn clean install --settings settings.xml -DskipTests -Dstorage-hadoop -Popencga-storage-hadoop-deps -Phdp-2.6.0 -DOPENCGA.STORAGE.DEFAULT_ENGINE=hadoop -Dopencga.war.name=opencga

RUN rm -fr ~/.m2