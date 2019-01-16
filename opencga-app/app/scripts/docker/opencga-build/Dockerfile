FROM maven:3.3.9-jdk-8

WORKDIR /build

# Copying across project poms
COPY ./pom.xml ./pom.xml
COPY ./opencga-analysis/pom.xml ./opencga-analysis/pom.xml
COPY ./opencga-app/pom.xml ./opencga-app/pom.xml
COPY ./opencga-catalog/pom.xml ./opencga-catalog/pom.xml
COPY ./opencga-client/pom.xml ./opencga-client/pom.xml
COPY ./opencga-core/pom.xml ./opencga-core/pom.xml
COPY ./opencga-server/pom.xml ./opencga-server/pom.xml
COPY ./opencga-storage/pom.xml ./opencga-storage/pom.xml
COPY ./opencga-storage/opencga-storage-app/pom.xml ./opencga-storage/opencga-storage-app/pom.xml
COPY ./opencga-storage/opencga-storage-benchmark/pom.xml ./opencga-storage/opencga-storage-benchmark/pom.xml
COPY ./opencga-storage/opencga-storage-core/pom.xml ./opencga-storage/opencga-storage-core/pom.xml
COPY ./opencga-storage/opencga-storage-hadoop/pom.xml ./opencga-storage/opencga-storage-hadoop/pom.xml
COPY ./opencga-storage/opencga-storage-hadoop/opencga-storage-hadoop-core/pom.xml ./opencga-storage/opencga-storage-hadoop/opencga-storage-hadoop-core/pom.xml
COPY ./opencga-storage/opencga-storage-hadoop/opencga-storage-hadoop-deps/pom.xml ./opencga-storage/opencga-storage-hadoop/opencga-storage-hadoop-deps/pom.xml
COPY ./opencga-storage/opencga-storage-mongodb/pom.xml ./opencga-storage/opencga-storage-mongodb/pom.xml
COPY ./opencga-storage/opencga-storage-server/pom.xml ./opencga-storage/opencga-storage-server/pom.xml
COPY ./opencga-test/pom.xml ./opencga-test/pom.xml

# Copy across settings
COPY ./opencga-app/app/scripts/docker/opencga-build/settings.xml .

# Build all dependencies
RUN mvn dependency:go-offline -B --settings settings.xml

# Copy the rest of the source files
COPY . .

# Build the application
RUN mvn -T 1C clean install --settings settings.xml -DskipTests -Dstorage-hadoop -Popencga-storage-hadoop-deps -Phdp-2.6.0 -DOPENCGA.STORAGE.DEFAULT_ENGINE=hadoop -Dopencga.war.name=opencga

RUN rm -fr ~/.m2