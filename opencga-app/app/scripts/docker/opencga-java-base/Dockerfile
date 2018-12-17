# up version?
FROM ubuntu:16.04

# Take from: https://launchpad.net/~webupd8team/+archive/ubuntu/java
ARG JAVA_VERSION="8u191-1~webupd8~1"

# Install dependencies
RUN apt-get update && \
apt-get install -y software-properties-common 

# Install JDK 8
RUN \
echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
add-apt-repository -y ppa:webupd8team/java && \
apt-get update && \
apt-get install -y oracle-java8-installer=${JAVA_VERSION} && \
rm -rf /var/lib/apt/lists/* && \
rm -rf /var/cache/oracle-jdk8-installer

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle