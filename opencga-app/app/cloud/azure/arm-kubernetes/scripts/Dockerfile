# Simple Docker file to test the mount.py scripts
# without the danger of breaking your local '/etc/fstab file'
FROM ubuntu:18.04

# Install local dependencies
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip setuptools
WORKDIR /tmp/
COPY . .

RUN python3 -m unittest -v test*.py