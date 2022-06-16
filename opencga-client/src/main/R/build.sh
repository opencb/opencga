#!/bin/sh

docker run --mount type=bind,source=$1,target=/opt/opencga/R --mount type=bind,source=/tmp,target=/opt/opencga opencb/opencga-r-builder:1.0.0 R CMD build /opt/opencga/R
