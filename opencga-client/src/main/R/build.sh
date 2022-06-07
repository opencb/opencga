#!/bin/sh

docker run --mount type=bind,source=$1,target=/opt/opencga/R --mount type=bind,source=/tmp,target=/opt/opencga opencb/opencga-ext-tools:$2 R CMD build /opt/opencga/R
