#!/bin/bash

# Check if script files is provided
if [ -n "$1" ] ; then
  docker exec -it daemon-opencga /opt/opencga/$1
else
  echo "No argument provided"
fi