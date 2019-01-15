#!/bin/bash

set -e

# Define all the docker images in dependecy order
declare -a images=(opencga-java-base opencga-build opencga opencga-app opencga-daemon opencga-init iva)

echo "------ Started building all images ------"
echo
imageCount=0
imagesLen=${#images[@]}
imagesLen=$((imagesLen-1))
for image in "${images[@]}"
do
    echo
    echo "------ Started building ${image} @ [${imageCount}/${imagesLen}] ------"
    docker build -t ${image} "./${image}"
    echo "------ Finished building ${image} ------"
    echo
    imageCount=$((imageCount+1))
done
echo
echo "------ Finished building all images ------"