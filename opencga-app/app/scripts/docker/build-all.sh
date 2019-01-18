#!/bin/bash

# ----------------------
# build-all.sh
# ----------------------
#
# The purpose of this script is to build all
# Dockerfiles against the current source code.
#
# Usage
# ----------------------
#
# This script is not intended to be run directly, rather
# indirectly by the Makefile.
#
# Arguments
# ----------------------
# - 1: The path to an envrionment file
# - 2: The path to an opencga build directory
#
# Variables
# ----------------------
#
# Variables can be loaded into the build process in 2 ways:
# - Set as environment variable
# - Set in a `make_env` .env file
#
# The default location for the `make_env` file is `./opencga-app/app/scripts/docker/make_env`.
# If you wish to use a different file location, please provide the ENVFILE
# envrionment variable to the `make` command at runtime.
# The `make_env` file is formatted like any other .env file i.e.
#   KEY0=VALUE0
#   KEY1=VALUE1
#   KEY2=VALUE2
#
# Available variables:
#
# - DOCKER_USERNAME=''   : (required) Username to login to the docker registry
# - DOCKER_PASSWORD=''   : (required) Password to login to the docker registry
# - DOCKER_SERVER=''     : (optional) Docker registry server (default: docker.io)
# - DOCKER_REPO=''       : (optional) Docker registry repository (default: docker username for docker.io)
# - DOCKER_BUILD_ARGS='' : (optional) Additional build arguments to pass to the docker build command
# - PUBLISH=''           : (optional) Set to 'true' to publish the docker images to a container registry
# - TAG=''               : (optional) Set to override the default Git commit SHA docker image tag
#
# When you have set your desired variables, you can simply run the Makefile with `make`.

set -e

envfile=$1
buildPath=$2

# Jump to the repo root dir so that we have a known working dir
cd $(git rev-parse --show-toplevel)
dockerDir="opencga-app/app/scripts/docker"

# make_image directory, image name, make target
function make_image {
    ENVFILE="${envfile}" \
    APP_NAME="${1}" \
    MAKECMDGOALS="${2}" \
    make -f "${dockerDir}/Makefile" "${2}"
}

# Define all the docker images in dependecy order
declare -a images=(opencga opencga-app opencga-daemon opencga-init iva)
imageCount=0
imagesLen="${#images[@]}"
imagesLen=$((imagesLen-1))

echo "---------------------"
echo "Started building container images"
echo "---------------------"

# Build OpenCGA
if [ ! -d "${buildPath}" ]; then
    echo "> No existing OpenCGA build."
    echo "> Starting OpenCGA build."
    docker run -it --rm \
    -v "$PWD":/src \
    -v "$HOME/.m2":/root/.m2 \
    -w /src maven:3.6-jdk-8 \
    mvn -T 1C install \
    -DskipTests -Dstorage-hadoop -Popencga-storage-hadoop-deps -Phdp-2.6.0 -DOPENCGA.STORAGE.DEFAULT_ENGINE=hadoop -Dopencga.war.name=opencga
    echo "> Finished OpenCGA build."
else
    echo "> Using existing build from $PWD/build"
fi

# Build all the child container images
for image in "${images[@]}"
do
    echo
    echo "--> Building image '${image}' : [${imageCount}/${imagesLen}]"
    make_image "$image" build
    echo "--> Done building image '${image}'"
    echo
    imageCount=$((imageCount+1))
done

echo "---------------------"
echo "Finished building container images"
echo "---------------------"
echo
echo "---------------------"
echo "Started publishing container images"
echo "---------------------"

# Publish the container images
if [ "$PUBLISH" = true ]; then
    imageCount=0
    for image in "${images[@]}"
    do
        if [ "${image}" = "opencga" ]; then
            continue
        fi

        echo
        echo "--> Publishing image '${image}' : [${imageCount}/${imagesLen}]"
        make_image "$dockerDir" "$image" publish
        echo "--> Done Publishing image '${image}'"
        echo
        imageCount=$((imageCount+1))
    done
else
        echo "Not publishing any docker images"
fi

echo "---------------------"
echo "Finished publishing container images"
echo "---------------------"