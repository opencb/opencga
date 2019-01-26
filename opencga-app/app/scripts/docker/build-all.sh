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
# - DOCKER_ORG =''       : (optional) Docker organization (default: docker username for users on docker.io)
# - DOCKER_BUILD_ARGS='' : (optional) Additional build arguments to pass to the docker build command
# - PUBLISH=''           : (optional) Set to 'true' to publish the docker images to a container registry
# - TAG=''               : (optional) Set to override the default Git commit SHA docker image tag
#
# When you have set your desired variables, you can simply run the Makefile with `make`.

# Functions
function make_image {
    ENVFILE="${envfile}" \
    APP_NAME="${1}" \
    make --no-print-directory -f "${dockerDir}/Makefile.docker" "${2}"
}

function version {
    echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }';
}

# Script
set -e

# Check prerequisites
minimumVersion="18.0.0"
dockerVersion=$(docker version --format '{{.Server.Version}}')
if [ "$(version "$dockerVersion")" -lt "$(version "$minimumVersion")" ]; then
    echo
    echo "Error:"
    echo "---"
    echo "This script requires at least Docker version $minimumVersion"
    echo "You currently have $dockerVersion installed, please update and try again"
    echo
    exit 1
fi

# Arguments
envfile=$1
buildPath=$2
dockerDir="opencga-app/app/scripts/docker"

# Validate parameters
if [ -f "${envfile}" ]; then
    . "${envfile}"  # If an envfile is set - use it
fi
if [ "$PUBLISH" ]; then
    if [ -z "$DOCKER_USERNAME" ]; then
        echo "DOCKER_USERNAME is required when parameter PUBLISH is true";
        exit 1;
    fi
    if [ -z "$DOCKER_PASSWORD" ]; then
        echo "DOCKER_PASSWORD is required when parameter PUBLISH is true";
        exit 1;
    fi
fi

# Define all the docker images in dependecy order
declare -a images=(opencga opencga-app opencga-daemon opencga-init iva)
imageCount=0
imagesLen="${#images[@]}"
imagesLen=$((imagesLen-1))

echo "---------------------"
echo "Started build job"
echo "---------------------"
echo

# Print useful build job metadata
ENVFILE="${envfile}" make --no-print-directory -f "${dockerDir}/Makefile.docker" "metadata"

# Build OpenCGA
# This needs to also run mvn install:install-file -Dfile ./opencga-app/app/scripts/azure/libs/hadoop-azure-2.7.3.2.6.5.3005-27.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-azure -Dversion=2.7.3.2.6.5.3005-27 -Dpackaging=jar 
# Also for hadoop-common I can't add and  test this as have remote docker
if [ ! -d "${buildPath}" ]; then
    echo "> No existing OpenCGA build, building from source..."
    docker run -it --rm \
    -v "$PWD":/src \
    -v "$HOME/.m2":/root/.m2 \
    -w /src maven:3.6-jdk-8 \
    mvn -T 1C install \
    -DskipTests -Dstorage-hadoop -Popencga-storage-hadoop-deps -Phdp-2.6.5-azure -DOPENCGA.STORAGE.DEFAULT_ENGINE=hadoop -Dopencga.war.name=opencga
    echo "> Finished OpenCGA build"
else
    echo "> Using existing OpenCGA build from $PWD/build"
fi

echo
echo "---------------------"
echo "Started building container images"
echo "---------------------"

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

# Publish the container images
if [ "$PUBLISH" = true ]; then
    echo "---------------------"
    echo "Started publishing container images"
    echo "---------------------"

    imageCount=0
    for image in "${images[@]}"
    do
        if [ "${image}" = "opencga" ]; then
            continue
        fi

        echo
        echo "--> Publishing image '${image}' : [${imageCount}/${imagesLen}]"
        make_image "$image" publish
        echo "--> Done Publishing image '${image}'"
        echo
        imageCount=$((imageCount+1))
    done

    echo "---------------------"
    echo "Finished publishing container images"
    echo "---------------------"
else
    echo "---------------------"
    echo "Not publishing any container images"
    echo "---------------------"
fi

echo
echo "---------------------"
echo "Finished build job"
echo "---------------------"