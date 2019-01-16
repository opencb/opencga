#!/bin/bash

# ----------------------
# build-all.sh
# ----------------------
#
# The purpose of this script is to build all
# Dockerfiles against the latest git commit.
#
# Variables can be loaded into the build process in 2 ways:
# - Set as environment variable
# - Set in a `make_env` .env file
#
# The default location for the `make_env` file is `./opencga-app/app/scripts/docker/make_env`.
# If you wish to use a different file location, please provide the ENVFILE
# envrionment variable to the `make` command at runtime. The `make_env` file is formatted
# like any other .env file i.e.
#   KEY0=VALUE0
#   KEY1=VALUE1
#   KEY2=VALUE2
#
# Variables
# ----------------------
# A number of variables can be set to configure the build process. Review the list below
# to decide which to set for your environment.
#
# - DOCKER_USERNAME='' : (required) Username to login to the docker registry
# - DOCKER_PASSWORD='' : (required) Password to login to the docker registry
# - DOCKER_SERVER='' : (optional) Docker registry server (default: docker.io)
# - DOCKER_REPO='' : (optional) Docker registry repository (default: docker username for docker.io)
# - DOCKER_BUILD_ARGS='' : (optional) Additional build arguments to pass to the docker build command
# - SEMVER='' : (optional) Semantic version to tag images with in releases (default: 0.0.0)
# - RELEASE_BRANCH='' : (optional) Your release branch (default: master)
# - PUSH='' : (optional) Flag to publish docker images on non-release builds
# - TAG='' : (optional) Force set a specific tag for the docker image
#
# If you are building on your release branch, the docker image will automatically
# be tagged with both the git commit SHA and the provided semantic version. This
# image will then be pushed to the provided docker registry.
#
# When you have set your desired variables, you can simply run the Makefile with `make`.

set -e

BRANCH=$(git branch | grep \* | cut -d ' ' -f2)
COMMIT=$(git rev-parse --verify HEAD)

echo
echo "---------------------"
echo "Running build process"
echo "---------------------"
echo "Release Branch:       $5"
echo "Current Branch:       ${BRANCH}"
echo "Current Commit:       ${COMMIT}"
echo "Semantic Version:     $2"
echo "Docker Tag:           $1"
echo "Docker Repo:          $3"
echo "Docker Username:      $4"
echo "Flags:"
echo " - PUSH:              $6"
echo "---------------------"

dockerDir="./opencga-app/app/scripts/docker"

# Jump to the repo root dir so that we have a global docker context
cd $(git rev-parse --show-toplevel)

echo
echo "------ Started building all container images ------"
echo

# Define all the docker images in dependecy order
declare -a images=(opencga-java-base opencga-build opencga opencga-app opencga-daemon opencga-init iva)
imageCount=0
imagesLen=${#images[@]}
imagesLen=$((imagesLen-1))
for image in "${images[@]}"
do
    echo
    echo "------ Image ${image} : [${imageCount}/${imagesLen}] ------"
    echo
    echo "> Building"
    export ENVFILE="$dockerDir/make_env"
    export APP_NAME="${image}"
    export PATH_PREFIX="$dockerDir/$image"
    make -f "$dockerDir/Makefile" image

    if [ "$BRANCH" = "$RELEASE_BRANCH" ]; then
        echo "> Releasing"
        make -f "$dockerDir/Makefile" release
    elif [ ! -z "$PUSH" ]; then
        echo "> Publishing"
        make -f "$dockerDir/Makefile" publish
    fi
    echo "------ Image ${image} finished ------"
    echo
    imageCount=$((imageCount+1))
done
echo
echo "------ Finished building all container images ------"
echo