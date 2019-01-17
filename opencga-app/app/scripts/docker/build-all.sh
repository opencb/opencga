#!/bin/bash

# ----------------------
# build-all.sh
# ----------------------
#
# The purpose of this script is to build all
# Dockerfiles against the current source code.
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
# - PUBLISH='' : (optional) Set to 'true' to publish the docker images to a container registry
# - TAG='' : (optional) Set to override the default Git commit SHA docker image tag
#
# When you have set your desired variables, you can simply run the Makefile with `make`.

set -e

BRANCH=$(git branch | grep \* | cut -d ' ' -f2)
COMMIT=$(git rev-parse --verify HEAD)

echo
echo "---------------------"
echo "Running build process"
echo "---------------------"
echo "Branch:          ${BRANCH}"
echo "Commit:          ${COMMIT}"
echo "Docker Tag:      $1"
echo "Docker Repo:     $2"
echo "Docker Username: $3"
echo "Publish:         $4"
echo "---------------------"
echo

dockerDir="./opencga-app/app/scripts/docker"

# Jump to the repo root dir so that we have a known working dir
cd $(git rev-parse --show-toplevel)

# make_image directory, image name, make target
function make_image {
    ENVFILE="${1}/make_env" \
    APP_NAME="${2}" \
    make -f "${1}/Makefile" ${3}
}

# Define all the docker images in dependecy order
declare -a images=(opencga-build opencga opencga-app opencga-daemon opencga-init iva)
imageCount=0
imagesLen=${#images[@]}
imagesLen=$((imagesLen-1))

echo "---------------------"
echo "Started building container images"
echo "---------------------"

# Build all the container images
for image in "${images[@]}"
do
    echo
    echo "--> Building image '${image}' : [${imageCount}/${imagesLen}]"
    make_image "$dockerDir" "$image" build
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
if [ "$PUBLISH" = true ];
then
    make_image "$dockerDir" opencga-init publish
    make_image "$dockerDir" opencga-app publish
    make_image "$dockerDir" opencga-daemon publish
    make_image "$dockerDir" iva publish
else
    echo "Not publishing docker images"
fi

echo "---------------------"
echo "Finished publishing container images"
echo "---------------------"