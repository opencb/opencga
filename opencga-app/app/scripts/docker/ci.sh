#!/bin/bash

# ----------
# ci.sh
# ----------
#
# The purpose of this script is to build all
# Dockerfiles against the latest git commit.
# If the build occurs on a branch other than the
# specified release branch the image tag will be
# the git commit SHA. Each image will be built,
# tagged and pushed to the specified container registry.
#
# This script can be used locally for development by manually
# creating a make_env file and setting the following variables.
#
# - DOCKER_USERNAME : Username to login to the docker registry
# - DOCKER_PASSWORD : Password to login to the docker registry
# - DOCKER_SERVER : Docker registry server (default: docker.io)
# - DOCKER_REPO : Docker registry repository (default: docker username for docker.io)
# - ENVFILE - The path to the make_env file you wish Make to include (default: make_env)
#
# If this script is ran as part of a CI process it is expected
# that the variables will be set as environment variables. Set
# the CI environment variable to true. This will ensure the CI
# build environment is dumped into a make_env so that
# the process works the same as locally.
# If the script is ran on the specified RELEASE_BRANCH you need
# to ensure following variable is set so that the image can be
# tagged correctly:
#
# - SEMVER : The semantic version used to tag a release
#
# If you want to pass additional build arguments to the docker
# build command you can do this by overriding the following variable:
#
# - DOCKER_BUILD_ARGS
#

set -e

# Validate all variables required to run any target
# defined in this script are set.
[ -z "$DOCKER_USERNAME" ] && echo "DOCKER_USERNAME is a required parameter"
[ -z "$DOCKER_PASSWORD" ] && echo "DOCKER_PASSWORD is a required parameter"

RELEASE_BRANCH="master"
BRANCH=$(git branch | grep \* | cut -d ' ' -f2)
COMMIT=$(git rev-parse --verify HEAD)
SEMVER="${SEMVER:-0.0.0}"
if [ "$BRANCH" = "$RELEASE_BRANCH" ]; then
    echo "..."
    echo "Build type: Release"
    echo "Commit: $COMMIT"
    echo "Version: v$SEMVER"
    echo "..."
    TARGET=release
else
    echo "..."
    echo "Build type: Non-Release"
    echo "Commit: $COMMIT"
    echo "Version: v$SEMVER"
    echo "..."
    TARGET=publish
fi

if [ "$CI" ]; then
    touch make_env
    env > make_env
fi

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
    echo "------ Started building ${image} : [${imageCount}/${imagesLen}] ------"
    ENVFILE="$dockerDir/make_env" \
    APP_NAME="${image}" \
    PATH_PREFIX="$dockerDir/$image" \
    make -f "$dockerDir/Makefile" "${TARGET}"
    echo "------ Finished building ${image} ------"
    echo
    imageCount=$((imageCount+1))
done
echo
echo "------ Finished building all container images ------"
echo