#!/bin/bash

set -e

VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "version=$VERSION" >> "$GITHUB_OUTPUT"
IMAGE="opencb/ext-tools:${VERSION}"

echo "Checking if Docker image exists: $IMAGE"

TOKEN=$( curl -sSLd "username=$DOCKER_USERNAME&password=$DOCKER_PASSWORD" https://hub.docker.com/v2/users/login | jq -r ".token" )
RESPONSE=$( curl -sH "Authorization: JWT $TOKEN" "https://hub.docker.com/v2/repositories/opencb/opencga-ext-tools/tags/$VERSION/" | jq .)



# Check if 'message' key exists -> tag not found
EXISTS=$( echo "$RESPONSE" | jq -r 'has("message") | not' )

if [ "$EXISTS" = "true" ]; then
  echo "Image exists for tag $VERSION"
  echo "exists=true" >> "$GITHUB_OUTPUT"
else
  echo "Image does not exist for tag $VERSION"
  echo "exists=false" >> "$GITHUB_OUTPUT"
fi