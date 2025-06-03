#!/bin/bash

set -e

VERSION="$1"
IMAGE="opencb/ext-tools:${VERSION}"

echo "Checking if Docker image exists: $IMAGE"

TOKEN=$(curl -s -u "$DOCKER_USERNAME:$DOCKER_PASSWORD" \
  "https://hub.docker.com/v2/users/login/" | jq -r .token)

EXISTS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: JWT $TOKEN" \
  "https://hub.docker.com/v2/repositories/opencb/ext-tools/tags/${VERSION}/")

if [ "$EXISTS" -eq 200 ]; then
  echo "Image exists"
  echo "exists=true" >> "$GITHUB_OUTPUT"
else
  echo "Image does not exist"
  echo "exists=false" >> "$GITHUB_OUTPUT"
fi
