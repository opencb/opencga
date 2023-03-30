#!/bin/bash
#Navigate to the root folder where the pom.xml is
cd /home/runner/work/opencg-enterprise/opencg-enterprise || exit 2

#Read the opencga version from the pom.xml
BUILD_VERSION=$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)

#We remove the -SNAPSHOT if it exists
CLEAN_BUILD_VERSION=$(echo "$BUILD_VERSION" | cut -d "-" -f 1)
#Count the number of points to know if it is a portpatch
COUNT=$(echo "$CLEAN_BUILD_VERSION" | grep -o '\.' | wc -l )

#Read the numbers separately to compose the name of the branch
MAJOR=$(echo "$CLEAN_BUILD_VERSION" | cut -d "." -f 1)
MINOR=$(echo "$CLEAN_BUILD_VERSION" | cut -d "." -f 2)
PATCH=$(echo "$CLEAN_BUILD_VERSION" | cut -d "." -f 3)

#Is Portpatch
if [ "$COUNT" -gt 2 ]; then
  echo "release-$MAJOR.$MINOR.$PATCH.x"
  exit 0
fi

#Is develop branch
if [[ "$PATCH" ==  "0" ]]; then
    echo "develop"
    exit 0
else #Is release branch
  echo "release-$MAJOR.$MINOR.x"
  exit 0
fi

