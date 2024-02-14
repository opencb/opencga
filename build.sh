#!/bin/bash

echo "DEPRECATED: Use 'run.sh build' instead of build.sh."
$(dirname "$0")/run.sh build "$@"