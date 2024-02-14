#!/bin/bash

echo "DEPRECATED: Use 'run.sh test' instead of test.sh."
$(dirname "$0")/run.sh test "$@"