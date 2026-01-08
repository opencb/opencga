#!/bin/bash
# get_modules.sh
# This script outputs the OpenCGA modules matrix for the CI workflow.
# Usage:
#   ./get_modules.sh <module>
#
# Argument:
#   module - OpenCGA module (e.g., all, opencga-core, ...)
#
# Output:
#   Prints a JSON array of modules for the matrix.

MODULE_INPUT="$1"

if [ "$MODULE_INPUT" == "all" ]; then
  echo '["opencga-analysis", "opencga-app", "opencga-catalog", "opencga-client", "opencga-core", "opencga-master", "opencga-server", "opencga-storage", "opencga-test"]'
else
  echo '["'"$MODULE_INPUT"'"]'
fi

