#!/bin/bash

# This script builds the test profile string for the OpenCGA CI workflow.
# It is used to determine which test types (short, medium, long) should be run.
#
# Usage:
#   ./get_profiles.sh <short_tests> <medium_tests> <long_tests>
#
# Arguments:
#   short_tests   - true/false, whether to include short tests
#   medium_tests  - true/false, whether to include medium tests
#   long_tests    - true/false, whether to include long tests
#
# Output:
#   Prints a comma-separated string of active test profiles (e.g., runShortTests,runMediumTests)
#   If no profile is selected, defaults to 'runShortTests'.

# Check that exactly 3 arguments are provided
if [ $# -eq 0 ];  then
    echo "The arguments must be 3"
    exit 1
fi

PROFILE=""

# Add 'runShortTests' to the profile string if the first argument is 'true'
if [ $1 == "true" ]; then
  PROFILE="${PROFILE}runShortTests,"
fi
# Add 'runMediumTests' to the profile string if the second argument is 'true'
if [ $2 == "true" ]; then
  PROFILE="${PROFILE}runMediumTests,"
fi
# Add 'runLongTests' to the profile string if the third argument is 'true'
if [ $3 == "true" ]; then
  PROFILE="${PROFILE}runLongTests"
fi

# Remove trailing comma if present
if [[ "${PROFILE}" == *"," ]]; then
  PROFILE="${PROFILE%?}"
fi

# If no profile is active, default to 'runShortTests'
if [ -z "${PROFILE}" ]; then
  PROFILE="runShortTests"
fi

# Output the resulting profile string
echo "${PROFILE}"
exit 0
