#!/bin/bash
# get_matrix.sh
# This script builds the test profile, module, and hadoop matrices for the test job in the OpenCGA CI workflow.
# It is intended to be called from the GitHub Actions workflow.
#
# Usage:
#   ./get_matrix.sh <short_tests> <medium_tests> <long_tests> <hadoop> <module>
#
# Arguments:
#   short_tests   - true/false, whether to include short tests
#   medium_tests  - true/false, whether to include medium tests
#   long_tests    - true/false, whether to include long tests
#   hadoop        - Hadoop profile (e.g., all, hdi5.1, emr6.1, ...)
#   module        - OpenCGA module (e.g., all, opencga-core, ...)
#
# Outputs:
#   Sets the following outputs for GitHub Actions:
#     profiles, modules, hadoop

set -e

SHORT_TESTS="$1"
MEDIUM_TESTS="$2"
LONG_TESTS="$3"
HADOOP_INPUT="$4"
MODULE_INPUT="$5"

# Call the original get_profiles.sh script to get the profiles string
if [ -f "$(dirname "$0")/get_profiles.sh" ]; then
    chmod +x "$(dirname "$0")/get_profiles.sh"
    PROFILES=$("$(dirname "$0")/get_profiles.sh" "$SHORT_TESTS" "$MEDIUM_TESTS" "$LONG_TESTS")
else
    echo "get_profiles.sh not found!" >&2
    exit 1
fi

MODULES='["'"$MODULE_INPUT"'"]'
HADOOP='["'"$HADOOP_INPUT"'"]'

if [ "$HADOOP_INPUT" == "all" ]; then
    HADOOP='["hdi5.1", "emr6.1", "emr6.13", "emr7.5", "hbase2.0"]'
    # When testing all Hadoop profiles, split matrix by profile
    IFS=',' read -r -a profile_array <<< "$PROFILES"
    PROFILES_JSON=""
    for profile in "${profile_array[@]}"; do
        if [ "$PROFILES_JSON" == "" ]; then
            PROFILES_JSON="\"$profile\""
        else
            PROFILES_JSON="$PROFILES_JSON, \"$profile\""
        fi
    done
    PROFILES="[$PROFILES_JSON]"
else
    PROFILES='["'"$PROFILES"'"]'
    if [ "$MODULE_INPUT" == "all" ]; then
        # Only execute modules with matrix strategy if testing a single Hadoop profile
        MODULES='["opencga-analysis", "opencga-app", "opencga-catalog", "opencga-client", "opencga-core", "opencga-master", "opencga-server", "opencga-storage", "opencga-test"]'
    fi
fi

# Set outputs for GitHub Actions
{
    echo "profiles=$PROFILES"
    echo "modules=$MODULES"
    echo "hadoop=$HADOOP"
} > matrix_outputs.txt

# Also print for debugging
cat matrix_outputs.txt

