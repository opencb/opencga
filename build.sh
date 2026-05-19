#!/bin/bash

set -e
set -o pipefail
set -o nounset

###########################################
####### Declare all functions first #######
###########################################

# Variables to store start time
START_TIME=$(date +%s)
START_DATE=$(date +"%Y-%m-%d %H:%M:%S")
TIME_SUMMARY=""
# Log file path
LOG_FILE="build.log"
# Check and delete the file if it exists
if [ -f "$LOG_FILE" ]; then
    rm "$LOG_FILE"
fi
touch "$LOG_FILE"

MVN_OPTS="${MVN_OPTS:-}"
MVN_OPTS="$MVN_OPTS --no-transfer-progress"

# Function to log messages
function log() {
    if [[ "$#" -gt 0 ]]; then
      echo "$@" | log
      return
    fi
    if [[ -n "$LOG_FILE" ]]; then
      tee -a $LOG_FILE
    else
      cat >&2
    fi
}

# Function to log error messages
function error() {
  log "=========================="
  log "[ERROR] - " "$@"
  log "=========================="
}

# Function to calculate the branch for dependencies based on opencga's own current branch
function calculate_branch() {
  local REPO_VERSION="$1"
  local EXISTS=""
  if [[ ! "$REPO_VERSION" =~ SNAPSHOT$ ]]; then
    echo "v${REPO_VERSION}"
    return
  fi

  if [[ -n $TASK_REFERENCE ]]; then
    local EXISTS=$(git ls-remote origin "$TASK_REFERENCE")
  fi
  if [[ -n $EXISTS ]]; then
    echo "$TASK_REFERENCE"
  else
    local TMP_DIR=$(pwd)
    cd "$OPENCGA_HOME_DIR"
    local OPENCGA_BRANCH="$(git branch --show-current)"
    cd "$TMP_DIR"
    if [[ "$OPENCGA_BRANCH" == "TASK"* || "$OPENCGA_BRANCH" == "release"* ]]; then
      local VERSION=$(echo "$REPO_VERSION" | cut -d "-" -f 1)
      local MAJOR=$(echo "$VERSION" | cut -d "." -f 1)
      local MINOR=$(echo "$VERSION" | cut -d "." -f 2)
      local PATCH=$(echo "$VERSION" | cut -d "." -f 3)
      if [ $PATCH -gt 0 ]; then
        echo "release-$MAJOR.$MINOR.x"
      elif [ $MINOR -eq 0 ]; then
        echo "develop"
      else
        echo "release-$MAJOR.x.x"
      fi
    else
      echo "$OPENCGA_BRANCH"
    fi
  fi
}

# Function to manage dependencies: clone, checkout matching branch and build/test
function manage_dependency() {
  local REPO=$1
  local REPO_VERSION=$2
  local REPO_ORG="opencb"
  if [[ "$REPO" == *"/"* ]]; then
    REPO_ORG=$(echo "$REPO" | cut -d "/" -f 1)
    REPO=$(echo "$REPO" | cut -d "/" -f 2)
  fi

  if [ -z "$REPO" ] || [ -z "$REPO_ORG" ] || [ -z "$REPO_VERSION" ]; then
    log "ERROR: REPO, REPO_ORG and REPO_VERSION must be provided"
    log "REPO: '$REPO'"
    log "REPO_ORG: '$REPO_ORG'"
    log "REPO_VERSION: '$REPO_VERSION'"
    exit 1
  fi

  local TEMP_DIR="$(mktemp -d --tmpdir="${TMP_DIR_HOME:?}" --suffix="opencga-$(date +%Y%m%d%H%M%S)-$REPO")"
  cd "$TEMP_DIR" || exit 2

  CLONE_URL="https://github.com/${REPO_ORG}/"${REPO}".git"
  rm -rf "${REPO:?}"
  echo "Cloning repository $REPO from $CLONE_URL"
  git clone "$CLONE_URL"

  if [ -d "./$REPO" ]; then
      cd "$REPO" || exit 2
      local BRANCH_NAME="$(calculate_branch "$REPO_VERSION")"
  else
      log "The $REPO cloning process has failed!"
      exit 1
  fi
  git checkout "$BRANCH_NAME"
  local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout $MVN_OPTS)
  if [ "$VERSION" == "$REPO_VERSION" ]; then
    log "Version of $REPO to download correct $VERSION should be in $BRANCH_NAME"
    log_summary "Version of $REPO to download correct $VERSION should be in $BRANCH_NAME"
    log_version_summary "$REPO,$VERSION,$BRANCH_NAME"
    if [ "$COMMAND" == "build" ]; then
      if [ "$CLEAN" == "true" ]; then
        mvn_step "${REPO}-clean" clean $MVN_OPTS
        mvn_step "${REPO}-${COMMAND}" install -B -T 2 -DskipTests $MVN_OPTS
      else
        mvn_step "${REPO}-${COMMAND}" clean install -B -T 2 -DskipTests $MVN_OPTS
      fi

      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND $REPO with $REPO_VERSION in $BRANCH_NAME FAILED!!!!!"
      else
        log_summary "$COMMAND $REPO with $REPO_VERSION branch $BRANCH_NAME Successful!!!"
      fi
    elif [ "$COMMAND" == "test" ]; then
      log "Testing $REPO branch $BRANCH_NAME."
      local pwd=$(pwd)
      echo "${pwd} $REPO" >> "$OPENCGA_HOME_DIR/reports/collected_reports.txt"
      mvn_step "${REPO}-${COMMAND}" install -B surefire-report:report ${FAIL_NEVER} -Dcheckstyle.skip $MVN_OPTS

      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND $REPO with $VERSION in $BRANCH_NAME FAILED!!!!!"
      else
        log_summary "$COMMAND $REPO with $VERSION branch $BRANCH_NAME Successful!!!"
      fi
    fi
  else
      log "Version mismatch for $REPO: found $VERSION but expected $REPO_VERSION in branch $BRANCH_NAME"
  fi
  cd "$OPENCGA_HOME_DIR" || exit 2
}

# Normalize short/medium/long aliases to Maven profile names
function normalize_test_level() {
  local result=""
  IFS=',' read -ra levels <<< "$1"
  for level in "${levels[@]}"; do
    case "$level" in
      short)  level="runShortTests" ;;
      medium) level="runMediumTests" ;;
      long)   level="runLongTests" ;;
    esac
    result="${result:+$result,}$level"
  done
  echo "$result"
}

# Function to validate test tags
function validate_tags() {
  IFS=',' read -ra my_array <<< "$1"
  for i in "${my_array[@]}"; do
    if [ "$i" != "runShortTests" ] && [ "$i" != "runMediumTests" ] && [ "$i" != "runLongTests" ]; then
      echo "The test level must be any combination of these values short|medium|long (or runShortTests|runMediumTests|runLongTests) separated by commas without spaces"
      exit 1
    fi
  done
}

# Function to print main usage of the script
function print_usage() {
  echo ""
  echo "Build and test opencga."
  echo ""
  echo "Usage:   $(basename $0) [options]"
  echo ""
  echo "  Options:"
  echo "     -H     --storage-hadoop      STRING         Hadoop flavour hbase2.0, hdp3.1, hdi5.1, emr6.1, emr6.13, emr7.5 [hdi5.1]"
  echo "     -T     --task                STRING         Task ID used for building and testing dependencies, serves as checkout reference"
  echo "     -t     --test-level          STRING         Run tests at the given level: short, medium, long (comma-separated for multiple). Omit to skip tests."
  echo "     -f     --test-fail-never     FLAG           Execute all tests even if some fail"
  echo "     -b     --prepare-branches    FLAG           Download and compile dependency branches before build"
  echo "     -S     --prepare-hadoop      FLAG           Download and compile opencga-hadoop-thirdparty dependency (use with --prepare-branches)"
  echo "     -c     --clean               FLAG           Run mvn clean as a separate step before mvn install (default: mvn clean install in one step)"
  echo "     -d     --docker              FLAG           Publish OpenCGA docker images to DockerHub"
  echo "     -p     --docker-tag          STRING         Tag for OpenCGA docker images"
  echo "     -i     --docker-images       STRING         Additional docker images to publish alongside base: workflow, python-notebook, ext-tools, r-builder"
  echo "     -A     --activate-profiles   STRING         Comma-delimited list of additional Maven profiles to activate"
  echo "     -P     --python-client       FLAG           Also build the OpenCGA Python client"
  echo "     -W     --javascript-client   FLAG           Also build the OpenCGA JavaScript client"
  echo "     -R     --R-client            FLAG           Also build the OpenCGA R client"
  echo "     -v     --verbose             FLAG           Print verbose logs"
  echo "     -h     --help                FLAG           Print this help and exit"
  echo ""
}

# Function to validate input parameters
function validate() {
  validate_tags "$TEST_TAG"

  if [[ -n "$STORAGE_HADOOP_DEPS" ]]; then
    mvn enforcer:enforce -q \
        --file "${OPENCGA_HOME_DIR}/pom.xml" \
        -Denforcer.rules=requireProfileIdsExist \
        -P"$STORAGE_HADOOP_DEPS" \
        -pl :opencga $MVN_OPTS || (error "OpenCGA storage hadoop '$STORAGE_HADOOP_DEPS' not found!" && exit 1)
  fi

  local CURRENT_BRANCH="$(git branch --show-current)"
  log_summary "opencga CURRENT_BRANCH $CURRENT_BRANCH"
  log_summary "opencga TASK_REFERENCE $TASK_REFERENCE"

  if [[ "$CURRENT_BRANCH" == "TASK"* ]]; then
    if [[ -n $TASK_REFERENCE ]]; then
      if [[ "$CURRENT_BRANCH" != "$TASK_REFERENCE" ]]; then
        log "If the opencga branch is a TASK branch, the name must be the same as the reference branch."
        exit 1
      fi
    fi
  fi
}

# Function to download and compile java-common-libs and biodata dependencies
function prepare_branches() {
  export TMP_DIR_HOME="${OPENCGA_HOME_DIR:?}/tmp/dependency-checkouts/"
  mkdir -p "$TMP_DIR_HOME"
  rm -rf "${TMP_DIR_HOME:?}"/*

  if [ "$PREPARE_BRANCHES" == "true" ]; then
    if [ "${PREPARE_BRANCHES_HADOOP:-false}" == "true" ]; then
      OPENCGA_HADOOP_THIRD_PARTY_VERSION="$(mvn help:evaluate --file "${OPENCGA_HOME_DIR}/pom.xml" -Dexpression=opencga.hadoop.thirdparty.version -q -DforceStdout $MVN_OPTS)"
      echo "Downloading and compiling opencga-hadoop-thirdparty $OPENCGA_HADOOP_THIRD_PARTY_VERSION"
      cd "${OPENCGA_HOME_DIR}" || exit 2
      chmod +x .github/workflows/scripts/prepare_hadoop.sh
      exec_step "opencga-hadoop-thirdparty-build" \
            .github/workflows/scripts/prepare_hadoop.sh \
            --hadoop-flavour "$STORAGE_HADOOP_DEPS" \
            --hadoop-thirdparty-version "$OPENCGA_HADOOP_THIRD_PARTY_VERSION"
      cd - || exit 2
    fi

    JCL_DEPENDENCY_VERSION="$(mvn help:evaluate --file "${OPENCGA_HOME_DIR}/pom.xml" -Dexpression=java-common-libs.version -q -DforceStdout $MVN_OPTS)"
    echo "Downloading and compiling java-common-libs $JCL_DEPENDENCY_VERSION"
    manage_dependency "java-common-libs" "$JCL_DEPENDENCY_VERSION"

    BIODATA_DEPENDENCY_VERSION="$(mvn help:evaluate --file "${OPENCGA_HOME_DIR}/pom.xml" -Dexpression=biodata.version -q -DforceStdout $MVN_OPTS)"
    echo "Downloading and compiling biodata $BIODATA_DEPENDENCY_VERSION"
    manage_dependency "biodata" "$BIODATA_DEPENDENCY_VERSION"
  else
    log_summary "Skipped prepare branches"
  fi
}

## Execute maven commands, filter output and save compressed log
function mvn_step() {
  STEP_NAME="$1"
  shift
  export GROUP_NAME="Maven Step"
  exec_step "$STEP_NAME" mvn "$@"
}

## Execute commands, filter output and save compressed log
function exec_step() {
  STEP_NAME="$1"
  shift
  if [ -z "${GROUP_NAME:-}" ]; then
    echo ""
  else
    echo "::group::${GROUP_NAME} - ${STEP_NAME}"
  fi
  echo "=== $STEP_NAME ==="
  echo "Executing: $*"
  STEP_LOG_FILE="$OPENCGA_HOME_DIR/reports/${STEP_NAME}.log.gz"
  if [ -f "$STEP_LOG_FILE" ]; then
      rm "$STEP_LOG_FILE"
  fi
  echo "STEP_LOG_FILE = $STEP_LOG_FILE"
  local STEP_START_TIME=$(date +%s)
  "$@" |& tee >(gzip > "$STEP_LOG_FILE" ) |& { grep -a -P '^\[[^\]]*(INFO|WARNING|ERROR)|::group::|::endgroup::|^\+' --colour=never --line-buffered || true; }
  local STATUS=${PIPESTATUS[0]}
  local STEP_END_TIME=$(date +%s)
  local DURATION=$((STEP_END_TIME - STEP_START_TIME))
  if [ -z "${GROUP_NAME:-}" ]; then
    echo ""
  else
    echo "::endgroup::"
  fi
  echo "== $STEP_NAME Completed =="
  echo " - Duration: $(date -u -d @"$DURATION" +%H:%M:%S) ($DURATION seconds)"
  return $STATUS
}

# Function to build or test opencga
function build_opencga() {
  cd "$OPENCGA_HOME_DIR" || exit 2

  local HADOOP_PROFILE=""
  if [[ -n "$STORAGE_HADOOP_DEPS" ]]; then
    HADOOP_PROFILE="-P$STORAGE_HADOOP_DEPS"
  fi

  if [ "$COMMAND" == "build" ] ; then
      log "Compiling opencga... $(pwd)"
      if [ "$CLEAN" == "true" ]; then
        mvn_step "opencga-clean" clean $HADOOP_PROFILE $MVN_OPTS
        mvn_step "opencga-build" install -DskipTests $HADOOP_PROFILE -T 2 $MVN_OPTS
      else
        mvn_step "opencga-build" clean install -DskipTests $HADOOP_PROFILE -T 2 $MVN_OPTS
      fi
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND opencga build FAILED!!!!!"
        print_log_summary
        exit 1
      else
        local BRANCH="$(git branch --show-current)"
        local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout $MVN_OPTS)
        log_version_summary "opencga,$VERSION,$BRANCH"
        log_summary "$COMMAND opencga build Success!"
      fi
  elif [ "$COMMAND" == "test" ] ; then
      local pwd=$(pwd -P)
      echo "${pwd} opencga" >> "$OPENCGA_HOME_DIR/reports/collected_reports.txt"
      local TEST_PROFILES="${TEST_TAG}"
      if [[ -n "$STORAGE_HADOOP_DEPS" ]]; then
        TEST_PROFILES="$STORAGE_HADOOP_DEPS,$TEST_TAG"
      fi
      if [ "$CLEAN" == "true" ]; then
        mvn_step "opencga-clean" clean -P "${TEST_PROFILES}" $MVN_OPTS
        mvn_step "opencga-test" install -B surefire-report:report ${FAIL_NEVER} -P "${TEST_PROFILES}" -Dcheckstyle.skip $MVN_OPTS
      else
        mvn_step "opencga-test" clean install -B surefire-report:report ${FAIL_NEVER} -P "${TEST_PROFILES}" -Dcheckstyle.skip $MVN_OPTS
      fi
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND opencga test FAILED!!!!!"
        print_log_summary
        exit 1
      else
        local BRANCH="$(git branch --show-current)"
        local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout $MVN_OPTS)
        log_version_summary "opencga,$VERSION,$BRANCH"
        log_summary "$COMMAND opencga test Success!"
      fi
  fi

  if [ "$BUILD_CLIENTS" == "true" ]; then
    cd "$OPENCGA_HOME_DIR" || exit 2
    SKIP_CLIENTS=""
    if [ "$PYTHON_CLIENT" == "false" ]; then
      SKIP_CLIENTS="--skip-python"
    fi
    if [ "$R_CLIENT" == "false" ]; then
      SKIP_CLIENTS="$SKIP_CLIENTS --skip-r"
    fi
    if [ "$JAVASCRIPT_CLIENT" == "false" ]; then
      SKIP_CLIENTS="$SKIP_CLIENTS --skip-javascript"
    fi
    echo "Working directory is: $(pwd)"
    #Do not put quotes in the following command or it will not work
    ./client-builder.sh --skip-build-opencga $SKIP_CLIENTS
  fi

  BYTES_PRE=$(du -s --block-size=1 . | cut -f1)
  find . -type d -name target | while read -r target_dir; do
    find "$target_dir" -mindepth 1 -maxdepth 1 ! -name "site" ! -name "surefire-reports" -exec rm -rf {} +
  done
  BYTES_POST=$(du -s --block-size=1 . | cut -f1)
  FREED_SPACE=$(numfmt --to=iec-i --suffix=B "$((BYTES_PRE - BYTES_POST))")
  echo "Freed space after cleaning target folders: $FREED_SPACE"
}

# Function to publish OpenCGA docker images to DockerHub (org: opencb)
function publish_dockers() {
  if [ "$DOCKER" == "true" ]; then
    cd "$OPENCGA_HOME_DIR" || exit 2
    if [[ -n "$DOCKER_TAG" ]]; then
      TAG="${DOCKER_TAG}"
    elif [[ -n $TASK_REFERENCE ]]; then
      TAG=$TASK_REFERENCE
    else
      TAG="$(mvn help:evaluate --file "${OPENCGA_HOME_DIR}/pom.xml" -Dexpression=project.version -q -DforceStdout $MVN_OPTS)"
    fi
    python3 ./opencga-app/app/cloud/docker/docker-build.py push --org zettagenomics --images base --tag "$TAG"
    if [[ "$?" -ne 0 ]] ; then
      log_summary "[ERROR] OPENCGA DOCKER UPLOAD FAILED (base image)!!!!!"
    else
      log_summary "OpenCGA base docker image uploaded correctly with tag $TAG"
    fi
    if [[ -n "$DOCKER_IMAGES" ]]; then
      python3 ./opencga-app/app/cloud/docker/docker-build.py push --org opencb --images "$DOCKER_IMAGES" --tag "$TAG"
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] OPENCGA DOCKER UPLOAD FAILED (extra images: $DOCKER_IMAGES)!!!!!"
      else
        log_summary "OpenCGA extra docker images ($DOCKER_IMAGES) uploaded correctly with tag $TAG"
      fi
    fi
  fi
}


## FUNCTIONS TO MANAGE LOGS AND PRINTS ##


function log_summary() {
  if [ -n "$LOG_SUMMARY" ]; then
    LOG_SUMMARY="$LOG_SUMMARY""\n"
  fi
  LOG_SUMMARY="$LOG_SUMMARY""INFO: $(date +"%Y-%m-%d %H:%M:%S")  $@"
}

function log_version_summary() {
  if [ -n "$VERSION_SUMMARY" ]; then
    VERSION_SUMMARY="$VERSION_SUMMARY""\n"
  fi
  VERSION_SUMMARY="$VERSION_SUMMARY""$@"
}

function log_time_summary() {
  if [ -n "$TIME_SUMMARY" ]; then
    TIME_SUMMARY="$TIME_SUMMARY""\n"
  fi
  TIME_SUMMARY="$TIME_SUMMARY""$@"
}

function log_param_summary() {
  if [ -n "$PARAM_SUMMARY" ]; then
    PARAM_SUMMARY="$PARAM_SUMMARY""\n"
  fi
  PARAM_SUMMARY="$PARAM_SUMMARY""$@"
}

function log_execution_time() {
    local END_TIME=$(date +%s)
    local END_DATE=$(date +"%Y-%m-%d %H:%M:%S")
    local DURATION=$((END_TIME - START_TIME))
    local SECONDS=$((DURATION % 60))
    local MINUTES=$((DURATION / 60 % 60))
    local HOURS=$((DURATION / 3600))
    cd "$OPENCGA_HOME_DIR" || exit 2
    local BRANCH="$(git branch --show-current)"
    local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout $MVN_OPTS)

    log_time_summary ""
    log_time_summary "==========================="
    log_time_summary ""
    log_time_summary "End OpenCGA-$VERSION $COMMAND for branch $BRANCH"
    log_time_summary " "
    log_time_summary "Script execution started at: $START_DATE"
    log_time_summary "Script execution finished at: $END_DATE"
    log_time_summary "Total execution time: ${HOURS}h ${MINUTES}m ${SECONDS}s"
    log_time_summary ""
    log_time_summary "==========================="
}

function print_time_summary() {
    echo ""
    echo "==========================" >> "$LOG_FILE"
    echo -e "$TIME_SUMMARY" >> "$LOG_FILE"
    echo "==========================" >> "$LOG_FILE"
}

function print_log_summary() {
    echo ""
    echo "==========================" >> "$LOG_FILE"
    echo -e "$LOG_SUMMARY" >> "$LOG_FILE"
    echo "==========================" >> "$LOG_FILE"
}

function yes_no() {
  local value=$1
  if [[ -n "$value" ]]; then
    echo "YES"
  else
    echo "NO"
  fi
}

function log_initial_state() {
    log_param_summary "COMMAND,$COMMAND"
    log_param_summary "OPENCGA_HOME_DIR,$OPENCGA_HOME_DIR"
    log_param_summary "STORAGE_HADOOP_DEPS,$STORAGE_HADOOP_DEPS"
    log_param_summary "TEST_TAG,$TEST_TAG"
    log_param_summary "LOG_FILE,$LOG_FILE"
    log_param_summary "TASK_REFERENCE,$TASK_REFERENCE"
    log_param_summary "FAIL_NEVER,$(yes_no "$FAIL_NEVER")"
    log_param_summary "PREPARE_BRANCHES,$(yes_no "$PREPARE_BRANCHES")"
    log_param_summary "PREPARE_BRANCHES_HADOOP,$(yes_no "${PREPARE_BRANCHES_HADOOP}")"
    log_param_summary "DEBUG,$(yes_no "$DEBUG")"
    log_param_summary "CLEAN,$(yes_no "$CLEAN")"
    log_param_summary "DOCKER,$(yes_no "$DOCKER")"
    log_param_summary "DOCKER_IMAGES,base${DOCKER_IMAGES:+,$DOCKER_IMAGES}"
}

function print_version_summary() {
    echo ""  >> "$LOG_FILE"
    printf "%-25s %-20s %-20s\n" "Repository" "Version" "Branch" >> "$LOG_FILE"
    printf "%-25s %-20s %-20s\n" "---------" "-------" "------" >> "$LOG_FILE"
    echo -e "$VERSION_SUMMARY" | while IFS=',' read -r repository version branch; do
        printf "%-25s %-20s %-20s\n" "$repository" "$version" "$branch" >> "$LOG_FILE"
    done
}

function print_param_summary() {
    echo ""  >> "$LOG_FILE"
    printf "%-25s %-20s \n" "Parameter" "Value" >> "$LOG_FILE"
    printf "%-25s %-20s \n" "---------" "-------" >> "$LOG_FILE"
    echo -e "$PARAM_SUMMARY" | while IFS=',' read -r param value; do
        printf "%-25s %-20s \n" "$param" "$value" >> "$LOG_FILE"
    done
}

function generate_param_table() {
    local table_html="<table style=\"width:100%; border-collapse: collapse;\">
        <thead style=\"background-color: #aaa; color: white;\">
            <tr>
                <th style=\"padding: 8px; border: 1px solid #ddd;\">PARAMETER</th>
                <th style=\"padding: 8px; border: 1px solid #ddd;\">VALUE</th>
            </tr>
        </thead>
        <tbody>"

    table_html=$table_html$(echo -e "$PARAM_SUMMARY" | while IFS=',' read -r param value; do
        echo "<tr>"
        echo "<td style=\"padding: 8px; border: 1px solid #ddd;\">$param</td>"
        echo "<td style=\"padding: 8px; border: 1px solid #ddd;\">$value</td>"
        echo "</tr>"
    done)

    table_html="$table_html</tbody></table>"
    echo "$table_html"
}

function generate_version_table() {
    local table_html="<table style=\"width:100%; border-collapse: collapse;\">
        <thead style=\"background-color: #aaa; color: white;\">
            <tr>
                <th style=\"padding: 8px; border: 1px solid #ddd;\">Repository</th>
                <th style=\"padding: 8px; border: 1px solid #ddd;\">Version</th>
                <th style=\"padding: 8px; border: 1px solid #ddd;\">Branch</th>
            </tr>
        </thead>
        <tbody>"

    table_html=$table_html$(echo -e "$VERSION_SUMMARY" | while IFS=',' read -r repository version branch; do
        echo "<tr>"
        echo "<td style=\"padding: 8px; border: 1px solid #ddd;\">$repository</td>"
        echo "<td style=\"padding: 8px; border: 1px solid #ddd;\">$version</td>"
        echo "<td style=\"padding: 8px; border: 1px solid #ddd;\">$branch</td>"
        echo "</tr>"
    done)

    table_html="$table_html</tbody></table>"
    echo "$table_html"
}

function generate_html_report() {
    local param_summary=$(generate_param_table)
    local version_summary=$(generate_version_table)
    local execution_time=$(echo -e "$TIME_SUMMARY")
    local template_file="$OPENCGA_HOME_DIR/reports/build.html.template"
    local output_file="$OPENCGA_HOME_DIR/reports/test/summary.html"

    local template_content=$(<"$template_file")

    template_content="${template_content//#PARAM_SUMMARY/$param_summary}"
    template_content="${template_content//#VERSION_SUMMARY/$version_summary}"
    template_content="${template_content//#EXECUTION_TIME/$execution_time}"

    mkdir -p "$(dirname "$output_file")"
    echo "$template_content" > "$output_file"
}

function print_log() {
  print_param_summary
  print_log_summary
  print_version_summary
  log_execution_time
  print_time_summary
  generate_html_report
  cat "$LOG_FILE"
}

###################################
####### Script starts here  #######
###################################
## 1. Initialize variables and set default values

LOG_SUMMARY=""
STORAGE_HADOOP_DEPS="hdi5.1"
TEST_TAG="runShortTests"
FAIL_NEVER=""
DOCKER_TAG=""
DOCKER_IMAGES=""
CLEAN=""
PREPARE_BRANCHES=""
PREPARE_BRANCHES_HADOOP=""
DEBUG=""
TASK_REFERENCE=""
DOCKER=""
COMMAND="build"
VERSION_SUMMARY=""
PARAM_SUMMARY=""
BUILD_CLIENTS="false"
PYTHON_CLIENT="false"
R_CLIENT="false"
JAVASCRIPT_CLIENT="false"
###################################

## 2. Read and parse CLI options
while [[ $# -gt 0 ]]; do
  key="$1"
  value="${2:-}"
  case $key in
  -h | --help)
    print_usage
    exit 0
    ;;
  -v | --verbose)
    set -x
    shift
    ;;
  -H | --storage-hadoop)
    STORAGE_HADOOP_DEPS="$value"
    shift
    shift
    ;;
  -P | --python-client)
    PYTHON_CLIENT="true"
    BUILD_CLIENTS="true"
    shift
    ;;
  -R | --R-client)
    R_CLIENT="true"
    BUILD_CLIENTS="true"
    shift
    ;;
  -W | --javascript-client)
    JAVASCRIPT_CLIENT="true"
    BUILD_CLIENTS="true"
    shift
    ;;
  -d | --docker)
    DOCKER="true"
    shift
    ;;
  -p | --docker-tag)
    DOCKER_TAG="$value"
    shift
    shift
    ;;
  -i | --docker-images)
    DOCKER_IMAGES="$value"
    shift
    shift
    ;;
  -c | --clean)
    CLEAN="true"
    shift
    ;;
  -t | --test-level)
    if [ -z "$value" ]; then
      echo "Test level is empty. The test level must be any combination of short|medium|long separated by commas without spaces"
      exit 1
    fi
    TEST_TAG="$(normalize_test_level "$value")"
    COMMAND="test"
    shift
    shift
    ;;
  -T | --task)
    TASK_REFERENCE="$value"
    shift
    shift
    ;;
  -A | --activate-profiles)
    MVN_OPTS="$MVN_OPTS -P$value"
    shift
    shift
    ;;
  -b | --prepare-branches)
    PREPARE_BRANCHES="true"
    shift
    ;;
  -S | --prepare-hadoop)
    PREPARE_BRANCHES="true"
    PREPARE_BRANCHES_HADOOP="true"
    shift
    ;;
  -f | --test-fail-never)
    FAIL_NEVER="--fail-never -Dmaven.test.failure.ignore=true -Dsurefire.testFailureIgnore=true"
    COMMAND="test"
    shift
    ;;
  --debug)
    DEBUG="true"
    set -x
    shift
    ;;
  *)
    echo "Unknown option $key"
    print_usage
    exit 1
    ;;
  esac
done

if [[ $MVN_OPTS != *"-Dopencga.war.name="* ]]; then
    MVN_OPTS="$MVN_OPTS -Dopencga.war.name=opencga"
fi

## 3. Set OPENCGA_HOME_DIR to the script's own directory
cd "$(dirname "$0")" || exit 2
OPENCGA_HOME_DIR=$PWD

# Prepare reports directory
mkdir -p "$OPENCGA_HOME_DIR/reports"
if [ -f "$OPENCGA_HOME_DIR/reports/collected_reports.txt" ]; then
    rm "$OPENCGA_HOME_DIR/reports/collected_reports.txt"
fi
touch "$OPENCGA_HOME_DIR/reports/collected_reports.txt"

## 4. Print debug info if requested
if [ "$DEBUG" == "true" ]; then
  log_summary "OPENCGA_HOME_DIR $OPENCGA_HOME_DIR"
  log_summary "STORAGE_HADOOP_DEPS $STORAGE_HADOOP_DEPS"
  log_summary "COMMAND $COMMAND"
  log_summary "PREPARE_BRANCHES $PREPARE_BRANCHES"
fi

## 5. Sequential calls to orchestrate the build

log_initial_state
validate
prepare_branches
build_opencga
publish_dockers
print_log
