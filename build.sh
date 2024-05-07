#!/bin/bash

set -e
set -o pipefail
set -o nounset

###########################################
####### Declare all functions first #######
###########################################

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

# Function to calculate the branch for dependencies
function calculate_branch() {
  local EXISTS=""
  if [[ -n $TASK_REFERENCE ]]; then
    local EXISTS=$(git ls-remote origin "$TASK_REFERENCE")
  fi
  if [[ -n $EXISTS ]]; then
    echo $TASK_REFERENCE
  else
    local TMP_DIR=$(pwd)
    cd "$OPENCGA_ENTERPRISE_HOME_DIR"
    ## This is opencga-enterprise
    local CURRENT_BRANCH="$(git branch --show-current)"
    cd "$TMP_DIR"
    ## If opencga-enterprise branch name is main, develop then we return the same name.
    ## Otherwise, we calculate the dependency branch from the dependency version.
    if [[ "$CURRENT_BRANCH" == "TASK"* ]]; then
      local VERSION=$(echo "$1" | cut -d "-" -f 1)
      local MAJOR=$(echo "$VERSION" | cut -d "." -f 1)
      local MINOR=$(echo "$VERSION" | cut -d "." -f 2)
      local PATCH=$(echo "$VERSION" | cut -d "." -f 3)
      local HOTFIX=$(echo "$VERSION" | cut -d "." -f 4)
      if [[ "$PATCH" == "0" ]]; then
        echo "develop"
      elif [ -z "$HOTFIX" ]; then
        echo "release-$MAJOR.$MINOR.x"
      else
        echo "release-$MAJOR.$MINOR.$PATCH.x"
      fi
    elif [[ "$CURRENT_BRANCH" == "release"* ]]; then
      local VERSION=$(echo "$1" | cut -d "-" -f 1)
      local MAJOR=$(echo "$VERSION" | cut -d "." -f 1)
      local MINOR=$(echo "$VERSION" | cut -d "." -f 2)
      local PATCH=$(echo "$VERSION" | cut -d "." -f 3)
      local HOTFIX=$(echo "$VERSION" | cut -d "." -f 4)
      if [ -z "$HOTFIX" ]; then
        echo "release-$MAJOR.$MINOR.x"
      else
        echo "release-$MAJOR.$MINOR.$PATCH.x"
      fi
    else
      echo "$CURRENT_BRANCH"
    fi
  fi
}

# Function to manage dependencies
function manage_dependency() {
  local REPO=$1
  local REPO_VERSION=$2
  local TEMP_DIR="$(mktemp -d "--suffix=opencga-enterprise-$(date +%Y%m%d%H%M%S)-$REPO")"
  cd "$TEMP_DIR" || exit 2
  git clone https://github.com/opencb/"$REPO".git
  if [ -d "./$REPO" ]; then
      cd "$REPO" || exit 2
      local BRANCH_NAME="$(calculate_branch "$REPO_VERSION")"
  else
   if [[ "$BRANCH_NAME" != "TASK"*  ]]; then
      log "The $REPO branch $BRANCH_NAME cloning process has failed!"
      exit 1
   fi
  fi
  log_summary "Version of $REPO to download correct $REPO_VERSION should be in $BRANCH_NAME"
  git checkout "$BRANCH_NAME"
  if [ "$COMMAND" == "test" ];then
    if [ "$SKIP_TESTS" == "true" ]; then
      log "Skipping test compiling $REPO branch $BRANCH_NAME."
      mvn clean install -T 2 -DskipTests || (error "The $REPO branch $BRANCH_NAME compilation process has failed!"; exit 1)
      log "$REPO Compilation Successful!!!"
    else
      log "Testing $REPO branch $BRANCH_NAME."
      mvn install surefire-report:report ${FAIL_NEVER} -Dcheckstyle.skip || (error "Testing $REPO branch $BRANCH_NAME ERROR" && exit 1)
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $REPO with $REPO_VERSION in $BRANCH_NAME FAILED!!!!!"
      else
        log_summary "$REPO branch $BRANCH_NAME Test Successful!!!"
      fi
    fi
  fi
  cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
}

# Function to validate test tags
function validate_tags() {
  #Split input string
  IFS=',' read -ra my_array <<< "$1"
  #Check the split string
  for i in "${my_array[@]}"
  do
    if [ "$i" != "runShortTests" ] && [ "$i" != "runMediumTests" ] && [ "$i" != "runLongTests" ];then
      echo "The test level must be any combination of these values runShortTests|runMediumTests|runLongTests separated by commas without spaces"
      exit 1
    fi
  done
}

# Function to print main usage of the script
function print_usage() {
  echo ""
  echo "Run opencga-enterprise."
  echo ""
  echo "Usage:   $(basename $0) <command> [options]"
  echo ""
  echo "  Options:"
  echo "     -o     --opencga-home        STRING         Opencga project repo directory. By default, ./opencga-home"
  echo "     -H     --storage-hadoop      STRING         Hadoop flavour. hdp3.1, hdi5.1, emr6.1, emr6.13 ..."
  echo "     -T     --task                STRING         Task ID used for building and testing dependencies, this will serve as a reference for checkouts"
  echo "     -l     --test-level          STRING         Level of test we must to execute(runShortTests,runMediumTests,runLongTests)"
  echo "     -t     --test                FLAG           Execute the Xetabase tests by default only buid"
  echo "     -f     --test-fail-never     FLAG           The process executes all tests even if some fail."
  echo "     -b     --prepare-branches    FLAG           Previous to run, it will download and compile all branches of the dependencies."
  echo "     -s     --test-save-reports   FLAG           Save OpenCGA JUnit test reports to XetaBase Report server (Quality Team)."
  echo "     -d     --docker              FLAG           Publish dockers of OpenCGA and OpenCGA-enterprise."
  echo "     -v     --verbose             FLAG           Print verbose logs"
  echo "     -h     --help                FLAG           Print this help and exit"
  echo ""
}

# Function to validate input parameters
function validate() {
  validate_tags "$TEST_TAG"
  ## Validate opencga home dir
  if [ ! -d "$OPENCGA_HOME_DIR" ]; then
    log "ERROR OPENCGA HOME NOT FOUND!!!"
    log "You must create in the current directory a symbolic link to the directory where you have downloaded opencga and call it opencga-home"
    log "         ln -s /path/to/opencga $OPENCGA_HOME_DIR    "
    print_usage
    exit 1
  fi

  if [ "$COMMAND" == "test" ]; then
    ## Clean tests dir
    if [ -d "${TESTS_DIR:?}" ]; then
      rm -rf "${TESTS_DIR:?}"
    fi
    mkdir -p "$TESTS_DIR"
  fi

  OPENCGA_DEPENDENCY_VERSION="$(mvn help:evaluate --file "${OPENCGA_ENTERPRISE_HOME_DIR}/pom.xml" -Dexpression=opencga.version -q -DforceStdout)"
  OPENCGA_CURRENT_VERSION="$(mvn help:evaluate --file "${OPENCGA_HOME_DIR}/pom.xml" -Dexpression=project.version -q -DforceStdout)"

  log_summary "OPENCGA_DEPENDENCY_VERSION= $OPENCGA_DEPENDENCY_VERSION"
  log_summary "OPENCGA_CURRENT_VERSION= $OPENCGA_CURRENT_VERSION"

  ## Validate opencga version
  if [ "$OPENCGA_DEPENDENCY_VERSION" != "$OPENCGA_CURRENT_VERSION" ]; then
    cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
    OPENCGA_EXPECTED_BRANCH="$(.github/workflows/scripts/opencga_branch.sh)"
    OPENCGA_EXPECTED_TAG="$(.github/workflows/scripts/opencga_branch.sh true)"
    REF_TYPE=
    REF=
    if git -C "$OPENCGA_HOME_DIR" tag --list  | grep "^${OPENCGA_EXPECTED_TAG}$" >/dev/null ; then
      REF_TYPE="tag"
      REF="$OPENCGA_EXPECTED_TAG"
    else
      REF_TYPE="branch"
      REF="$OPENCGA_EXPECTED_BRANCH"
    fi
    log "OpenCGA version no match! You must checkout $REF_TYPE \"$REF\" to build from version \"$OPENCGA_DEPENDENCY_VERSION\" of opencga"
    log "Please, execute bellow command and retry:"
    log "  git -C \"$OPENCGA_HOME_DIR\" checkout $REF"
    exit 1
  fi

  ## Validate that if the current branch is a task, the reference branch must be the same
  local CURRENT_BRANCH="$(git branch --show-current)"
  log_summary "CURRENT_BRANCH $CURRENT_BRANCH"
  log_summary "TASK_REFERENCE $TASK_REFERENCE"

  if [[ "$CURRENT_BRANCH" == "TASK"* ]]; then
    if [[ -n $TASK_REFERENCE ]]; then
      if [[ "$CURRENT_BRANCH" != "$TASK_REFERENCE" ]]; then
      log "If the opencga-enterprise branch is a TASK branch, the name must be the same as the reference branch."
      exit 1
      fi
    fi
  fi

  ## Validate opencga-storage-hadoop
  mvn enforcer:enforce -q \
      --file "${OPENCGA_HOME_DIR}/pom.xml" \
      -Denforcer.rules=requireProfileIdsExist \
      -P"$STORAGE_HADOOP_DEPS" \
      -pl :opencga || (error "OpenCGA storage hadoop '$STORAGE_HADOOP_DEPS' not found!" && exit 1)
}

# Function to download and compile java-common-libs, cellbase and biodata dependencies
function prepare_branches() {
  ## Only if you pass the parameter: --prepare-branch
  if [ "$PREPARE_BRANCHES" == "true" ]; then
    JCL_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=java-common-libs.version -q -DforceStdout)"
    manage_dependency "java-common-libs" "$JCL_DEPENDENCY_VERSION"

    BIODATA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=biodata.version -q -DforceStdout)"
    manage_dependency "biodata" "$BIODATA_DEPENDENCY_VERSION"

    CELLBASE_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=cellbase.version -q -DforceStdout)"
    manage_dependency "cellbase" "$CELLBASE_DEPENDENCY_VERSION"
  fi
}

# Function to build or/and test the opencga
function build_opencga() {
  cd "$OPENCGA_HOME_DIR" || exit 2
  if [ "$COMMAND" == "build" ];then
      log "Compiling opencga... $(pwd)"
      mvn clean install -DskipTests -P"$STORAGE_HADOOP_DEPS" -T 2
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND opencga FAILED!!!!!"
      else
        log_summary "$COMMAND opencga Success!"
      fi
  elif [ "$COMMAND" == "test" ];then
      mvn clean install surefire-report:report ${FAIL_NEVER} -P "$STORAGE_HADOOP_DEPS","${TEST_TAG}" -Dcheckstyle.skip
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND opencga FAILED!!!!!"
      else
        log_summary "$COMMAND opencga Success!"
      fi
      cp "$OPENCGA_HOME_DIR"/opencga-*/target/surefire-reports/TEST*.xml "$TESTS_DIR"
  fi
}

# Function to build or/and test the opencga-enterprise
function build_opencga_enterprise() {
  ## Move to opencga-enterprise to build or test
  cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2

  if [ "$COMMAND" == "build" ];then
    mvn clean install -DskipTests -T 2 -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" \
    -Dopencga-hadoop-shaded.id="$STORAGE_HADOOP_DEPS" -Dopencga.war.name=opencga
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND opencga-enterprise FAILED!!!!!"
      else
        log_summary "$COMMAND opencga-enterprise Success!"
      fi
  elif [ "$COMMAND" == "test" ]; then
      mvn clean install -B verify surefire-report:report -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" \
      -Dopencga-hadoop-shaded.id="$STORAGE_HADOOP_DEPS" ${FAIL_NEVER}
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND opencga-enterprise FAILED!!!!!"
      else
        log_summary "$COMMAND opencga-enterprise Success!"
      fi
      cp "$OPENCGA_ENTERPRISE_HOME_DIR"/opencga-enterprise-*/target/surefire-reports/TEST*.xml "$TESTS_DIR"
  fi
}

# Function to upload the opencga and opencga-enterprise test reports to the Zettagenomics test report server
#It is do it with azure and AZ_COPY command
function publish_reports() {
  if [ "$PUBLISH" == "true" ];then
    ## Move to opencga-enterprise to build or test
    cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
    azcopy login --service-principal --application-id $AZCOPY_SPA_APPLICATION_ID
    if [[ -n $TASK_REFERENCE ]]; then
      BRANCH_FOLDER=$TASK_REFERENCE
    else
      BRANCH_FOLDER=$(git branch --show-current)
    fi
    VERSION_FOLDER="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"
    COMMIT=$(git show -q | grep commit | cut -d " " -f 2)
    azcopy copy "$TESTS_DIR" https://zettatest.blob.core.windows.net/test-data/opencga-enterprise/$VERSION_FOLDER/$BRANCH_FOLDER/$COMMIT --recursive
  fi
}

# Function to upload the docker of Oopencga-enterprise to https://hub.docker.com/repositories/zettagenomics
function publish_docker() {
  if [ "$DOCKER" == "true" ];then
    ## Move to opencga-enterprise to build or test
    cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
    if [[ -n $TASK_REFERENCE ]]; then
      TAG=$TASK_REFERENCE
    else
      TAG="$(mvn help:evaluate --file "${OPENCGA_ENTERPRISE_HOME_DIR}/pom.xml" -Dexpression=project.version -q -DforceStdout)"
    fi
    python3 ./build/cloud/docker/docker-build.py push --org zettagenomics --images enterprise --tag "$TAG"
  fi
}

# Function to add messages to the log summary
function log_summary() {
  if [ -n "$LOG_SUMMARY" ]; then
    LOG_SUMMARY="$LOG_SUMMARY""\n"
  fi
  LOG_SUMMARY="$LOG_SUMMARY""$@"
}

# Function to print all the log summary
function print_log_summary() {
  echo "=========================="
  echo -e "$LOG_SUMMARY"
  echo "=========================="
}

###################################
####### Script starts here  #######
###################################
## 1. Initialize variables and set default values

# Initialize the global variable LOG_SUMMARY
LOG_SUMMARY=""

OPENCGA_HOME_DIR="$PWD/opencga-home/"
STORAGE_HADOOP_DEPS="hdp3.1"
TEST_TAG="runShortTests"
FAIL_NEVER=""
PREPARE_BRANCHES=""
SAVE_REPORTS=""
DEBUG=""
SKIP_TESTS=false
TESTS_DIR="$PWD/tests"
LOG_FILE=""
TASK_REFERENCE=""
DOCKER=""
COMMAND="build"
PUBLISH="false"

## 2. Read and parse CLI options
while [[ $# -gt 0 ]]; do
  key="$1"
  value="${2:-}"
  case $key in
  -h | --help)
    print_usage
    exit 0
    ;;
  --verbose)
    set -x
    shift # past argument
    ;;
  -o | --opencga-home)
    OPENCGA_HOME_DIR=$(realpath "$value")
    shift # past argument
    shift # past value
    ;;
  -H | --storage-hadoop )
    STORAGE_HADOOP_DEPS="$value"
    shift # past argument
    shift # past value
    ;;
  -s | --test-save-reports )
      SAVE_REPORTS="true"
      COMMAND="test"
      shift # past argument
      ;;
  -d | --docker )
      DOCKER="true"
      shift # past argument
      ;;
    -l | --test-level )
      if [ -z "$value" ];  then
            echo "Test level is empty. The test level must be any combination of these values runShortTests|runMediumTests|runLongTests separated by commas without spaces"
            exit 1
      fi
    TEST_TAG="$value"
    COMMAND="test"
    shift # past argument
    shift # past value
    ;;
  -T | --task )
    TASK_REFERENCE="$value"
    shift # past argument
    shift # past value
    ;;
  -b | --prepare-branches )
    PREPARE_BRANCHES="true"
    shift # past argument
    ;;
  -f | --test-fail-never )
    FAIL_NEVER="--fail-never"
    COMMAND="test"
    shift # past argument
    ;;
  -t | --test )
   COMMAND="test"
    shift # past argument
    ;;
  -d | --debug )
    DEBUG="true"
    shift # past argument
    ;;
  *) # unknown option
    echo "Unknown option $key"
    print_usage
    exit 1
    ;;
  esac
done

## 3. Ensure where is the opencga-enterprise root directory and set it to a variable
cd "$(dirname "$0")" || exit 2
OPENCGA_ENTERPRISE_HOME_DIR=$PWD

## 4. Print parameters if is needed by debug
if [ "$DEBUG" == "true" ];then
  log_summary "OPENCGA_ENTERPRISE_HOME_DIR $OPENCGA_ENTERPRISE_HOME_DIR"
  log_summary "OPENCGA_HOME_DIR $OPENCGA_HOME_DIR"
  log_summary "STORAGE_HADOOP_DEPS $STORAGE_HADOOP_DEPS"
  log_summary "COMMAND $COMMAND"
  log_summary "SAVE_REPORTS $SAVE_REPORTS"
  log_summary "PREPARE_BRANCHES $PREPARE_BRANCHES"
  log_summary "SKIP_TESTS $SKIP_TESTS"
fi

## 5. Sequential call to functions so that the script does everything it should do based on the parameters received

# Validate input parameters
validate

# Prepare branches if needed
prepare_branches

# Build opencb-opencga
build_opencga

# Build opencga-enterprise
build_opencga_enterprise

# Publish test reports
publish_reports

# Publish Docker images
publish_docker

# Print log summary
print_log_summary