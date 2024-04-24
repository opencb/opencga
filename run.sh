#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Return value of a pipeline is the value of the last (rightmost) command to exit with a non-zero status
set -o nounset  # Treat unset variables as an error

## Functions
# Function to print usage based on the command
function printUsage() {
  case $COMMAND in
    build)
      printBuildUsage
      ;;
    test)
      printTestUsage
      ;;
    *)
      printMainUsage
      ;;
  esac
}

# Function to print main usage of the script
function printMainUsage() {
  echo ""
  echo "Run opencga-enterprise."
  echo ""
  echo "Usage:   $(basename $0) <command> [options]"
  echo ""
  echo "Commands:"
  echo "    build                Build the opencga-enterprise application"
  echo "    test                 Test all the application tests"
  echo ""
}

# Function to print usage for the build command
function printBuildUsage() {
  echo ""
  echo "Run opencga-enterprise."
  echo "'build' command:"
  echo ""
  echo "  Usage:   $(basename $0) build [options]"
  echo ""
  echo "  Options:"
  echo "     -o     --opencga-home        STRING         Opencga project repo directory. By default, ./opencga-home"
  echo "     -H     --storage-hadoop      STRING         Hadoop flavour. hdp3.1, hdi5.1, emr6.1, emr6.13 ..."
  echo "     -b     --prepare-branches    FLAG           Previous to run tests, it will download and compile all branches of the dependencies."
  echo "            --skip-opencga-build  FLAG           Assume opencga is already build in the correct version. Use with caution"
  echo "            --verbose             FLAG           Print verbose logs"
  echo "            --help                FLAG           Print this help and exit"
  echo ""
}

# Function to print usage for the test command
function printTestUsage() {
  echo ""
  echo "Run opencga-enterprise."
  echo "'test' command:"
  echo ""
  echo "  Usage:   $(basename $0) test [options]"
  echo ""
  echo "  Options:"
  echo "     -o     --opencga-home        STRING         Opencga project repo directory. By default, ./opencga-home"
  echo "     -t     --task                STRING         Task that we are testing and that will serve as a reference for checkouts"
  echo "     -l     --level               STRING         Level of test we must to execute(runShortTests,runMediumTests,runLongTests)"
  echo "     -f     --fail-never          FLAG           The process executes all tests even if some fail."
  echo "     -b     --prepare-branches    FLAG           Previous to run tests, it will download and compile all branches of the dependencies."
  echo "     -p     --publish             FLAG           Save OpenCGA JUnit test reports to XetaBase Report server (Quality Team)."
  echo "     -d     --docker              FLAG           Publish dockers of OpenCGA and OpenCGA-enterprise."
  echo "     -s     --skip-tests          FLAG           Publish the results on a reports server without rerunning the test."
  echo "     -v     --verbose             FLAG           Print verbose logs"
  echo "     -h     --help                FLAG           Print this help and exit"
  echo ""
}

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
    log "Entering the if statement with $EXISTS"
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
    else
      log "The $REPO branch $BRANCH_NAME doesn't exist we use the version $REPO_VERSION from maven repo"
    fi
  fi
  log "Version of $REPO to download correct $REPO_VERSION should be in $BRANCH_NAME"
  log_summary "$REPO $REPO_VERSION $BRANCH_NAME"
  git checkout "$BRANCH_NAME"
  if [ "$COMMAND" == "test" ];then
    if [ "$SKIP_TESTS" == "true" ]; then
      log "Skipping test compiling $REPO branch $BRANCH_NAME."
      mvn clean install -T 2 -DskipTests || (error "The $REPO branch $BRANCH_NAME compilation process has failed!"; exit 1)
      log "$REPO Compilation Successful!!!"
    else
      log "Testing $REPO branch $BRANCH_NAME."
      mvn install surefire-report:report ${FAIL_NEVER} -Dcheckstyle.skip || (error "Testing $REPO branch $BRANCH_NAME ERROR" && exit 1)
      log "$REPO branch $BRANCH_NAME Test Successful!!!"
    fi
  fi
  cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
}

# Function to validate test tags
function validateTags() {

  #Split input string
  IFS=',' read -ra my_array <<< "$1"

  #Check the split string
  for i in "${my_array[@]}"
  do
    if [ "$i" != "runShortTests" ] && [ "$i" != "runMediumTests" ] && [ "$i" != "runLongTests" ];then
      echo "Level of test must be a combination of runShortTests,runMediumTests,runLongTests without spaces"
      exit 1
    fi
  done

}

LOG_SUMMARY=""

# Function to log summary
function log_summary() {
  if [ -n "$LOG_SUMMARY" ]; then
    LOG_SUMMARY="$LOG_SUMMARY""\n"
  fi
  LOG_SUMMARY="$LOG_SUMMARY""$@"
}

# Function to print log summary
function print_log_summary() {
  echo "=========================="
  echo -e "$LOG_SUMMARY"
  echo "=========================="
}

###################################
####### Script starts here  #######
###################################
## 1. Set default values
OPENCGA_HOME_DIR="$PWD/opencga-home/"
STORAGE_HADOOP_DEPS="hdp3.1"
TEST_TAG="runShortTests"
FAIL_NEVER=""
PREPARE_BRANCHES=""
PUBLISH=""
DEBUG=""
SKIP_OPENCGA_BUILD=false
SKIP_TESTS=false
TESTS_DIR="$PWD/tests"
LOG_FILE=""
TASK_REFERENCE=""
DOCKER=""

## 2. Parse and validate CLI options
COMMAND=${1:-}
case $COMMAND in
  build | test)
    shift
    ;;
  *)
    printUsage
    exit 1
  ;;
esac

while [[ $# -gt 0 ]]; do
  key="$1"
  value="${2:-}"
  case $key in
  -h | --help)
    if [ "$COMMAND" == "build" ];then
      printBuildUsage
      exit 0
    fi
    if [ "$COMMAND" == "test" ];then
      printTestUsage
      exit 0
    fi
    printUsage
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
  -p | --publish )
      PUBLISH="true"
      shift # past argument
      ;;
  -d | --docker )
      DOCKER="true"
      shift # past argument
      ;;
    -l | --level )
    TEST_TAG="$value"
    shift # past argument
    shift # past value
    ;;
  -t | --task )
    TASK_REFERENCE="$value"
    shift # past argument
    shift # past value
    ;;
  -b | --prepare-branches )
    PREPARE_BRANCHES="true"
    shift # past argument
    ;;
  -f | --fail-never )
    FAIL_NEVER="--fail-never"
    shift # past argument
    ;;
  --skip-opencga-build)
    SKIP_OPENCGA_BUILD=true
    shift # past argument
    ;;
  -s | --skip-tests )
    SKIP_TESTS="true"
    shift # past argument
    ;;
  -d | --debug )
    DEBUG="true"
    shift # past argument
    ;;
  *) # unknown option
    echo "Unknown option $key"
    printUsage
    exit 1
    ;;
  esac
done

cd "$(dirname "$0")" || exit 2
OPENCGA_ENTERPRISE_HOME_DIR=$PWD

if [ "$DEBUG" == "true" ];then
  echo "OPENCGA_ENTERPRISE_HOME_DIR $OPENCGA_ENTERPRISE_HOME_DIR"
  echo "OPENCGA_HOME_DIR $OPENCGA_HOME_DIR"
  echo "STORAGE_HADOOP_DEPS $STORAGE_HADOOP_DEPS"
  echo "COMMAND $COMMAND"
  echo "PUBLISH $PUBLISH"
  echo "PREPARE_BRANCHES $PREPARE_BRANCHES"
  echo "SKIP_TESTS $SKIP_TESTS"
  exit 0
fi

# Validate input parameters
validate

# Prepare branches if needed
prepareBranches

# Build opencb-opencga
build_opencb_opencga

# Build opencga-enterprise
build_opencga_enterprise

# Publish test reports
publish

# Publish Docker images
publish_docker

# Print log summary
print_log_summary
