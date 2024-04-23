#!/bin/bash

set -e
set -o pipefail
set -o nounset

## Functions
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

function error() {
  log "=========================="
  log "[ERROR] - " "$@"
  log "=========================="
}

function calculate_branch() {

  local EXISTS=""
  if [[ -n $TASK_REFERENCE ]]; then
    local EXISTS=$(git ls-remote origin "$TASK_REFERENCE")
  fi
  if [[ -n $EXISTS ]]; then
    log "Entrando en el if con $EXISTS"
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

function log_summary() {
  if [ -n "$LOG_SUMMARY" ]; then
    LOG_SUMMARY="$LOG_SUMMARY""\n"
  fi
  LOG_SUMMARY="$LOG_SUMMARY""$@"
}
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

function validate() {
  validateTags "$TEST_TAG"

  ## Validate opencga home dir
  if [ ! -d "$OPENCGA_HOME_DIR" ]; then
    log "ERROR OPENCGA HOME NOT FOUND!!!"
    log "You must create in the current directory a symbolic link to the directory where you have downloaded opencga and call it opencga-home"
    log "         ln -s /path/to/opencga $OPENCGA_HOME_DIR    "
    printUsage
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

  log "OPENCGA_DEPENDENCY_VERSION= $OPENCGA_DEPENDENCY_VERSION"
  log "OPENCGA_CURRENT_VERSION= $OPENCGA_CURRENT_VERSION"

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

  local CURRENT_BRANCH="$(git branch --show-current)"
  log "CURRENT_BRANCH $CURRENT_BRANCH"
  log "TASK_REFERENCE $TASK_REFERENCE"

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

function prepareBranches() {
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

function build_opencb_opencga() {
  cd "$OPENCGA_HOME_DIR" || exit 2
  if [ "$COMMAND" == "build" ];then
    if [ "$SKIP_OPENCGA_BUILD" == "true" ] ; then
      log "-- Skipping opencga build"
    else
      log "Compiling opencga... $(pwd)"
      mvn clean install -DskipTests -P"$STORAGE_HADOOP_DEPS" -T 2 || (error "Opencga compilation ERROR" && exit 1)
    fi
  elif [ "$COMMAND" == "test" ];then
    if [ "$SKIP_TESTS" == "true" ]; then
      log "-- Skipping opencga tests"
    else
      mvn clean install surefire-report:report \
        ${FAIL_NEVER} -P "$STORAGE_HADOOP_DEPS","${TEST_TAG}" \
        -Dcheckstyle.skip \
        || (error "Opencga tests ERROR" && exit 1)
      cp "$OPENCGA_HOME_DIR"/opencga-*/target/surefire-reports/TEST*.xml "$TESTS_DIR"
    fi
  fi
}

function build_opencga_enterprise() {
  ## Move to opencga-enterprise to build or test
  cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2

  if [ "$COMMAND" == "build" ];then
    mvn clean install -DskipTests -T 2 \
        -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" \
        -Dopencga-hadoop-shaded.id="$STORAGE_HADOOP_DEPS" \
        -Dopencga.war.name=opencga \
        || (error "Opencga enterprise compilation ERROR" && exit 1)
  elif [ "$COMMAND" == "test" ]; then
    if [ "$SKIP_TESTS" == "true" ]; then
      log "-- Skipping opencga enterprise tests"
    else
      mvn clean install -B verify surefire-report:report \
        -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" \
        -Dopencga-hadoop-shaded.id="$STORAGE_HADOOP_DEPS" \
        ${FAIL_NEVER} \
        || (error "Opencga enterprise tests ERROR" && exit 1)
    fi
    cp "$OPENCGA_ENTERPRISE_HOME_DIR"/opencga-enterprise-*/target/surefire-reports/TEST*.xml "$TESTS_DIR"
  fi
}

function publish() {
  if [ "$PUBLISH" == "true" ];then
    ## Move to opencga-enterprise to build or test
    cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
#    export AZCOPY_SPA_CLIENT_SECRET="kEp8Q~NkI3oQzB-BhUpcKmIRkBF1V-Bf7KFqqbrd"
#    export AZCOPY_AUTO_LOGIN_TYPE="SPN"
#    export AZCOPY_SPA_APPLICATION_ID="6814e731-f1e3-41d7-9d48-6a02989d79e1"
#    export AZCOPY_TENANT_ID="1f730307-f4e7-4a90-ad6b-ebba14be8e24"
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

function publish_docker() {
  if [ "$DOCKER" == "true" ];then
#    cd "$OPENCGA_HOME_DIR" || exit 2
#    TAG=""
#    if [[ -n $TASK_REFERENCE ]]; then
#    	TAG=$TASK_REFERENCE
#    else
#      TAG="$(mvn help:evaluate --file "${OPENCGA_HOME_DIR}/pom.xml" -Dexpression=project.version -q -DforceStdout)"
#    fi
#    python3 ./build/cloud/docker/docker-build.py push --org opencb --images base,init --tag "$TAG"
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

validate

prepareBranches

build_opencb_opencga

build_opencga_enterprise

publish

publish_docker

print_log_summary