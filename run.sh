#!/bin/bash

## Functions
function printUsage() {
  echo ""
  echo "Run opencga-enterprise."
  echo ""
  echo "Usage:   $(basename $0) <command> [options]"
  echo ""
  echo "Commands:"
  echo "    build                Build the opencga-enterprise application"
  echo "    test                 Test all the application tests"
  echo ""
  printBuildUsage
  printTestUsage
}

function printBuildUsage() {
  echo ""
  echo "'build' command:"
  echo ""
  echo "  Usage:   $(basename $0) build [options]"
  echo ""
  echo "  Options:"
  echo "     -o     --opencga-home        STRING         Opencga project repo directory. By default, ./opencga-home"
  echo "     -H     --storage-hadoop      STRING         Hadoop flavour. hdp3.1, emr6.1, ..."
  echo "     -b     --prepare-branches    FLAG           Previous to run tests, it will download and compile all branches of the dependencies."
  echo "            --verbose             FLAG           Print verbose logs"
  echo "            --help                FLAG           Print this help and exit"
  echo ""
}

function printTestUsage() {
  echo ""
  echo "'test' command:"
  echo ""
  echo "  Usage:   $(basename $0) test [options]"
  echo ""
  echo "  Options:"
  echo "     -o     --opencga-home        STRING         Opencga project repo directory. By default, ./opencga-home"
  echo "     -t     --tags                STRING         Level of test we must to execute(runShortTests,runMediumTests,runLongTests)"
  echo "     -f     --fail-never          FLAG           The process executes all tests even if some fail."
  echo "     -b     --prepare-branches    FLAG           Previous to run tests, it will download and compile all branches of the dependencies."
  echo "     -p     --publish             FLAG           Save OpenCGA JUnit test reports to XetaBase Report server (Quality Team)."
  echo "     -s     --skip-tests          FLAG           Publish the results on a reports server without rerunning the test."
  echo "     -v     --verbose             FLAG           Print verbose logs"
  echo "     -h     --help                FLAG           Print this help and exit"
  echo ""
}

function calculate_branch() {
  ## This is opencga-enterprise
  local CURRENT_BRANCH="$(git branch --show-current)"
  ## If opencga-enterprise branch name is main, develop or TASK-XYZ then we return the same name.
  ## Otherwise, we calculate the dependency branch from the dependency version.
  if [[ "$CURRENT_BRANCH" != "release"* ]]; then
    echo "$CURRENT_BRANCH"
  else
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
  fi
}

function install_dependency() {
  cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
  local REPO=$1
  local BRANCH_NAME="$(calculate_branch $2)"
  echo "Version of $REPO to download correct $2 should be in $BRANCH_NAME"
  local TEMP_DIR="$(mktemp -d)"
  cd "$TEMP_DIR" || exit 2
  git clone https://github.com/opencb/"$REPO".git -b "$BRANCH_NAME"
  if [ -d "./$REPO" ]; then
    cd "$REPO" || exit 2
    echo "Branch name $BRANCH_NAME already exists."
    mvn clean install -T 2 -DskipTests
    if [ $? -eq 0 ]; then
      echo "$REPO Compilation Successful!!!"
    fi
  else
   if [[ "$BRANCH_NAME" != "TASK"*  ]]; then
      echo "The $REPO branch $BRANCH_NAME cloning process has failed!"
      exit 1
    else
      echo "The $REPO branch $BRANCH_NAME doesn't exist we use the version $2 from maven repo"
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

###################################
####### Script starts here  #######
###################################
## 1. Set default values
OPENCGA_HOME_DIR="$PWD/opencga-home/"
STORAGE_HADOOP_DEPS="hdp3.1"
TEST_TAG="runShortTests"
FAIL_NEVER=""
TESTS_DIR="tests"

## 2. Parse and validate CLI options
if [ "$1" != "build" ] && [ "$1" != "test" ];then
  printUsage
  exit 0
else
  COMMAND=$1
  shift
fi

while [[ $# -gt 0 ]]; do
  key="$1"
  value="$2"
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
  -t | --tags )
    TEST_TAG="$value"
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

## 2.1 Validate options
validateTags "$TEST_TAG"
echo "Ready to execute $TEST_TAG tests"

## 3. Execute scripts
cd "$(dirname "$0")" || exit 2
OPENCGA_ENTERPRISE_HOME_DIR=$PWD
if [ -d "$TESTS_DIR" ]; then
  rm -rf "$TESTS_DIR"
fi

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

if [ -d "$OPENCGA_HOME_DIR" ]; then
  OPENCGA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)"
  cd "$OPENCGA_HOME_DIR" || exit 2
  OPENCGA_CURRENT_VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"

  echo "OPENCGA_DEPENDENCY_VERSION= $OPENCGA_DEPENDENCY_VERSION"
  echo "OPENCGA_CURRENT_VERSION= $OPENCGA_CURRENT_VERSION"

  if [ "$OPENCGA_DEPENDENCY_VERSION" == "$OPENCGA_CURRENT_VERSION" ]; then
    ## Only if you pass the parameter: --prepare-branch
    if [ "$PREPARE_BRANCHES" == "true" ]; then
      cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
      JCL_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=java-common-libs.version -q -DforceStdout)"
      install_dependency "java-common-libs" "$JCL_DEPENDENCY_VERSION"
      BIODATA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=biodata.version -q -DforceStdout)"
      install_dependency "biodata" "$BIODATA_DEPENDENCY_VERSION"
      CELLBASE_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=cellbase.version -q -DforceStdout)"
      install_dependency "cellbase" "$CELLBASE_DEPENDENCY_VERSION"
    fi

    cd "$OPENCGA_HOME_DIR" || exit 2
    echo "Compiling opencga... $(pwd)"

    if [ "$COMMAND" == "build" ];then
      if ! (mvn enforcer:enforce -Denforcer.rules=requireProfileIdsExist -P"$STORAGE_HADOOP_DEPS" -pl :opencga) ; then
        echo OpenCGA storage hadoop "$STORAGE_HADOOP_DEPS" not found!
        exit 1
      fi
      mvn clean install -DskipTests -Pstorage-hadoop -P"$STORAGE_HADOOP_DEPS" -T 2
      # shellcheck disable=SC2181
      if [ $? -eq 0 ]; then
        echo "Opencga compilation success!"
      else
        echo "Opencga compilation ERROR"
        exit 1
      fi
    fi
    if [ "$COMMAND" == "test" ];then
      if ! (mvn enforcer:enforce -Denforcer.rules=requireProfileIdsExist -P"$STORAGE_HADOOP_DEPS" -pl :opencga) ; then
        echo OpenCGA storage hadoop "$STORAGE_HADOOP_DEPS" not found!
        exit 1
      fi
      if [ "$SKIP_TESTS" != "true" ]; then
        mvn install surefire-report:report ${FAIL_NEVER} -P storage-hadoop,hdp3.1,"${TEST_TAG}" -Dcheckstyle.skip -Popencga-storage-hadoop-deps -pl '!:opencga-storage-hadoop-deps-emr6.1,!:opencga-storage-hadoop-deps-hdp2.6'
      fi
      # shellcheck disable=SC2181
      if [ $? -eq 0 ]; then
        echo "Opencga tests success!"
      else
        echo "Opencga tests ERROR"
        exit 1
      fi
    fi

  else
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
    echo "OpenCGA version no match! You must checkout $REF_TYPE \"$REF\" to build from version \"$OPENCGA_DEPENDENCY_VERSION\" of opencga"
    echo "Please, execute bellow command and retry:"
    echo "  git -C \"$OPENCGA_HOME_DIR\" checkout $REF"
    exit 1
  fi
else
  echo "ERROR OPENCGA HOME NOT FOUND!!!"
  echo "You must create in the current directory a symbolic link to the directory where you have downloaded opencga and call it opencga-home"
  echo "         ln -s /path/to/opencga $OPENCGA_HOME_DIR    "
  printUsage
  exit 1
fi

## 4. Move to opencga-enterprise to build or test
cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2

if [ "$COMMAND" == "build" ];then
  mvn clean install -DskipTests -T 2 -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" -Dopencga-storage-hadoop-deps.id="$STORAGE_HADOOP_DEPS" -Dopencga.war.name=opencga
fi

if [ "$COMMAND" == "test" ]; then
  if [ "$SKIP_TESTS" != "true" ]; then
    mvn -B verify surefire-report:report "${FAIL_NEVER}"
  fi
  mkdir tests
  cp **/target/surefire-reports/TEST*.xml "$TESTS_DIR"
fi
if [ "$PUBLISH" == "true" ];then

  export AZCOPY_SPA_CLIENT_SECRET="kEp8Q~NkI3oQzB-BhUpcKmIRkBF1V-Bf7KFqqbrd"
  export AZCOPY_AUTO_LOGIN_TYPE="SPN"
  export AZCOPY_SPA_APPLICATION_ID="6814e731-f1e3-41d7-9d48-6a02989d79e1"
  export AZCOPY_TENANT_ID="1f730307-f4e7-4a90-ad6b-ebba14be8e24"
  azcopy login --service-principal
  BRANCH_FOLDER=$(git branch --show-current)
  VERSION_FOLDER="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"
  COMMIT=$(git show -q | grep commit | cut -d " " -f 2)
  azcopy copy tests https://zettatest.blob.core.windows.net/test-data/opencga-enterprise/$VERSION_FOLDER/$BRANCH_FOLDER/$COMMIT --recursive
fi
