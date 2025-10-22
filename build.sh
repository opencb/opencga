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
    echo "$TASK_REFERENCE"
  else
    local TMP_DIR=$(pwd)
    cd "$OPENCGA_ENTERPRISE_HOME_DIR"
    ## This is opencga-enterprise
    ENTERPRISE_BRANCH="$(git branch --show-current)"
    cd "$TMP_DIR"
    ## If opencga-enterprise branch name is main, develop then we return the same name.
    ## Otherwise, we calculate the dependency branch from the dependency version.
    if [[ "$ENTERPRISE_BRANCH" == "TASK"* || "$ENTERPRISE_BRANCH" == "release"* ]]; then
      local VERSION=$(echo "$1" | cut -d "-" -f 1)
      local MAJOR=$(echo "$VERSION" | cut -d "." -f 1)
      local MINOR=$(echo "$VERSION" | cut -d "." -f 2)
      local PATCH=$(echo "$VERSION" | cut -d "." -f 3)
      if [ $PATCH -gt 0 ]; then ## It's a hotfix
        echo "release-$MAJOR.$MINOR.x"
      elif [ $MINOR -eq 0 ]; then ## It's a develop branch
        echo "develop"
      else  ## It's a release branch
        echo "release-$MAJOR.x.x"
      fi
    else
      echo "$ENTERPRISE_BRANCH"
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
      log "The $REPO branch $BRANCH_NAME cloning process has failed!"
      exit 1
  fi
  git checkout "$BRANCH_NAME"
  local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
  if [ "$VERSION" == "$REPO_VERSION" ];then
    log "Version of $REPO to download correct $VERSION should be in $BRANCH_NAME"
    log_summary "Version of $REPO to download correct $VERSION should be in $BRANCH_NAME"
    log_version_summary "$REPO,$VERSION,$BRANCH_NAME"
    if [ "$COMMAND" == "build" ];then
      log "Building $REPO branch $BRANCH_NAME."
      mvn clean install -B -T 2 -DskipTests --no-transfer-progress
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND $REPO with $REPO_VERSION in $BRANCH_NAME FAILED!!!!!"
      else
        log_summary "$COMMAND $REPO with $REPO_VERSION branch $BRANCH_NAME Test Successful!!!"
      fi
    elif [ "$COMMAND" == "test" ]; then
      log "Testing $REPO branch $BRANCH_NAME."
      local pwd=$(pwd)
      echo "${pwd} $REPO" >> "$OPENCGA_ENTERPRISE_HOME_DIR/reports/collected_reports.txt"
      if [ "$REPO" == "cellbase" ]; then
        log "mvn install surefire-report:report ${FAIL_NEVER} -Dcheckstyle.skip -DJUNIT.CELLBASE.DB.MONGODB.HOST=${DB_CELLBASE} --no-transfer-progress"
        mvn install -B surefire-report:report ${FAIL_NEVER} -Dcheckstyle.skip -DJUNIT.CELLBASE.DB.MONGODB.HOST=${DB_CELLBASE} --no-transfer-progress
      else
        mvn install -B surefire-report:report ${FAIL_NEVER} -Dcheckstyle.skip --no-transfer-progress
      fi
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND $REPO with $VERSION in $BRANCH_NAME FAILED!!!!!"
      else
        log_summary "$COMMAND $REPO with $VERSION branch $BRANCH_NAME Test Successful!!!"
      fi
    fi
  else
      log "Version of $REPO to download correct $VERSION should be in $BRANCH_NAME"
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
  echo "     -o     --opencga-home        STRING         OpenCGA project repo directory [./opencga-home]"
  echo "     -H     --storage-hadoop      STRING         Hadoop flavour. hdp3.1, hdi5.1, emr6.1, emr6.13 ... [hdp3.1]"
  echo "     -T     --task                STRING         Task ID used for building and testing dependencies, this will serve as a reference for checkouts"
  echo "     -l     --test-level          STRING         Level of test we must to execute(runShortTests,runMediumTests,runLongTests)"
  echo "     -t     --test                FLAG           Execute the XetaBase tests (by default only build)"
  echo "     -f     --test-fail-never     FLAG           The process executes all tests even if some fail."
  echo "     -b     --prepare-branches    FLAG           Previous to run, it will download and compile all branches of the dependencies."
  echo "            --skip-opencga-build  FLAG           Skip OpenCGA build."
  echo "     -s     --test-save-reports   FLAG           Save OpenCGA JUnit test reports to XetaBase Report server (Quality Team)."
  echo "     -d     --docker              FLAG           Publish docker of OpenCGA-enterprise."
  echo "     -p     --docker-tag          STRING         Tag for docker of OpenCGA-enterprise."
  echo "     -c     --cellbase-db         STRING         Connection to mongodb to test cellbase (host:port)."
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

    cd "$OPENCGA_HOME_DIR" || exit 2
    # Get the current branch name
    branch=$(git branch --show-current)
    cd - || exit 2
    # Check if the command was successful
    if [ $? -eq 0 ]; then
      log "Opencga is on branch: \"$branch\""
    else
      log "Unable to determine the current branch."
    fi
    log "OpenCGA version no match! You must checkout $REF_TYPE \"$REF\" to build from version \"$OPENCGA_DEPENDENCY_VERSION\" of opencga"
    log "Please, execute bellow command and retry:"
    log "  git -C \"$OPENCGA_HOME_DIR\" checkout $REF"
    exit 1
  fi

  ## Validate that if the current branch is a task, the reference branch must be the same
  local CURRENT_BRANCH="$(git branch --show-current)"
  log_summary "opencga-enterprise CURRENT_BRANCH $CURRENT_BRANCH"
  log_summary "opencga-enterprise TASK_REFERENCE $TASK_REFERENCE"

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
  ## Only if you pass the parameter: --prepare-branch -b

  if [ "$PREPARE_BRANCHES" == "true" ]; then
    JCL_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=java-common-libs.version -q -DforceStdout)"
    echo "Downloading and compiling java-common-libs $JCL_DEPENDENCY_VERSION"
    manage_dependency "java-common-libs" "$JCL_DEPENDENCY_VERSION"

    BIODATA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=biodata.version -q -DforceStdout)"
    echo "Downloading and compiling biodata $BIODATA_DEPENDENCY_VERSION"
    manage_dependency "biodata" "$BIODATA_DEPENDENCY_VERSION"

    CELLBASE_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=cellbase.version -q -DforceStdout)"
    echo "Downloading and compiling cellbase $CELLBASE_DEPENDENCY_VERSION"
    manage_dependency "cellbase" "$CELLBASE_DEPENDENCY_VERSION"
  else
    log_summary "Skipped prepare branches"
  fi
}

# Function to build or/and test the opencga
function build_opencga() {
  cd "$OPENCGA_HOME_DIR" || exit 2
  if [ "$COMMAND" == "build" ];then
      if [[ "$SKIP_OPENCGA_BUILD" == "true" ]]; then
        log "Skip opencga build!"
        return
      fi
      log "Compiling opencga... $(pwd)"
      mvn clean install -DskipTests -P"$STORAGE_HADOOP_DEPS" -T 2 --no-transfer-progress
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND opencga build FAILED!!!!!"
        print_log_summary
        exit 1
      else
        local BRANCH="$(git branch --show-current)"
        local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
        log_version_summary "opencga,$VERSION,$BRANCH"
        log_summary "$COMMAND opencga build Success!"
      fi
  elif [ "$COMMAND" == "test" ];then
      local pwd=$(pwd -P)
      echo "${pwd} opencga" >> "$OPENCGA_ENTERPRISE_HOME_DIR/reports/collected_reports.txt"
      mvn clean install -B surefire-report:report ${FAIL_NEVER} -P "$STORAGE_HADOOP_DEPS","${TEST_TAG}" -Dcheckstyle.skip --no-transfer-progress
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND opencga test FAILED!!!!!"
        print_log_summary
        exit 1
      else
        local BRANCH="$(git branch --show-current)"
        local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
        log_version_summary "opencga,$VERSION,$BRANCH"
        log_summary "$COMMAND opencga test Success!"
      fi
  fi
}

# Function to build or/and test the opencga-enterprise
function build_opencga_enterprise() {
  ## Move to opencga-enterprise to build or test
  cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2

  if [ "$COMMAND" == "build" ];then
    mvn clean install -DskipTests -T 2 -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" \
    -Dopencga-hadoop-shaded.id="$STORAGE_HADOOP_DEPS" -Dopencga.war.name=opencga --no-transfer-progress
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND opencga-enterprise build FAILED!!!!!"
        print_log_summary
        exit 1
      else
        local BRANCH="$(git branch --show-current)"
        local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
        log_version_summary "opencga-enterprise,$VERSION,$BRANCH"
        log_summary "$COMMAND opencga-enterprise build Success!"
      fi
  elif [ "$COMMAND" == "test" ]; then
      local pwd=$(pwd)
      echo "${pwd} opencga-enterprise" >> "$OPENCGA_ENTERPRISE_HOME_DIR/reports/collected_reports.txt"
      mvn clean install -B surefire-report:report -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" \
      -Dopencga-hadoop-shaded.id="$STORAGE_HADOOP_DEPS" ${FAIL_NEVER} -Dopencga.war.name=opencga --no-transfer-progress
      if [[ "$?" -ne 0 ]] ; then
        log_summary "[ERROR] $COMMAND opencga-enterprise test FAILED!!!!!"
        print_log_summary
        exit 1
      else
        "$OPENCGA_ENTERPRISE_HOME_DIR"/reports/collect_reports.sh "$OPENCGA_ENTERPRISE_HOME_DIR/reports/collected_reports.txt"
        local BRANCH="$(git branch --show-current)"
        local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
        log_version_summary "opencga-enterprise,$VERSION,$BRANCH"
        log_summary "$COMMAND opencga-enterprise test Success!"
      fi
  fi
}

# Function to upload the opencga and opencga-enterprise test reports to the Zettagenomics test report server
#It is do it with azure and AZ_COPY command
function publish_reports() {
  if [ "$SAVE_REPORTS" == "true" ];then
    ## Move to opencga-enterprise to build or test
    echo "Move to opencga-enterprise to build or test"
    cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
    echo "Preparing destination path"
    local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)

    # Define the local directory to compress and the output file
    FILE_TO_SEND="$OPENCGA_ENTERPRISE_HOME_DIR/reports/$VERSION/"
    echo "The reports are in $FILE_TO_SEND"

    echo "Xetabase tested is $VERSION"
    mv "$OPENCGA_ENTERPRISE_HOME_DIR/reports/test/" "$FILE_TO_SEND"

    DESTINATION_PATH="/var/www/html/reports/xetabase"
    if [[ $TASK_REFERENCE == TASK* ]]; then
      DESTINATION_PATH="$DESTINATION_PATH/$TASK_REFERENCE"
    fi
    echo "Destination path: $DESTINATION_PATH"

    COMPRESSED_FILE="tests.tar.gz"

    tar -czf "$COMPRESSED_FILE" -C "$OPENCGA_ENTERPRISE_HOME_DIR/reports/" "$VERSION"

    # Create the destination directory on the remote server
    sshpass -p "$SSH_PASS" ssh -p "$SSH_PORT" "$SSH_USER@$SSH_HOST" "mkdir -p $DESTINATION_PATH"

    # Send the compressed file to the remote server using scp
    sshpass -p "$SSH_PASS" scp -P "$SSH_PORT" "$COMPRESSED_FILE" "$SSH_USER@$SSH_HOST:$DESTINATION_PATH/$COMPRESSED_FILE"

    # Connect to the remote server and decompress the file
    sshpass -p "$SSH_PASS" ssh -p "$SSH_PORT" "$SSH_USER@$SSH_HOST" "tar -xzf $DESTINATION_PATH/$COMPRESSED_FILE -C $DESTINATION_PATH"

    # Optional: remove the compressed file after decompressing it on the remote server
    sshpass -p "$SSH_PASS" ssh -p "$SSH_PORT" "$SSH_USER@$SSH_HOST" "rm $DESTINATION_PATH/$COMPRESSED_FILE"

    if [ $? -eq 0 ]; then
      echo "Uploaded test report to $DESTINATION_PATH"
    else
      echo "Error transferring file to $SSH_HOST"
      exit 1
    fi
  fi
}

## Function to upload the docker of Oopencga-enterprise to https://hub.docker.com/repositories/zettagenomics
#function publish_dockers() {
#  if [ "$DOCKER" == "true" ];then
#    upload_docker_opencga
#    upload_docker_enterprise
#  fi
#}

#function upload_docker_opencga() {
#  if [ "$DOCKER" == "true" ];then
#    ## Move to opencga-enterprise to build or test
#    cd "$OPENCGA_OPENCGA_HOME_DIR" || exit 2
#    if [[ -n $TASK_REFERENCE ]]; then
#      TAG=$TASK_REFERENCE
#    else
#      TAG="$(mvn help:evaluate --file "${OPENCGA_OPENCGA_HOME_DIR}/pom.xml" -Dexpression=project.version -q -DforceStdout)"
#    fi
#    python3 ./build/cloud/docker/docker-build.py push --org opencb --images base,init --tag "$TAG"
#    if [[ "$?" -ne 0 ]] ; then
#      log_summary "[ERROR] OPENCGA DOCKER UPLOAD FAILED!!!!!"
#    else
#      log_summary "Opencga docker uploaded correctly with tag $TAG"
#    fi
#  fi
#}

# Function to upload the docker of Oopencga-enterprise to https://hub.docker.com/repositories/zettagenomics
function publish_dockers() {
  if [ "$DOCKER" == "true" ];then
    ## Move to opencga-enterprise to build or test
    cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
    if [[ -n "$DOCKER_TAG" ]]; then
      TAG="${DOCKER_TAG}"
    elif [[ -n $TASK_REFERENCE ]]; then
      TAG=$TASK_REFERENCE
    else
      TAG="$(mvn help:evaluate --file "${OPENCGA_ENTERPRISE_HOME_DIR}/pom.xml" -Dexpression=project.version -q -DforceStdout)"
    fi
    python3 ./build/cloud/docker/docker-build.py push --org zettagenomics --images enterprise --tag "$TAG"
    if [[ "$?" -ne 0 ]] ; then
      log_summary "[ERROR] OPENCGA ENTERPRISE DOCKER UPLOAD FAILED!!!!!"
    else
      log_summary "Opencga-enterprise docker uploaded correctly with tag $TAG"
    fi
  fi
}


## FUNCTIONS TO MANAGE LOGS AND PRINTS ##


# Function to add messages to the log summary
function log_summary() {
  if [ -n "$LOG_SUMMARY" ]; then
    LOG_SUMMARY="$LOG_SUMMARY""\n"
  fi
  LOG_SUMMARY="$LOG_SUMMARY""INFO: $(date +"%Y-%m-%d %H:%M:%S")  $@"
}

# Function to add messages to the version summary
function log_version_summary() {
  if [ -n "$VERSION_SUMMARY" ]; then
    VERSION_SUMMARY="$VERSION_SUMMARY""\n"
  fi
  VERSION_SUMMARY="$VERSION_SUMMARY""$@"
}


# Function to add messages to the time summary
function log_time_summary() {
  if [ -n "$TIME_SUMMARY" ]; then
    TIME_SUMMARY="$TIME_SUMMARY""\n"
  fi
  TIME_SUMMARY="$TIME_SUMMARY""$@"
}

# Function to add messages to the param summary
function log_param_summary() {
  if [ -n "$PARAM_SUMMARY" ]; then
    PARAM_SUMMARY="$PARAM_SUMMARY""\n"
  fi
  PARAM_SUMMARY="$PARAM_SUMMARY""$@"
}

# Function to log the execution time
function log_execution_time() {
    local END_TIME=$(date +%s)
    local END_DATE=$(date +"%Y-%m-%d %H:%M:%S")
    local DURATION=$((END_TIME - START_TIME))
    local SECONDS=$((DURATION % 60))
    local MINUTES=$((DURATION / 60 % 60))
    local HOURS=$((DURATION / 3600))
    cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
    local BRANCH="$(git branch --show-current)"
    local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)

    log_time_summary ""
    log_time_summary "==========================="
    log_time_summary ""
    log_time_summary "End Xetabase-$VERSION $COMMAND for branch $BRANCH "
    log_time_summary " "
    log_time_summary "Script execution started at: $START_DATE"
    log_time_summary "Script execution finished at: $END_DATE"
    log_time_summary "Total execution time: ${HOURS}h ${MINUTES}m ${SECONDS}s"
    log_time_summary ""
    log_time_summary "==========================="
}

# Function to print all the log summary
function print_time_summary() {
    echo ""
    echo "==========================" >> "$LOG_FILE"
    echo -e "$TIME_SUMMARY" >> "$LOG_FILE"
    echo "==========================" >> "$LOG_FILE"
}
# Function to print all the log summary
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

# Function to log parameters and global variables
function log_initial_state() {
    log_param_summary "COMMAND,$COMMAND"
    log_param_summary "DB_CELLBASE,$DB_CELLBASE"
    log_param_summary "OPENCGA_HOME_DIR,$OPENCGA_HOME_DIR"
    log_param_summary "STORAGE_HADOOP_DEPS,$STORAGE_HADOOP_DEPS"
    log_param_summary "TEST_TAG,$TEST_TAG"
    log_param_summary "TESTS_DIR,$TESTS_DIR"
    log_param_summary "LOG_FILE,$LOG_FILE"
    log_param_summary "TASK_REFERENCE,$TASK_REFERENCE"
    log_param_summary "SAVE_REPORTS,$(yes_no "$SAVE_REPORTS")"
    log_param_summary "FAIL_NEVER,$(yes_no "$FAIL_NEVER")"
    log_param_summary "PREPARE_BRANCHES,$(yes_no "$PREPARE_BRANCHES")"
    log_param_summary "DEBUG,$(yes_no "$DEBUG")"
    log_param_summary "DOCKER,$(yes_no "$DOCKER")"
}

# Function to print the version summary
function print_version_summary() {
    echo ""  >> "$LOG_FILE"
    printf "%-25s %-20s %-20s\n" "Repository" "Version" "Branch" >> "$LOG_FILE"
    printf "%-25s %-20s %-20s\n" "---------" "-------" "------" >> "$LOG_FILE"
    echo -e "$VERSION_SUMMARY" | while IFS=',' read -r repository version branch; do
        printf "%-25s %-20s %-20s\n" "$repository" "$version" "$branch" >> "$LOG_FILE"
    done
}

# Function to print the parameters summary
function print_param_summary() {
    echo ""  >> "$LOG_FILE"
    printf "%-25s %-20s \n" "Parameter" "Value" >> "$LOG_FILE"
    printf "%-25s %-20s \n" "---------" "-------" >> "$LOG_FILE"
    echo -e "$PARAM_SUMMARY" | while IFS=',' read -r param value; do
        printf "%-25s %-20s \n" "$param" "$value" >> "$LOG_FILE"
    done
}
# Function to generate an HTML table from VERSION_SUMMARY
function generate_param_table() {

    # Start the table with headers
    local table_html="<table style=\"width:100%; border-collapse: collapse;\">
        <thead style=\"background-color: #aaa; color: white;\">
            <tr>
                <th style=\"padding: 8px; border: 1px solid #ddd;\">PARAMETER</th>
                <th style=\"padding: 8px; border: 1px solid #ddd;\">VALUE</th>
            </tr>
        </thead>
        <tbody>"

    # Iterate over the version summary data
    table_html=$table_html$(echo -e "$PARAM_SUMMARY" | while IFS=',' read -r param value; do
        # Add row to the table
        echo "<tr>"
        echo "<td style=\"padding: 8px; border: 1px solid #ddd;\">$param</td>"
        echo "<td style=\"padding: 8px; border: 1px solid #ddd;\">$value</td>"
        echo "</tr>"
    done)

    # Close the table
    table_html="$table_html</tbody></table>"
    echo "$table_html"
}

# Function to generate an HTML table from VERSION_SUMMARY
function generate_version_table() {

    # Start the table with headers
    local table_html="<table style=\"width:100%; border-collapse: collapse;\">
        <thead style=\"background-color: #aaa; color: white;\">
            <tr>
                <th style=\"padding: 8px; border: 1px solid #ddd;\">Repository</th>
                <th style=\"padding: 8px; border: 1px solid #ddd;\">Version</th>
                <th style=\"padding: 8px; border: 1px solid #ddd;\">Branch</th>
            </tr>
        </thead>
        <tbody>"

    # Iterate over the version summary data
    table_html=$table_html$(echo -e "$VERSION_SUMMARY" | while IFS=',' read -r repository version branch; do
        # Add row to the table
        echo "<tr>"
        echo "<td style=\"padding: 8px; border: 1px solid #ddd;\">$repository</td>"
        echo "<td style=\"padding: 8px; border: 1px solid #ddd;\">$version</td>"
        echo "<td style=\"padding: 8px; border: 1px solid #ddd;\">$branch</td>"
        echo "</tr>"
    done)

    # Close the table
    table_html="$table_html</tbody></table>"
    echo "$table_html"
}


# Function to generate the final HTML report
function generate_html_report() {

    # Generate the html report log
    local param_summary=$(generate_param_table)
    local version_summary=$(generate_version_table)
    local execution_time=$(echo -e "$TIME_SUMMARY")
    local template_file="reports/build.html.template"
    local output_file="reports/test/summary.html"

    # Read the template content
    local template_content=$(<"$template_file")

    # Replace placeholders with actual content
    template_content="${template_content//#PARAM_SUMMARY/$param_summary}"
    template_content="${template_content//#VERSION_SUMMARY/$version_summary}"
    template_content="${template_content//#EXECUTION_TIME/$execution_time}"

    # Ensure the output directory exists
    mkdir -p "$(dirname "$output_file")"

    # Write the final content to the output file
    echo "$template_content" > "$output_file"
}


function print_log() {
  # Print log parameters
  print_param_summary
  # Print log summary
  print_log_summary
  # Print version table summary
  print_version_summary
  # Log execution time
  log_execution_time
  print_time_summary
  # Generate html log file
  generate_html_report
  #Print in console the log file
  cat "$LOG_FILE"
}

###################################
####### Script starts here  #######
###################################
## 1. Initialize variables and set default values

# Initialize the global variable LOG_SUMMARY
LOG_SUMMARY=""
DB_CELLBASE="localhost:27017"
OPENCGA_HOME_DIR="$PWD/opencga-home/"
SKIP_OPENCGA_BUILD="false"
STORAGE_HADOOP_DEPS="hdp3.1"
TEST_TAG="runShortTests"
FAIL_NEVER=""
DOCKER_TAG=""
PREPARE_BRANCHES=""
DEBUG=""
SKIP_TESTS=false
TESTS_DIR="$PWD/reports/test"
TASK_REFERENCE=""
DOCKER=""
COMMAND="build"
SAVE_REPORTS="false"
VERSION_SUMMARY=""
PARAM_SUMMARY=""

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
  --verbose)
    set -x
    shift # past argument
    ;;
  -o | --opencga-home)
    OPENCGA_HOME_DIR=$(realpath "$value")
    shift # past argument
    shift # past value
    ;;
  --skip-opencga-build)
    SKIP_OPENCGA_BUILD=true
    shift # past value
    ;;
  -c | --cellbase-db)
    DB_CELLBASE="$value"
    shift # past argument
    shift # past value
    ;;
  -H | --storage-hadoop)
    STORAGE_HADOOP_DEPS="$value"
    shift # past argument
    shift # past value
    ;;
  -s | --test-save-reports)
      SAVE_REPORTS="true"
      COMMAND="test"
      shift # past argument
      ;;
  -d | --docker)
      DOCKER="true"
      shift # past argument
      ;;
  -p | --docker-tag)
      DOCKER_TAG="$value"
      shift # past argument
      shift # past value
      ;;
  -l | --test-level)
      if [ -z "$value" ];  then
            echo "Test level is empty. The test level must be any combination of these values runShortTests|runMediumTests|runLongTests separated by commas without spaces"
            exit 1
      fi
    TEST_TAG="$value"
    COMMAND="test"
    shift # past argument
    shift # past value
    ;;
  -T | --task)
    TASK_REFERENCE="$value"
    shift # past argument
    shift # past value
    ;;
  -b | --prepare-branches)
    PREPARE_BRANCHES="true"
    shift # past argument
    ;;
  -f | --test-fail-never)
    FAIL_NEVER="--fail-never -Dmaven.test.failure.ignore=true -Dsurefire.testFailureIgnore=true"
    COMMAND="test"
    shift # past argument
    ;;
  -t | --test)
   COMMAND="test"
    shift # past argument
    ;;
  --debug)
    DEBUG="true"
    set -x
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

# Check and delete the file if it exists
if [ -f "$OPENCGA_ENTERPRISE_HOME_DIR/reports/collected_reports.txt" ]; then
    rm "$OPENCGA_ENTERPRISE_HOME_DIR/reports/collected_reports.txt"
fi
touch "$OPENCGA_ENTERPRISE_HOME_DIR/reports/collected_reports.txt"

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

# Log initial state of global variables
log_initial_state

# Validate input parameters
validate

# Prepare branches if needed
prepare_branches

# Build opencb-opencga
build_opencga

# Build opencga-enterprise
build_opencga_enterprise

# Publish Docker images
publish_dockers

# Print final log summary
print_log

# Publish test reports as last step because we need finished log file with all information
publish_reports
