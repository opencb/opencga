#!/bin/bash
set -e
set -x

# Function to upload the opencga and opencga-enterprise test reports to the Zettagenomics test report server
#It is do it with azure and AZ_COPY command
OPENCGA_ENTERPRISE_HOME_DIR=$PWD
SAVE_REPORTS=true
function publish_reports() {
  if [ "$SAVE_REPORTS" == "true" ];then
    ## Move to opencga-enterprise to build or test
    echo "Move to opencga-enterprise to build or test"
    cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
    local BRANCH="$(git branch --show-current)"
    local VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
    FILE_TO_SEND="$OPENCGA_ENTERPRISE_HOME_DIR/pom.xml"
    DESTINATION_PATH="/var/www/html/reports/xetabase/TMP/"
    if [[ $BRANCH == TASK* ]]; then
      DESTINATION_PATH="$DESTINATION_PATH/$BRANCH/$VERSION/"
    else
      DESTINATION_PATH="$DESTINATION_PATH/$VERSION/"
    fi
    # Asegurarse de que el directorio de destino existe en el servidor
    sshpass -p "$SSH_PASS" ssh -p "$SSH_PORT" "$SSH_USER@$SSH_HOST" "mkdir -p $DESTINATION_PATH"
    echo "Directory created"

    sshpass -p "$SSH_PASS" scp -P "$SSH_PORT" "$FILE_TO_SEND" "$SSH_USER@$SSH_HOST:$DESTINATION_PATH"
    if [ $? -eq 0 ]; then
      echo "Uploaded file to $SSH_HOST"
    else
      echo "Error transfering file to $SSH_HOST"
      exit 1
    fi
    echo "END!"
  fi

}

publish_reports