#!/bin/bash
function yellow (){
   echo "$(tput setaf 3)$1$(tput setaf 7)"
}
function green (){
   echo "$(tput setaf 2)$1$(tput setaf 7)"
}
function cyan (){
   echo "$(tput setaf 6)$1$(tput setaf 7)"
}
function red(){
   echo "$(tput setaf 1)$1$(tput setaf 7)"
}

if [ -d "./opencga-home" ]; then

  OPENCGA_DENDENCY_VERSION="$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)"
  cd opencga-home
  OPENCGA_CURRENT_VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"

  if [ "$OPENCGA_DENDENCY_VERSION" == "$OPENCGA_CURRENT_VERSION" ]; then
    green "Compiling opencga..."
    mvn clean install -DskipTests -T 2
    # shellcheck disable=SC2181
    if [ $? -eq 0 ]; then
      green "Opencga compilation succes!"
    else
      red "Opencga compilation ERROR"
      exit 1
    fi
  else
    red "Opencga version no match! You must checkout the $OPENCGA_DENDENCY_VERSION of opencga"
    exit 1
  fi
else
  red "ERROR OPENCGA HOME NOT FOUND!!!"
  yellow "You must create in the current directory a symbolic link to the directory where you have downloaded opencga and call it opencga-home"
  cyan "         ln -s /path/to/opencga opencga-home    "
  exit 1
fi

cd ..
mvn clean install -DskipTests -T 2
