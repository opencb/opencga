
WORKSPACE=${WORKSPACE:-/home/runner/work/}


function compile() {
  local REPO=$1
  if [ ! -d "${WORKSPACE}/$REPO" ]; then
    echo "Directory ${WORKSPACE}/$REPO does not exist. Skip compile"
    return 0;
  fi
  echo "::group::Compiling '$REPO' project from branch $BRANCH_NAME"
  cd "${WORKSPACE}/$REPO" || exit 2
  mvn clean install -DskipTests --no-transfer-progress
  echo "::endgroup::"
}


compile "java-common-libs"
compile "biodata"