
#!/bin/bash

if [ $# -eq 0 ];  then
    echo "The arguments must be 3"
    exit 1
fi
PROFILE=""

if [ $1 == "true" ]; then
  PROFILE="${PROFILE}runShortTests,"
fi
if [ $2 == "true" ]; then
  PROFILE="${PROFILE}runMediumTests,"
fi
if [ $3 == "true" ]; then
  PROFILE="${PROFILE}runLongTests"
fi

if [[ "${PROFILE}" == *"," ]]; then
  PROFILE="${PROFILE%?}"
fi

if [ -z "${PROFILE}" ]; then
  echo "There must be at least one active profile"
  exit 1
fi

echo "${PROFILE}"
exit 0
