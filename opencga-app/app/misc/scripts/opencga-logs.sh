#!/bin/sh

FILE=""
LINES=+1
LOG_LEVEL=INFO
OPENCB_ONLY=FALSE
FOLLOW=FALSE
COLOR=TRUE
LOG_PREFIX=""

printUsage() {
  echo ""
  echo "Usage:   $(basename $0) [OPTION]... [FILE]..."
  echo ""
  echo "  With no FILE, or when FILE is -, read standard input."
  echo ""
  echo "Options:"
  echo "     -n     --tail            INT       Output the last NUM lines"
  echo "     -f     --follow                    Output appended data as the file grows"
  echo "     -l     --log-level       STRING    Log level filter. [debug, info, warn, error]. Default: info"
  echo "     --ocb  --opencb                    Output org.opencb.* loggers only"
  echo "            --color           BOOL      Use color output. Default: true"
  echo "            --prefix          STRING    Log prefix. Typically the host-name source of the logs"
  echo "     -h     --help                      Print this help"
  echo "            --verbose                   Verbose mode. Print debugging messages about the progress."
  echo ""
}

while [ $# -gt 0 ]; do
  key="$1"
  value="$2"
  case $key in
  -h | --help)
    printUsage
    return 0
    ;;
  -n | --tail)
    LINES="${value}"
    shift # past argument
    shift # past value
    ;;
  -l | --log-level)
    LOG_LEVEL="${value}"
    shift # past argument
    shift # past value
    ;;
  --color)
    COLOR=$(echo "${value}" | tr '[:lower:]' '[:upper:]') # Read value in uppercase
    shift                                                 # past argument
    shift                                                 # past value
    ;;
  -f | --follow)
    FOLLOW="TRUE"
    shift # past argument
    ;;
  --ocb | --opencb)
    OPENCB_ONLY="TRUE"
    shift # past argument
    ;;
  --prefix)
    LOG_PREFIX="${value} "
    shift # past argument
    shift # past value
    ;;
  --verbose)
    set -x
    shift # past argument
    ;;
  *) # unknown option
    if [ -z "$FILE" ]; then
      FILE="$key"
    else
      FILE="$key $FILE"
    fi
    shift # past argument
    ;;
  esac
done

if [ -z "$FILE" ]; then
  #  echo "Missing file"
  #  printUsage
  #  exit 1
  FILE="-"
fi

if [ $FOLLOW = "TRUE" ]; then
  if [ "$FILE" = "-" ]; then
    TAIL_OPTS="${TAIL_OPTS} -f "
  else
    TAIL_OPTS="${TAIL_OPTS} -F "
  fi
fi

if ! command -v tput >/dev/null 2>&1; then
  COLOR="FALSE"
fi

if [ ${COLOR} = "TRUE" ] || [ ${COLOR} = "YES" ]; then
  export TERM=xterm-256color
  BOLD=$(tput bold)
  TEXT_BLACK=$(tput setaf 0)
  TEXT_RED=$(tput setaf 1)
  TEXT_GREEN=$(tput setaf 2)
  TEXT_YELLOW=$(tput setaf 3)
  TEXT_BLUE=$(tput setaf 4)
  TEXT_MAGENTA=$(tput setaf 5)
  TEXT_CYAN=$(tput setaf 6)
  TEXT_WHITE=$(tput setaf 7)
  TEXT_GRAY=$(tput setaf 245)
  RESET_FORMATTING=$(tput sgr0)
else
  BOLD=
  TEXT_BLACK=
  TEXT_RED=
  TEXT_GREEN=
  TEXT_YELLOW=
  TEXT_BLUE=
  TEXT_MAGENTA=
  TEXT_CYAN=
  TEXT_WHITE=
  TEXT_GRAY=
  RESET_FORMATTING=
fi

case $LOG_LEVEL in
INFO | info)
  SELECT='. | select( .level != "DEBUG")'
  ;;
WARN | warn)
  SELECT='. | select( .level != "DEBUG" and .level != "INFO")'
  ;;
ERROR | error)
  SELECT='. | select( .level == "ERROR")'
  ;;
*)
  SELECT="."
  ;;
esac

if [ $OPENCB_ONLY = "TRUE" ]; then
  SELECT=${SELECT}' | select( .loggerName  | startswith("org.opencb.") ) '
fi

opencgaLogs() {

  FILE=$1
  LOG_PREFIX="$2"
  JQ_SCRIPT=$(
    cat <<EOM
  . as \$line |
    try (
        fromjson  |
        ${SELECT} |
            "${TEXT_MAGENTA}${LOG_PREFIX}${TEXT_CYAN}" + ( .instant.epochSecond | todate)
            + " ${TEXT_YELLOW}[" + .thread + "] "

            + if   .level == "INFO" then "${BOLD}${TEXT_GREEN}"
              elif .level == "ERROR" then "${BOLD}${TEXT_RED}"
              elif .level == "WARN" then "${BOLD}${TEXT_YELLOW}"
              else "${TEXT_BLUE}" end
            + .level + "${RESET_FORMATTING} "

            + "${BOLD}" + ( .loggerName | split(".") )[-1] + "${RESET_FORMATTING}"

            + if .level == "DEBUG" then "${TEXT_GRAY}" else "${TEXT_WHITE}" end
            + " - " + .message + " " + .thrown.extendedStackTrace
     ) catch (
         "${TEXT_MAGENTA}${LOG_PREFIX}${BOLD}${TEXT_RED}" + "[RAW]" + "${RESET_FORMATTING}"
          + " - " + \$line
     )
EOM
  )

  exec tail -n ${LINES} ${TAIL_OPTS} "${FILE}" | jq --unbuffered -r -R "${JQ_SCRIPT}"
}

if [ "$FILE" = "-" ]; then
  opencgaLogs "$FILE" "$LOG_PREFIX"
else
  # See https://stackoverflow.com/a/22644006/2073398
  trap "exit" INT TERM
  trap "kill 0" EXIT

  for i in $FILE; do
    opencgaLogs "$i" "$(basename "$i") " &
  done

  wait
fi
