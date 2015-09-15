#!/bin/bash

PRGDIR=`dirname "$0"`
BASEDIR=`cd "$PRGDIR/.." >/dev/null; pwd`
LOG_FILE=$BASEDIR'/logs/daemon.log'

BOLD=`tput bold`
UNDERLINE_ON=`tput smul`
UNDERLINE_OFF=`tput rmul`
TEXT_BLACK=`tput setaf 0`
TEXT_RED=`tput setaf 1`
TEXT_GREEN=`tput setaf 2`
TEXT_YELLOW=`tput setaf 3`
TEXT_BLUE=`tput setaf 4`
TEXT_MAGENTA=`tput setaf 5`
TEXT_CYAN=`tput setaf 6`
TEXT_WHITE=`tput setaf 7`
BACKGROUND_BLACK=`tput setab 0`
BACKGROUND_RED=`tput setab 1`
BACKGROUND_GREEN=`tput setab 2`
BACKGROUND_YELLOW=`tput setab 3`
BACKGROUND_BLUE=`tput setab 4`
BACKGROUND_MAGENTA=`tput setab 5`
BACKGROUND_CYAN=`tput setab 6`
BACKGROUND_WHITE=`tput setab 7`
RESET_FORMATTING=`tput sgr0`

tail -f $LOG_FILE $@ |& sed  -e "s/\(WARN.*\)/${BOLD}${TEXT_YELLOW}\1${RESET_FORMATTING}/g"  \
                -e "s/\(ERROR.*\)/${BOLD}${TEXT_RED}\1${RESET_FORMATTING}/g"    \
                -e "s/\(INFO.*\)/${BOLD}${TEXT_BLUE}\1${RESET_FORMATTING}/g"    \
                -e "s/\(\[.*\]\)/${BOLD}\1${RESET_FORMATTING}/g"
