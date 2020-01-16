#!/bin/bash

if [ ! $OPENCGA_HOME ]
then
    export OPENCGA_HOME=/opt/opencga
fi
export OPENCGA_BIN=$OPENCGA_HOME'/bin/opencga.sh'
export OPENCGA_STORAGE_BIN=$OPENCGA_HOME'/bin/opencga-storage.sh'



export user=admin
export password=admin

export log_level=info
export input_files=()
export input_files_len=0
export database=0


function main {

    while getopts "hu:i:l:d:" opt; do
        case "$opt" in
        h)
            echo "Usage: "
            echo "       -h            :   "
            echo "       -i vcf_file   : Indexed vcf fileName  "
            echo "       -u user_name  : User name.  "
            echo "       -d db_name    : Database name.  "
            echo "       -l log_level  : error, warn, info, debug  "
            #echo "       -            :   "

            #echo "Usage: -h, -i (vcf_input_file), -u (user_name), -l (log_level), -t, -U (study_URI)"

            exit 1
            ;;
        u)
            user=$OPTARG
            password=$OPTARG
            echo "Using user "$user
            echo "Using password "$password
            ;;
        i)
            echo "Using indexed file "$OPTARG
            input_files[$input_files_len]=$OPTARG
            input_files_len=$(( $input_files_len + 1 ))
            ;;
        d)
            echo "Using database "$OPTARG
            database=$OPTARG
            ;;
        l)
            log_level=$OPTARG
            echo "Using log-level "$log_level
            if [ $log_level == "debug" ]; then
                set -x
            fi
            ;;
        \?)
            ;;
        esac
    done

    if [[ $input_files_len == 0 ]]; then
        echo "ERROR: No input files!"
        exit 1
    fi

    if [[ $database == 0 ]]; then
        database=opencga_test_${user}
    fi


    for input_file in ${input_files[@]}; do
        echo "Info file $input_file"
        $OPENCGA_BIN files info -id ${input_file} -u $user -p $password --log-level ${log_level}
        file_id=$($OPENCGA_BIN files info -id ${input_file} -u $user -p $password --log-level ${log_level} --output-format IDS)
        echo "$input_file = $file_id"

        $OPENCGA_STORAGE_BIN fetch-variants --alias ${file_id} -d ${database} -Dlimit=2

    done

}


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

main $@ |& sed  -e "s/\(WARN.*\)/${BOLD}${TEXT_YELLOW}\1${RESET_FORMATTING}/g"  \
                -e "s/\(ERROR.*\)/${BOLD}${TEXT_RED}\1${RESET_FORMATTING}/g"    \
                -e "s/\(INFO.*\)/${BOLD}${TEXT_BLUE}\1${RESET_FORMATTING}/g"    \
                -e "s/\(\[.*\]\)/${BOLD}\1${RESET_FORMATTING}/g"
