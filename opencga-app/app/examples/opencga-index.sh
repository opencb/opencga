#!/bin/bash


#echo $@

#set -v


PRGDIR=`dirname "$0"`
export OPENCGA_HOME=`cd "$PRGDIR/.." >/dev/null; pwd`
export OPENCGA_BIN=$OPENCGA_HOME'/bin/opencga.sh'


user=admin
password=admin
project_alias=1000g
study_alias=ph1
uri_arg=""
study_uri=""

split_index_job=false
transform_file=false
load_file=false
pedigree_file=false
link=false
enqueue=""
annotate=""
calculateStats=""
log_level=info
input_files=()
input_files_len=0
database=""

function getFileId() {
    $OPENCGA_BIN files search --study-id $user@${project_alias}:${study_alias} -u $user -p $password --name $1 --output-format IDS --log-level ${log_level}
}

function main() {
while getopts "htu:s:i:p:l:U:d:qacxTL" opt; do
	#echo $opt "=" $OPTARG
	case "$opt" in
	h)
	    echo "Usage: "
	    echo "       -h             :   "
	    echo "       -u user_name   : User name. [admin] "
	    echo "       -s study_alias : Study alias. [ph1] "
	    echo "    *  -i vcf_file    : VCF input file  "
	    echo "       -x             : Link file instead of copy  "
	    echo "       -p ped_file    : Pedigree input file  "
	    echo "       -l log_level   : error, warn, info, debug [info] "
	    echo "       -d database    : database name [opencga_test_<userId>] "
	    echo "       -T             : If present it only runs the transform stage. Loading requires -L "
	    echo "       -L             : If present only the load stage is executed. Transformation requires -T "
	    echo "       -t             : Transform and Load in 2 steps [DEPRECATED] "
	    echo "       -a             : Annotate database  "
	    echo "       -c             : Calculate stats  "
	    echo "       -U uri         : Study URI location "
	    echo "       -q             : Enqueue index jobs. Leave jobs \"PREPARED\". Require a daemon."
	    #echo "       -             :   "

	    #echo "Usage: -h, -i (vcf_input_file), -u (user_name), -l (log_level), -t, -U (study_URI)"

	    exit 1
	    ;;
	u)
	    user=$OPTARG
	    password=$OPTARG
	    echo "Using user "$user
	    echo "Using password "$password
	    ;;
	s)
	    study_alias=$OPTARG
	    echo "Using study_alias "$study_alias
	    ;;
	i)
	    echo "Using input file "$OPTARG
	    input_files[$input_files_len]=$OPTARG
	    input_files_len=$(( $input_files_len + 1 ))
	    ;;
	p)
	    pedigree_file=$OPTARG
	    echo "Using pedigree file " $OPTARG
	    ;;
	U)
	    uri_arg="--uri"
	    study_uri=$OPTARG
	    echo "Using URI "$study_uri
	    ;;
	l)
	    log_level=$OPTARG
	    echo "Using log-level "$log_level
        if [ $log_level == "debug" ]; then
            set -x
        fi
	    ;;
	d)
	    database=$OPTARG
	    echo "Using database "$log_level
	    ;;
	t)
	    split_index_job=true
	    transform_file=true
	    load_file=true
	    echo "Transforming file before load in two different jobs. DEPRECATED use -TL instead"
	    ;;
	T)
	    split_index_job=true
	    transform_file=true
	    echo "Transforming file"
	    ;;
	L)
	    split_index_job=true
	    load_file=true
	    echo "Loading file"
	    ;;
	a)
	    annotate="--annotate"
	    echo "Annotating database before load"
	    ;;
	c)
	    calculateStats="--calculate-stats"
	    echo "Calculate stats over cohort ALL"
	    ;;
	q)
	    enqueue="--enqueue"
	    echo "Queuing index jobs"
	    ;;
	x)
	    link=true
	    echo "Linking vcf input file"
	    ;;
	\?)
	    ;;
	esac
done

if [[ $enqueue != "" && $transform_file != false ]]; then
    echo "ERROR: Can't index file in 2 steps (transform and load) and enqueue the jobs"
    exit 1
fi

if [[ $input_files_len == 0 && $pedigree_file == false ]]; then
	echo "ERROR: No input files!"
	exit 1
fi



$OPENCGA_BIN users create -u $user -p $password -n $user -e user@email.com --log-level ${log_level}
$OPENCGA_BIN projects create -a ${project_alias} -d "1000 genomes" -n "1000 Genomes" -u $user -p $password --log-level ${log_level}
$OPENCGA_BIN users list -u $user -p $password -R

if [ "$database" == "" ]; then
	database="opencga_test_${user}"
fi

$OPENCGA_BIN studies create -a ${study_alias}  -n "Phase 1" -u $user -p $password --project-id $user@${project_alias} -d "Default study" --type CONTROL_SET --log-level ${log_level} $uri_arg "$study_uri" --datastore "variant:mongodb:${database}"
$OPENCGA_BIN files create-folder -s $user@${project_alias}:${study_alias} -u $user -p $password --log-level ${log_level} --path data/jobs/


$OPENCGA_BIN users list -u $user -p $password -R


if [ $pedigree_file == false ]; then
	echo "Do not load ped file"
else
	PEDIGREE_FILE_NAME=$(echo $pedigree_file | rev | cut -d / -f1 | rev )
	$OPENCGA_BIN files create -P -s $user@${project_alias}:${study_alias} -u $user -p $password --input $pedigree_file --path data/peds/ --checksum --output-format IDS  --log-level ${log_level}
	$OPENCGA_BIN samples load -u $user -p $password --pedigree-id $(getFileId ^${PEDIGREE_FILE_NAME}"$" ) --output-format ID_CSV --log-level ${log_level}
fi

for input_file in ${input_files[@]}; do
	echo "Indexing file $input_file"

	FILE_NAME=$(echo $input_file | rev | cut -d / -f1 | rev )
	VCF_FILE_ID=$(getFileId ${FILE_NAME}"$" )

	if [ -z $VCF_FILE_ID ]; then
		if [ "$link" == "true" ]; then
			$OPENCGA_BIN files link -P -s $user@${project_alias}:${study_alias} -u $user -p $password --input $input_file --path data/vcfs/ --checksum --output-format IDS  --log-level ${log_level}
		else
			$OPENCGA_BIN files create -P -s $user@${project_alias}:${study_alias} -u $user -p $password --input $input_file --path data/vcfs/ --checksum --output-format IDS  --log-level ${log_level}
		fi
		VCF_FILE_ID=$(getFileId ${FILE_NAME}"$" )
	else
		echo "File already pressent on Catalog"
	fi


	echo "Added VCF file "$input_file" = "$VCF_FILE_ID

	$OPENCGA_BIN users list -u $user -p $password -R

	if [ $split_index_job == true ]; then
		#Transform file
		if [ $transform_file == true ]; then
			echo "Transforming file $input_file"
			$OPENCGA_BIN files index -u $user -p $password --file-id $VCF_FILE_ID --output-format IDS --log-level ${log_level} --transform -o $user@${project_alias}:${study_alias}:data:jobs:
			$OPENCGA_BIN users list -u $user -p $password -R
			$OPENCGA_BIN files info -u $user -p $password -id $VCF_FILE_ID --exclude projects.studies.files.attributes,projects.studies.files.sampleIds
		fi

		#Load file
		if [ $load_file == true ]; then
			echo "Loading file $input_file"
			TRANSFORMED_VARIANTS_FILE_ID=$(getFileId ^$FILE_NAME".variants")
			$OPENCGA_BIN files index -u $user -p $password --file-id $TRANSFORMED_VARIANTS_FILE_ID --log-level ${log_level} --load $annotate $calculateStats  -o $user@${project_alias}:${study_alias}:data:jobs:
			$OPENCGA_BIN files info -u $user -p $password -id $VCF_FILE_ID --exclude projects.studies.files.attributes,projects.studies.files.sampleIds
		fi
	else
		$OPENCGA_BIN files index -u $user -p $password --file-id $VCF_FILE_ID  --log-level ${log_level} $enqueue $annotate $calculateStats  -o $user@${project_alias}:${study_alias}:data:jobs:
		$OPENCGA_BIN files info -u $user -p $password -id $VCF_FILE_ID --exclude projects.studies.files.attributes,projects.studies.files.sampleIds
	fi
done

$OPENCGA_BIN users list -u $user -p $password -R

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
                -e "s/\(\[[^ ]*\]\)/${BOLD}\1${RESET_FORMATTING}/g"
