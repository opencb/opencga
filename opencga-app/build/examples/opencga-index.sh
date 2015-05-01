#!/bin/bash


#echo $@

#set -v

if [ ! $OPENCGA_HOME ]
then
    export OPENCGA_HOME=/opt/opencga
fi
export OPENCGA_BIN=$OPENCGA_HOME'/bin/opencga.sh'


export user=admin
export password=admin
export project_alias=1000g
export study_alias=ph1
export uri_arg=""
export study_uri=""

export transform_file=false
export pedigree_file=false
export log_level=info
export input_files=()
export input_files_len=0

function getFileId() {
    $OPENCGA_BIN files search --study-id $user@${project_alias}:${study_alias} -u $user -p $password --name $1 --output-format IDS --log-level ${log_level}
}

while getopts "htu:i:p:l:U:" opt; do
	#echo $opt "=" $OPTARG
	case "$opt" in
	h)
	    echo "Usage: "
	    echo "       -h            :   "
	    echo "       -i vcf_file   : VCF input file  "
	    echo "       -p ped_file   : Pedigree input file  "
	    echo "       -u user_name  : User name.  "
	    echo "       -l log_level  : error, warn, info, debug  "
	    echo "       -t            : Transform and Load in 2 steps  "
	    echo "       -U uri        : Study URI location "
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
	    ;;
	t)
	    transform_file=true
	    echo "Transforming file before load"
	    ;;
	\?)
	    ;;
	esac
done


if [[ $input_files_len == 0 && $pedigree_file == false ]]; then
	echo "No input files!"
	exit 1
fi

if [ $log_level == "debug" ]; then
	set -x
fi


$OPENCGA_BIN users create -u $user -p $password -n $user -e user@email.com --log-level ${log_level}
$OPENCGA_BIN projects create -a ${project_alias} -d "1000 genomes" -n "1000 Genomes" -u $user -p $password --log-level ${log_level}
$OPENCGA_BIN users list -u $user -p $password -R
$OPENCGA_BIN studies create -a ${study_alias}  -n "Phase 1" -u $user -p $password --project-id $user@${project_alias} -d asdf --type CONTROL_SET --log-level ${log_level} $uri_arg "$study_uri" --datastore "variant:mongodb:opencga_test_$user"

$OPENCGA_BIN users list -u $user -p $password -R


if [ $pedigree_file == false ]; then
	echo "Do not load ped file"
else
	PEDIGREE_FILE_NAME=$(echo $pedigree_file | rev | cut -d / -f1 | rev )
	$OPENCGA_BIN files create -P -s $user@${project_alias}:${study_alias} -u $user -p $password --input $pedigree_file --bioformat PEDIGREE  --path data/peds/ --checksum --output-format IDS  --log-level ${log_level}
	$OPENCGA_BIN samples load --study-id $user@${project_alias}:${study_alias} -u $user -p $password --pedigree-id $(getFileId $PEDIGREE_FILE_NAME ) --log-level ${log_level}
fi

for input_file in ${input_files[@]}; do
	echo "Indexing file $input_file"
	$OPENCGA_BIN files create -P -s $user@${project_alias}:${study_alias} -u $user -p $password --input $input_file --bioformat VARIANT  --path data/vcfs/ --checksum --output-format IDS  --log-level ${log_level}

	FILE_NAME=$(echo $input_file | rev | cut -d / -f1 | rev )
	VCF_FILE_ID=$(getFileId $FILE_NAME )

	echo "Added VCF file "$input_file" = "$VCF_FILE_ID

	$OPENCGA_BIN users list -u $user -p $password -R

	if [ "$transform_file" == "true" ]; then
		#Transform file
		$OPENCGA_BIN files index -u $user -p $password --file-id $VCF_FILE_ID --output-format IDS --log-level ${log_level} --transform
		$OPENCGA_BIN users list -u $user -p $password -R
		$OPENCGA_BIN files info -u $user -p $password -id $VCF_FILE_ID --exclude projects.studies.files.attributes,projects.studies.files.sampleIds

		#Load file
		TRANSFORMED_VARIANTS_FILE_ID=$(getFileId $FILE_NAME".variants.json")
		$OPENCGA_BIN files index -u $user -p $password --file-id $TRANSFORMED_VARIANTS_FILE_ID --log-level ${log_level} --load -Dannotate=false
		$OPENCGA_BIN files info -u $user -p $password -id $VCF_FILE_ID --exclude projects.studies.files.attributes,projects.studies.files.sampleIds
	else
		$OPENCGA_BIN files index -u $user -p $password --file-id $VCF_FILE_ID  --log-level ${log_level}
		$OPENCGA_BIN files info -u $user -p $password -id $VCF_FILE_ID --exclude projects.studies.files.attributes,projects.studies.files.sampleIds
	fi
done

$OPENCGA_BIN users list -u $user -p $password -R

