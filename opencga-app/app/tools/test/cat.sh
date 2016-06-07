#!/bin/bash

input=/dev/null
outdir=""
name=""


while getopts "hi:o:n:" opt; do
	#echo $opt "=" $OPTARG
	case "$opt" in
	h)
	    echo "Usage: "
	    echo "       -h             :   "
	    echo "    *  -i input       : input file "
	    echo "       -o             : output dir  "
	    #echo "       -             :   "

	    exit 1
	    ;;
	i)
	    echo "Using input file " $OPTARG
	    input=$OPTARG
#	    input_files[$input_files_len]=$OPTARG
#	    input_files_len=$(( $input_files_len + 1 ))
	    ;;
	o)
	    outdir=$OPTARG
	    echo "Using outdir " $outdir
	    ;;
	n)
	    name=$OPTARG
	    echo "Using output name " $name
	    ;;
	\?)
	    ;;
	esac
done

if [[ $outdir == "" || $name == "" ]]
then
    cat ${input}
else
    outdir=${outdir}/${name}
    cat ${input} > "$outdir"
fi
