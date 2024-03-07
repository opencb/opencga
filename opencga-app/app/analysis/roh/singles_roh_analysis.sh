#!/bin/bash
############################################################
# Help                                                     #
############################################################

Help() 
{
   # Display Help
   echo "Script to calculate regions of homozygosity (ROH) using plink and python. Some pre-processing steps are performed with bcftools."
   echo "Usage example: ${0##*/} -i /home/data/sample.vcf.gz -p /home/scripts -s sample -c 22 [OPTIONS]"
   echo
   echo "Options:"
   echo "-i, --input-vcf-path             STRING      Full path or relative path of the input vcf. [REQUIRED]"
   echo "-p, --python-roh-script-path     STRING      Full path or relative path to the folder where the roh_opencgadatamodel_visualisation.py is located. [REQUIRED]"
   echo "-s, --sample-name                STRING      Sample to analyse. [REQUIRED]"
   echo "-c, --chromosome                 INTEGER     Chromosome to analyse. [REQUIRED]"
   echo "-o, --output-folder-dir          STRING      Full path or relative path to the desired output folder directory. Default: Current directory"
   echo "--output-folder-name             STRING      Output folder name. Default: samplename_chr_roh_results_pipeline_timestamp"
   echo "--filter                         STRING      FILTER (VCF FILTER annotation field) category to filter in variants in the ROH analysis. Default: PASS."
   echo "--genotype-quality               INTEGER     GQ (VCF genotype quality annotation field) threshold to filter in variants in the ROH analysis. Default: 40 (GQ>40)."
   echo "--skip-genotype-quality                      Flag to not use the GQ (VCF genotype quality annotation field) to filter in variants in the ROH analysis. Default: false"
   echo "--homozyg-window-snp             INTEGER     Plink's parameter to set the scanning window size to look for ROHs. Default: 50" 
   echo "--homozyg-window-het             INTEGER     Plink's parameter to set the maximum number of heterozygous calls in a scanning window hit. Default: 1"
   echo "--homozyg-window-missing         INTEGER     Plink's parameter to set the maximum number of missing calls in a scanning window hit. Default: 5"
   echo "--homozyg-window-threshold       FLOAT       Plink's parameter to set the minimum scanning window rate. Default: 0.05"
   echo "--homozyg-kb                     INTEGER     Plink's parameter to set the minimum ROH length in kilobases (kb). Default: 1000"
   echo "--homozyg-snp                    INTEGER     Plink's parameter to set the minimum number of snps in a ROH. Default: 100"
   echo "--homozyg-het                    INTEGER     Plink's parameter to set the maximum number of heterozygous calls in a ROH. Default: unlimited."
   echo "--homozyg-density                INTEGER     Plink's parameter to set the maximum inverse ROH density (kb/SNP). Minimum ROH density: 1 SNP per X kb. Default: 50"
   echo "--homozyg-gap                    INTEGER     Plink's parameter to set the maximum gap length in kb between two consecutive SNPs to be considered part of the same ROH. Default: 1000"
   echo "-h, --help                                   Display this help message and exit. Default: false"
   echo 
}

############################################################
############################################################
# Main program                                             #
############################################################
############################################################

# Initial default values for variables
output_folder_dir=$(pwd)
vcf_filter="PASS"
gq=40
skip_gq=0
# Default values used in plink:
# https://www.cog-genomics.org/plink/1.9/ibd
homozyg_window_snp=50
homozyg_window_het=1
homozyg_window_missing=5
homozyg_window_threshold=0.05
homozyg_kb=1000
homozyg_snp=100
homozyg_density=50
homozyg_gap=1000

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
   case $1 in
      -i|--input-vcf-path) # Input VCF path         
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            input_vcf=$2
            shift 2
         else
            echo "Error: --input-vcf-path requires a non-empty argument."
            exit 1
         fi
         ;;
      -p|--python-roh-script-path) # Input python roh script path
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            roh_python_script_folder=$2
            shift 2
         else
            echo "Error: --python-roh-script-path requires a non-empty argument."
            exit 1
         fi
         ;;
      -s|--sample-name) # Sample name
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            sample=$2
            shift 2
         else
            echo "Error: --sample-name requires a non-empty argument."
            exit 1
         fi
         ;;
      -c|--chromosome) # Chromosome number
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            chr=$2
            shift 2
         else
            echo "Error: --chromosome requires a non-empty argument."
            exit 1
         fi
         ;;
      -o|--output-folder-dir) # Output folder dir
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            output_folder_dir=$2
            shift 2
         else
            echo "Error: --output-folder-dir requires a non-empty option argument."
            exit 1
         fi
         ;;
      --output-folder-name) # Output folder name
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            output_folder_name=$2
            shift 2
         else
            echo "Error: --output-folder-name requires a non-empty option argument."
            exit 1
         fi
         ;;
      --filter) # FILTER category to filter in variants in the ROH analysis"
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            vcf_filter=$2
            shift 2
         else
            echo "Error: --filter requires a non-empty option argument."
            exit 1
         fi
         ;;
      --genotype-quality) # GQ threshold to filter in variants in the ROH analysis (GQ > X)
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            gq=$2
            shift 2
         else
            echo "Error: --genotype-quality requires a non-empty option argument."
            exit 1
         fi
         ;;
      --skip-genotype-quality) # Flag to not use GQ field to filter in variants in the ROH analysis
         skip_gq=1
         gq='"false"'
         echo "The --genotype-quality parameter will be ignored because the flag --skip-genotype-quality has been activated."
         shift
         ;;
      --homozyg-window-snp) # Scanning window size
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            homozyg_window_snp=$2
            shift 2
         else
            echo "Error: --homozyg-window-snp requires a non-empty option argument."
            exit 1
         fi
         ;;
      --homozyg-window-het) # Maximum number of heterozygous calls in a scanning window hit
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            homozyg_window_het=$2
            shift 2
         else
            echo "Error: --homozyg-window-het requires a non-empty option argument."
            exit 1
         fi
         ;;
      --homozyg-window-missing) # Maximum number of missing calls in a scanning window hit
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            homozyg_window_missing=$2
            shift 2
         else
            echo "Error: --homozyg-window-missing requires a non-empty option argument."
            exit 1
         fi
         ;;
      --homozyg-window-threshold) # Minimum scanning window rate
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            homozyg_window_threshold=$2
            shift 2
         else
            echo "Error: --homozyg-window-threshold requires a non-empty option argument."
            exit 1
         fi
         ;;
      --homozyg-kb) # Minimum ROH length in kilobases (kb)
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            homozyg_kb=$2
            shift 2
         else
            echo "Error: --homozyg-kb requires a non-empty option argument."
            exit 1
         fi
         ;;
      --homozyg-snp) # Minimum number of snps in a ROH
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            homozyg_snp=$2
            shift 2
         else
            echo "Error: --homozyg-snp requires a non-empty option argument."
            exit 1
         fi
         ;;
      --homozyg-het) # Maximum number of heterozygous calls in a ROH
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            homozyg_het=$2
            shift 2
         else
            echo "Error: --homozyg-het requires a non-empty option argument."
            exit 1
         fi
         ;;
      --homozyg-density) # Maximum inverse ROH density (kb/SNP). Minimum ROH density: 1 SNP per X kb.
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            homozyg_density=$2
            shift 2
         else
            echo "Error: --homozyg-density requires a non-empty option argument."
            exit 1
         fi
         ;;
      --homozyg-gap) # Maximum gap length in kb between two consecutive SNPs to be considered part of the same ROH
         if [[ -n $2 && ${2:0:1} != "-" ]]; then
            homozyg_gap=$2
            shift 2
         else
            echo "Error: --homozyg-gap requires a non-empty option argument."
            exit 1
         fi
         ;;
      -h|--help) # Display help
         Help
         exit 0
         ;;
      --)  # End of all options
         shift
         break
         ;;
      *)  # Handle unrecognised options and break out of loop if necessary
         echo "Error: Unsupported flag $1" >&2
         echo ""
         Help
         exit 1
         ;;
   esac
done

# More checks
if [[ -z $input_vcf ]] || [[ -z $roh_python_script_folder ]] || [[ -z $chr ]] || [[ -z $sample ]]; then
   echo "Error: --input-vcf-path, --python-roh-script-path, --sample-name, and --chromosome are required arguments."
   exit 1
fi

if [[ -z $output_folder_name ]]; then
   timestamp=$(date +%d%m%Y_%H%M%S)
   output_folder_name=$sample"_chr"$chr"_roh_results_pipeline_"$timestamp
fi

if ! zgrep "#CHROM" $input_vcf | grep -q $sample; then 
   echo "Error: The sample $sample has not been found in the VCF file. Please check the sample name and the VCF file provided."
   exit 1
fi

if ! zgrep -v "#" $input_vcf | cut -f9 | grep -q GQ; then
   if [[ skip_gq -eq 0 ]]; then
      echo "Error: GQ field is not present under the FORMAT column in the VCF file provided. Please use the flag --skip-genotype-quality."
      exit 1
   fi
fi

# Creating folder with all the results
full_directory_all_results=$output_folder_dir"/"$output_folder_name
mkdir $full_directory_all_results


############################################################
# BCFTOOLS                                                 #
############################################################

# 1- Changing rsid column in the vcf file to have variant identifiers chr:pos:ref_allele:alt_allele(s) with bcftools
vcf_file_prefix="$(echo $input_vcf | rev | cut -d "/" -f1 | rev | grep -oP '.*?(?=.vcf)')"
vcf_bgz=$full_directory_all_results"/"$vcf_file_prefix".vcf.bgz"
# BGZIP the VCF to read it with BCFTOOLS
gunzip -c $input_vcf | bgzip  > $vcf_bgz
# Indexing BGZIP VCF
bcftools index $vcf_bgz
# Filtering in variants by FILTER and FORMAT/GQ VCF fields and saving the new filtered and annotated VCF BGZIPPED
annotated_vcf=$full_directory_all_results"/"$sample"_"$vcf_file_prefix"_filtered_annotated.vcf.gz"

if [[ $skip_gq -eq 1 ]]; then
   filters='FILTER="'$vcf_filter'"'
else
   filters='FILTER="'$vcf_filter'" & FMT/GQ>'$gq
fi
bcftools view --samples $sample $vcf_bgz | bcftools filter --include "$filters" | bcftools annotate --set-id '%CHROM:%POS:%REF:%ALT' -Oz -o $annotated_vcf 


############################################################
# PLINK                                                    #
############################################################

# 2- Converting and calculating allele frequencies only for bi-allelic SNPs from the VCF BGZIPPED to BED file
annotated_vcf_files_prefix="$(echo $annotated_vcf | rev | cut -d "/" -f1 | rev | grep -oP '.*?(?=.vcf)')"
files_prefix=$annotated_vcf_files_prefix'_chr'$chr
output_folder_files_prefix=$full_directory_all_results"/"$files_prefix
plink --vcf $annotated_vcf --make-bed --chr $chr --snps-only --biallelic-only strict --vcf-half-call haploid --freq --out $output_folder_files_prefix 


# 3- Calculating regions of homozygosity (ROH).
# Best parameters for WES based on this paper (genetics in medicine).
# https://www.gimjournal.org/article/S1098-3600(21)04386-0/fulltext
# homozyg_kb=1000
# homozyg_snp=10
# homozyg_window_snp=20
# homozyg_window_missing=10
# homozyg_window_threshold=0.05
# homozyg_density=200
# homozyg_gap=4000
# homozyg_window_het=1

# Best parameters for WES based on this paper (nature).
# https://www.nature.com/articles/s41467-020-20584-4#Sec3
# homozyg_kb=1000
# homozyg_snp=10
# homozyg_window_snp=20
# homozyg_window_missing=10
# homozyg_window_threshold=0.05
# homozyg_density=10000
# homozyg_gap=10000
# homozyg_window_het=3
input_bfiles=$output_folder_files_prefix
if [[ -n $homozyg_het ]]; then
   plink --bfile $input_bfiles --homozyg --homozyg-kb $homozyg_kb --homozyg-snp $homozyg_snp --homozyg-het $homozyg_het --homozyg-window-snp $homozyg_window_snp --homozyg-window-missing $homozyg_window_missing --homozyg-window-threshold $homozyg_window_threshold --homozyg-density $homozyg_density --homozyg-gap $homozyg_gap --homozyg-window-het $homozyg_window_het --out $output_folder_files_prefix
else
   plink --bfile $input_bfiles --homozyg --homozyg-kb $homozyg_kb --homozyg-snp $homozyg_snp --homozyg-window-snp $homozyg_window_snp --homozyg-window-missing $homozyg_window_missing --homozyg-window-threshold $homozyg_window_threshold --homozyg-density $homozyg_density --homozyg-gap $homozyg_gap --homozyg-window-het $homozyg_window_het --out $output_folder_files_prefix
fi

############################################################
# PYTHON                                                   #
############################################################

# 4- Generating OpenCGA ROH analysis json data model. Content: bcftools and plink parameters used to calculate ROHs.
bcftools_version="$(bcftools --version | cut -d" " -f2 | awk 'NR==1 {print $0}')"
plink_version="$(plink --version | cut -d" " -f2)"
roh_json_data_model_opencga_file=$full_directory_all_results"/opencga_roh_analysis_method_data_model.json"
if [[ -n $homozyg_het ]]; then
   roh_json_data_model_opencga="$(echo "{\"roh\":[{\"methods\":[{\"software\":\"BCFTOOLS\",\"version\":\""$bcftools_version"\",\"params\":{\"FILTER\": \"$vcf_filter\",\"FMT/GQ\":$gq}},{\"software\":\"PLINK\",\"version\":\""$plink_version"\",\"params\":{\"chr\":$chr,\"homozyg\":\"true\",\"homozyg-snp\":"$homozyg_snp",\"homozyg-kb\":"$homozyg_kb",\"homozyg-het\":"$homozyg_het",\"homozyg-density\":"$homozyg_density",\"homozyg-gap\":"$homozyg_gap",\"homozyg-window-snp\":"$homozyg_window_snp",\"homozyg-window-het\":"$homozyg_window_het",\"homozyg-window-missing\":"$homozyg_window_missing",\"homozyg-window-threshold\":"$homozyg_window_threshold"}}]}]}")"
else
   roh_json_data_model_opencga="$(echo "{\"roh\":[{\"methods\":[{\"software\":\"BCFTOOLS\",\"version\":\""$bcftools_version"\",\"params\":{\"FILTER\": \"$vcf_filter\",\"FMT/GQ\":$gq}},{\"software\":\"PLINK\",\"version\":\""$plink_version"\",\"params\":{\"chr\":$chr,\"homozyg\":\"true\",\"homozyg-snp\":"$homozyg_snp",\"homozyg-kb\":"$homozyg_kb",\"homozyg-density\":"$homozyg_density",\"homozyg-gap\":"$homozyg_gap",\"homozyg-window-snp\":"$homozyg_window_snp",\"homozyg-window-het\":"$homozyg_window_het",\"homozyg-window-missing\":"$homozyg_window_missing",\"homozyg-window-threshold\":"$homozyg_window_threshold"}}]}]}")"
fi
echo "$roh_json_data_model_opencga" > $roh_json_data_model_opencga_file

# 5- Executing python script to finish completing the OpenCGA ROH analysis json data model with the ROHs results and generating plots for visualising the ROHs results.
roh_script=$roh_python_script_folder"/roh_opencgadatamodel_visualisation.py"
python $roh_script -i $full_directory_all_results -p $files_prefix -j "$roh_json_data_model_opencga" -o $full_directory_all_results
