#
# Copyright 2015-2020 OpenCB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#/bin/bash

INPUT_FILE=$1
TARGET_ASSEMBLY=$2  ## Values accepted are: GRCh38, hg38
OUTPUT_DIR=$3
LOCAL_RESOURCES_DIR=$4

if [ -z "$INPUT_FILE" ] || [ -z "$TARGET_ASSEMBLY" ] || [ -z "$OUTPUT_DIR" ] || [ -z "$LOCAL_RESOURCES_DIR" ]; then
    echo "Usage: $0 <vcf_file> <target_assembly> <output_dir> <resources_dir>"
    exit 1
fi

# Check if the file ends with .vcf.gz or .vcf
if [[ "$INPUT_FILE" == *.vcf.gz ]]; then
    basename=$(basename "$INPUT_FILE" .vcf.gz)
elif [[ "$INPUT_FILE" == *.vcf ]]; then
    basename=$(basename "$INPUT_FILE" .vcf)
else
    echo "File extension not recognized ($INPUT_FILE): only .vcf or vcf.gz are valid"
    exit 1
fi


if [ $TARGET_ASSEMBLY == "GRCh38" ]; then
    echo "Liftover from GRCh37 to $TARGET_ASSEMBLY"

    ## Prepare GRCh37 and GRCh38 reference genomes
#    if [ ! -f "${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz" ]; then
#      wget --no-check-certificate https://resources.opencb.org/opencb/opencga/analysis/commons/reference-genomes/Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz
#    fi
#
#    if [ ! -f Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz ]; then
#      wget --no-check-certificate https://resources.opencb.org/opencb/opencga/analysis/commons/reference-genomes/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
#    fi

    echo "gunzip ${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz"
    gunzip ${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz
    SOURCE_REFERENCE_FILE="${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh37.dna.primary_assembly.fa"

    echo "gunzip ${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"
    gunzip ${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
    TARGET_REFERENCE_FILE="${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh38.dna.primary_assembly.fa"

    CHAIN_FILE="${LOCA_RESOURCES_DIR}/GRCh37_to_GRCh38.chain.gz"
    wget http://ftp.ensembl.org/pub/assembly_mapping/homo_sapiens/GRCh37_to_GRCh38.chain.gz -O $CHAIN_FILE
elif [ $TARGET_ASSEMBLY == "hg38" ]; then
    echo "Liftover from hg19 to $TARGET_ASSEMBLY"
#
#    ## Prepare hg19 and hg38 reference genomes
#    if [ ! -f hg19.fa.gz ]; then
#      wget https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz
#    fi
#
#    if [ ! -f hg38.fa.gz ]; then
#      wget https://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/hg38.fa.gz
#    fi
#
    echo "gunzip ${LOCAL_RESOURCES_DIR}/hg19.fa.gz"
    gunzip ${LOCAL_RESOURCES_DIR}/hg19.fa.gz
    SOURCE_REFERENCE_FILE="${LOCAL_RESOURCES_DIR}/hg19.fa"

    echo "gunzip ${LOCAL_RESOURCES_DIR}/hg38.fa.gz"
    gunzip ${LOCAL_RESOURCES_DIR}/hg38.fa.gz
    TARGET_REFERENCE_FILE="${LOCAL_RESOURCES_DIR}/hg38.fa"

    CHAIN_FILE="${LOCAL_RESOURCES_DIR}/hg19ToHg38.over.chain.gz"
    wget http://hgdownload.cse.ucsc.edu/goldenpath/hg19/liftOver/hg19ToHg38.over.chain.gz -O $CHAIN_FILE
else
    echo "Unsupported target assembly $TARGET_ASSEMBLY"
    exit 1
fi

## Execute bcftools
echo "bcftools +liftover --no-version -Oz $INPUT_FILE -- -s $SOURCE_REFERENCE_FILE -f $TARGET_REFERENCE_FILE -c $CHAIN_FILE --reject ${OUTPUT_DIR}/${basename}.${TARGET_ASSEMBLY}.liftover.rejected.vcf --reject-type v --write-reject --write-src > ${OUTPUT_DIR}/${basename}.${TARGET_ASSEMBLY}.liftover.vcf.gz"
bcftools +liftover --no-version -Oz $INPUT_FILE -- -s $SOURCE_REFERENCE_FILE -f $TARGET_REFERENCE_FILE -c $CHAIN_FILE --reject ${OUTPUT_DIR}/${basename}.${TARGET_ASSEMBLY}.liftover.rejected.vcf --reject-type v --write-reject --write-src > ${OUTPUT_DIR}/${basename}.${TARGET_ASSEMBLY}.liftover.vcf.gz

## Resources are cleaned by the Liftover wrapper executor after saving them as attributes in the result
#rm *.fa
#rm *.fai
#rm $CHAIN_FILE