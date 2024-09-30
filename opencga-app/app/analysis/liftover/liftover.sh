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
TARGET_ASSEMBLY=$2
OUTPUT_FILE=$3

if [ -z "$INPUT_FILE" ] || [ -z "$TARGET_ASSEMBLY" ] || [ -z "$OUTPUT_FILE" ]; then
    echo "Usage: $0 <source_assembly> <target_assembly>"
    exit 1
fi

if [ $TARGET_ASSEMBLY == "GRCh38" ]; then
    echo "Liftover from GRCh37 to $TARGET_ASSEMBLY"

    ## Prepare GRCh37 and GRCh38 reference genomes
    if [ ! -f Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz ]; then
      wget --no-check-certificate https://resources.opencb.org/opencb/opencga/analysis/commons/reference-genomes/Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz
    fi

    if [ ! -f Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz ]; then
      wget --no-check-certificate https://resources.opencb.org/opencb/opencga/analysis/commons/reference-genomes/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
    fi

    gunzip Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz
    SOURCE_REFERENCE_FILE="Homo_sapiens.GRCh37.dna.primary_assembly.fa"

    gunzip Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
    TARGET_REFERENCE_FILE="Homo_sapiens.GRCh38.dna.primary_assembly.fa"

    wget http://ftp.ensembl.org/pub/assembly_mapping/homo_sapiens/GRCh37_to_GRCh38.chain.gz
    CHAIN_FILE="GRCh37_to_GRCh38.chain.gz"
elif [ $TARGET_ASSEMBLY == "hg38" ]; then
    echo "Liftover from hg19 to $TARGET_ASSEMBLY"

    ## Prepare hg19 and hg38 reference genomes
    if [ ! -f hg19.fa.gz ]; then
      wget https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz
    fi

    if [ ! -f hg38.fa.gz ]; then
      wget https://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/hg38.fa.gz
    fi

    gunzip hg19.fa.gz
    SOURCE_REFERENCE_FILE="hg19.fa"

    gunzip hg38.fa.gz
    TARGET_REFERENCE_FILE="hg38.fa"

    wget http://hgdownload.cse.ucsc.edu/goldenpath/hg19/liftOver/hg19ToHg38.over.chain.gz
    CHAIN_FILE="hg19ToHg38.over.chain.gz"
else
    echo "Unsupported target assembly $TARGET_ASSEMBLY"
    exit 1
fi

## Execute bcftools
bcftools +liftover --no-version -Oz $INPUT_FILE -- -s $SOURCE_REFERENCE_FILE -f $TARGET_REFERENCE_FILE -c $CHAIN_FILE --reject rejected.vcf --reject-type v --write-reject --write-src > $OUTPUT_FILE

## Clean folders
rm *.fai
rm *.chain.gz