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

    echo "gunzip ${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz"
    gunzip ${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz
    SOURCE_REFERENCE_FILE="${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh37.dna.primary_assembly.fa"

    echo "gunzip ${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"
    gunzip ${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
    TARGET_REFERENCE_FILE="${LOCAL_RESOURCES_DIR}/Homo_sapiens.GRCh38.dna.primary_assembly.fa"

    CHAIN_FILE="${LOCAL_RESOURCES_DIR}/GRCh37_to_GRCh38.chain.gz"
elif [ $TARGET_ASSEMBLY == "hg38" ]; then
    echo "Liftover from hg19 to $TARGET_ASSEMBLY"

    echo "gunzip ${LOCAL_RESOURCES_DIR}/hg19.fa.gz"
    gunzip ${LOCAL_RESOURCES_DIR}/hg19.fa.gz
    SOURCE_REFERENCE_FILE="${LOCAL_RESOURCES_DIR}/hg19.fa"

    echo "gunzip ${LOCAL_RESOURCES_DIR}/hg38.fa.gz"
    gunzip ${LOCAL_RESOURCES_DIR}/hg38.fa.gz
    TARGET_REFERENCE_FILE="${LOCAL_RESOURCES_DIR}/hg38.fa"

    CHAIN_FILE="${LOCAL_RESOURCES_DIR}/hg19ToHg38.over.chain.gz"
else
    echo "Unsupported target assembly $TARGET_ASSEMBLY"
    exit 1
fi

## Execute bcftools
echo "Running bcftools liftover..."

LIFTOVER_CMD="bcftools +liftover --no-version -Oz ${INPUT_FILE} -- \
  -s ${SOURCE_REFERENCE_FILE} \
  -f ${TARGET_REFERENCE_FILE} \
  -c ${CHAIN_FILE} \
  --reject ${OUTPUT_DIR}/${basename}.${TARGET_ASSEMBLY}.liftover.rejected.vcf \
  --reject-type v \
  --write-reject \
  --write-src > ${OUTPUT_DIR}/${basename}.${TARGET_ASSEMBLY}.liftover.vcf.gz"

echo "$LIFTOVER_CMD"
eval "$LIFTOVER_CMD"

echo "Liftover completed!"

