#!/bin/bash

set -e
set -o pipefail

# Load demo data: create project, study, fetch VCF, index, annotate, stats, secondary index, and export.
# Modeled after opencga-hadoop-test load-test.sh

OPENCGA_HOME=${OPENCGA_HOME:-/opt/opencga}
OWNER_ID=${OPENCGA_OWNER_ID:?Missing OPENCGA_OWNER_ID}
OWNER_PASSWORD=${OPENCGA_OWNER_PASSWORD:?Missing OPENCGA_OWNER_PASSWORD}
ORG_ID=${OPENCGA_ORG_ID:?Missing OPENCGA_ORG_ID}
PROJECT="${OPENCGA_DEMO_PROJECT:-family}"
STUDY="${OPENCGA_DEMO_STUDY:-corpasome}"
STUDY_FQN="${PROJECT}:${STUDY}"

echo "============================================="
echo " OpenCGA Demo Data Load"
echo " Organization: ${ORG_ID}"
echo " User: ${OWNER_ID}"
echo " Project: ${PROJECT}"
echo " Study: ${STUDY}"
echo "============================================="

# Login as the owner user
echo "Logging in as ${OWNER_ID}..."
echo "${OWNER_PASSWORD}" | "${OPENCGA_HOME}/bin/opencga.sh" users login -u "${OWNER_ID}" -p --organization "${ORG_ID}"

# Create project
echo "Creating project ${PROJECT}..."
"${OPENCGA_HOME}/bin/opencga.sh" projects create --id "${PROJECT}" \
    --name "Project ${PROJECT} - GRCh37" \
    --organism-scientific-name 'hsapiens' \
    --organism-assembly 'GRCh37' \
    --cellbase-url https://ws.zettagenomics.com/cellbase/ \
    --cellbase-version v5.8 \
    --cellbase-data-release 1

# Create study
echo "Creating study ${STUDY}..."
"${OPENCGA_HOME}/bin/opencga.sh" studies create --project "${PROJECT}" \
    --id "${STUDY}" \
    --name 'Corpas Family' \
    --description 'This study simulates two disorders and some phenotypes in the Corpas family for training purposes'

# Configure variant storage
echo "Configuring variant storage for ${STUDY_FQN}..."
"${OPENCGA_HOME}/bin/opencga.sh" operations variant-setup --study "${STUDY_FQN}" \
    --expected-samples 4 --expected-files 1 --average-samples-per-file 4 --average-file-size 20MiB

# Create data directory and fetch VCF
echo "Fetching demo VCF file..."
"${OPENCGA_HOME}/bin/opencga.sh" files create --study "${STUDY_FQN}" --path 'data' --type 'DIRECTORY'
"${OPENCGA_HOME}/bin/opencga.sh" files fetch --study "${STUDY_FQN}" --path 'data' \
    --url 'http://resources.opencb.org/datasets/corpasome/data/quartet.variants.annotated.vcf.gz' \
    --job-id "${PROJECT}_download_vcf"

# Index variants
echo "Submitting variant index job..."
"${OPENCGA_HOME}/bin/opencga.sh" operations variant-index --study "${STUDY_FQN}" \
    --file 'quartet.variants.annotated.vcf.gz' \
    --job-id "${PROJECT}_variant_index" \
    --job-depends-on "${PROJECT}_download_vcf"

# Annotation index
echo "Submitting variant annotation job..."
"${OPENCGA_HOME}/bin/opencga.sh" operations variant-annotation-index --project "${PROJECT}" \
    --job-id "${PROJECT}_variant_annotation" \
    --job-depends-on "${PROJECT}_variant_index"

# Stats index
echo "Submitting variant stats job..."
"${OPENCGA_HOME}/bin/opencga.sh" operations variant-stats-index --study "${STUDY_FQN}" --cohort 'ALL' \
    --job-id "${PROJECT}_variant_stats" \
    --job-depends-on "${PROJECT}_variant_index"

# Secondary index
echo "Submitting variant secondary index job..."
"${OPENCGA_HOME}/bin/opencga.sh" operations variant-secondary-index --project "${PROJECT}" \
    --job-id "${PROJECT}_variant_secondary_index" \
    --job-depends-on "${PROJECT}_variant_stats,${PROJECT}_variant_annotation"

# Export: all variants as JSON_SPARSE (reads from variants table)
echo "Submitting export job (all variants, JSON_SPARSE)..."
"${OPENCGA_HOME}/bin/opencga.sh" variant export-run \
    --study "${STUDY_FQN}" \
    --output-file-format JSON_SPARSE \
    --include-sample all \
    --job-id "${PROJECT}_variant_export_all" \
    --job-depends-on "${PROJECT}_variant_secondary_index"

# Export: single sample as VCF (reads from sample-index + variants table)
echo "Submitting export job (sample ISDBM322016, VCF)..."
"${OPENCGA_HOME}/bin/opencga.sh" variant export-run \
    --study "${STUDY_FQN}" \
    --sample ISDBM322016 \
    --output-file-format VCF \
    --job-id "${PROJECT}_variant_export_sample" \
    --job-depends-on "${PROJECT}_variant_secondary_index"

# Export: filtered by cohort stats as AVRO (reads from variants table)
echo "Submitting export job (cohort filter, AVRO)..."
"${OPENCGA_HOME}/bin/opencga.sh" variant export-run \
    --study "${STUDY_FQN}" \
    --include-sample all \
    --output-file-format AVRO \
    --cohort-stats-alt "ALL>0.01" \
    --job-id "${PROJECT}_variant_export_filtered" \
    --job-depends-on "${PROJECT}_variant_secondary_index"

echo "============================================="
echo " Demo data jobs submitted!"
echo " Use 'opencga.sh jobs top' to monitor progress."
echo "============================================="
