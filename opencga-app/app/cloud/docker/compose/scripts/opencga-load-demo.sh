#!/bin/bash

set -e
set -o pipefail

# Load demo data: create project, study, fetch VCF, index, annotate, stats, secondary index, and export.
# Modeled after opencga-hadoop-test load-test.sh

usage() {
    cat <<'EOF'
Usage: opencga-load-demo.sh [OPTIONS]

Load demo data into OpenCGA: create project and study, fetch a VCF, and submit
jobs for indexing, annotation, stats, secondary index, and export.

Steps performed:
  1. Login as owner user
  2. Create project (GRCh37, CellBase v5.8)
  3. Create study (Corpas Family)
  4. Configure variant storage
  5. Fetch demo VCF (quartet.variants.annotated.vcf.gz)
  6. Submit variant index job
  7. Submit variant annotation job
  8. Submit variant stats job
  9. Submit variant secondary index job
  10. Submit export jobs (JSON_SPARSE, VCF, AVRO)
  11. Upload and run study template (if --template provided)

Options (override environment variables):
  --opencga-home DIR        OpenCGA installation directory (env: OPENCGA_HOME, default: /opt/opencga)
  --owner-password PASS     Owner user password (env: OPENCGA_OWNER_PASSWORD) [required]
  --org-id ID               Organization identifier (env: OPENCGA_ORG_ID, default: test)
  --owner-id ID             Owner user identifier (env: OPENCGA_OWNER_ID, default: test-user)
  --project ID              Project identifier (env: OPENCGA_DEMO_PROJECT, default: family)
  --study ID                Study identifier (env: OPENCGA_DEMO_STUDY, default: corpasome)
  --template DIR            Template directory to upload and run (env: OPENCGA_DEMO_TEMPLATE)
  --host URL                REST host URL (exports OPENCGA_CLIENT_REST_HOST)
  -h, --help                Show this help message
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)          usage; exit 0 ;;
        --opencga-home)     OPENCGA_HOME="$2"; shift 2 ;;
        --host)             export OPENCGA_CLIENT_REST_HOST="$2"; shift 2 ;;
        --org-id)           OPENCGA_ORG_ID="$2"; shift 2 ;;
        --owner-id)         OPENCGA_OWNER_ID="$2"; shift 2 ;;
        --owner-password)   OPENCGA_OWNER_PASSWORD="$2"; shift 2 ;;
        --project)          OPENCGA_DEMO_PROJECT="$2"; shift 2 ;;
        --study)            OPENCGA_DEMO_STUDY="$2"; shift 2 ;;
        --template)         OPENCGA_DEMO_TEMPLATE="$2"; shift 2 ;;
        *)                  echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

# Run a command, tolerating "already exists" errors for idempotency.
# Usage: run_idempotent "description" command [args...]
run_idempotent() {
    local desc="$1"; shift
    echo "${desc}..."
    output=$("$@" 2>&1) && rc=0 || rc=$?
    if [ "$rc" -eq 0 ]; then
        echo "${desc}... done."
    elif echo "$output" | grep -qiE "already exists|already has"; then
        echo "${desc}... already done, skipping."
    else
        echo "$output" >&2
        exit "$rc"
    fi
}

OPENCGA_HOME=${OPENCGA_HOME:-/opt/opencga}
OWNER_ID=${OPENCGA_OWNER_ID:-test-user}
OWNER_PASSWORD=${OPENCGA_OWNER_PASSWORD:?Missing --owner-password or OPENCGA_OWNER_PASSWORD}
ORG_ID=${OPENCGA_ORG_ID:-test}
PROJECT="${OPENCGA_DEMO_PROJECT:-family}"
STUDY="${OPENCGA_DEMO_STUDY:-corpasome}"
TEMPLATE="${OPENCGA_DEMO_TEMPLATE:-}"
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
run_idempotent "Creating project '${PROJECT}'" \
    "${OPENCGA_HOME}/bin/opencga.sh" projects create --id "${PROJECT}" \
    --name "Project ${PROJECT} - GRCh37" \
    --organism-scientific-name 'hsapiens' \
    --organism-assembly 'GRCh37' \
    --cellbase-url https://ws.zettagenomics.com/cellbase/ \
    --cellbase-version v5.8 \
    --cellbase-data-release 1

# Create study
run_idempotent "Creating study '${STUDY}'" \
    "${OPENCGA_HOME}/bin/opencga.sh" studies create --project "${PROJECT}" \
    --id "${STUDY}" \
    --name 'Corpas Family' \
    --description 'This study simulates two disorders and some phenotypes in the Corpas family for training purposes'

# Configure variant storage
run_idempotent "Configuring variant storage for ${STUDY_FQN}" \
    "${OPENCGA_HOME}/bin/opencga.sh" operations variant-setup --study "${STUDY_FQN}" \
    --expected-samples 4 --expected-files 1 --average-samples-per-file 4 --average-file-size 20MiB

# Create data directory
run_idempotent "Creating directory 'data'" \
    "${OPENCGA_HOME}/bin/opencga.sh" files create --study "${STUDY_FQN}" --path 'data' --type 'DIRECTORY'

# Fetch VCF
run_idempotent "Fetching demo VCF file" \
    "${OPENCGA_HOME}/bin/opencga.sh" files fetch --study "${STUDY_FQN}" --path 'data' \
    --url 'http://resources.opencb.org/datasets/corpasome/data/quartet.variants.annotated.vcf.gz' \
    --job-id "${PROJECT}_download_vcf"

# Index variants
run_idempotent "Submitting variant index job" \
    "${OPENCGA_HOME}/bin/opencga.sh" operations variant-index --study "${STUDY_FQN}" \
    --file 'quartet.variants.annotated.vcf.gz' \
    --job-id "${PROJECT}_variant_index" \
    --job-depends-on "${PROJECT}_download_vcf"

# Annotation index
run_idempotent "Submitting variant annotation job" \
    "${OPENCGA_HOME}/bin/opencga.sh" operations variant-annotation-index --project "${PROJECT}" \
    --job-id "${PROJECT}_variant_annotation" \
    --job-depends-on "${PROJECT}_variant_index"

# Stats index
run_idempotent "Submitting variant stats job" \
    "${OPENCGA_HOME}/bin/opencga.sh" operations variant-stats-index --study "${STUDY_FQN}" --cohort 'ALL' \
    --job-id "${PROJECT}_variant_stats" \
    --job-depends-on "${PROJECT}_variant_index"

# Secondary index
run_idempotent "Submitting variant secondary index job" \
    "${OPENCGA_HOME}/bin/opencga.sh" operations variant-secondary-index --project "${PROJECT}" \
    --job-id "${PROJECT}_variant_secondary_index" \
    --job-depends-on "${PROJECT}_variant_stats,${PROJECT}_variant_annotation"

# Export: all variants as JSON_SPARSE
run_idempotent "Submitting export job (all variants, JSON_SPARSE)" \
    "${OPENCGA_HOME}/bin/opencga.sh" variant export-run \
    --study "${STUDY_FQN}" \
    --output-file-format JSON_SPARSE \
    --include-sample all \
    --job-id "${PROJECT}_variant_export_all" \
    --job-depends-on "${PROJECT}_variant_secondary_index"

# Export: single sample as VCF
run_idempotent "Submitting export job (sample ISDBM322016, VCF)" \
    "${OPENCGA_HOME}/bin/opencga.sh" variant export-run \
    --study "${STUDY_FQN}" \
    --sample ISDBM322016 \
    --output-file-format VCF \
    --job-id "${PROJECT}_variant_export_sample" \
    --job-depends-on "${PROJECT}_variant_secondary_index"

# Export: filtered by cohort stats as AVRO
run_idempotent "Submitting export job (cohort filter, AVRO)" \
    "${OPENCGA_HOME}/bin/opencga.sh" variant export-run \
    --study "${STUDY_FQN}" \
    --include-sample all \
    --output-file-format AVRO \
    --cohort-stats-alt "ALL>0.01" \
    --job-id "${PROJECT}_variant_export_filtered" \
    --job-depends-on "${PROJECT}_variant_secondary_index"

# Upload and run template (if provided)
if [ -n "$TEMPLATE" ]; then
    echo "Uploading template from ${TEMPLATE}..."
    TEMPLATE_ID=$("${OPENCGA_HOME}/bin/opencga.sh" studies templates-upload -i "$TEMPLATE" --study "${STUDY_FQN}")
    echo "Running template '${TEMPLATE_ID}'..."
    "${OPENCGA_HOME}/bin/opencga.sh" studies templates-run --id "$TEMPLATE_ID" --study "${STUDY_FQN}" --overwrite
fi

echo "============================================="
echo " Demo data jobs submitted!"
echo " Use 'opencga.sh jobs top' to monitor progress."
echo "============================================="
