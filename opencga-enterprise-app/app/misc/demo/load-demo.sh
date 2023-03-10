#!/bin/sh

ADMIN_PASSWORD=$1
OPENCGA_HOME=${2:-/opt/opencga}
USER_PASSWORD='Demo_P4ss'

cd "$OPENCGA_HOME/bin" || exit 1

set -x

echo "Creating user for OpenCGA Catalog ....."
echo "$ADMIN_PASSWORD" | ./opencga-admin.sh users create -u demo --email demo@opencb.com --name "Demo User" --user-password "$USER_PASSWORD"
echo "Login user demo ...."
echo $USER_PASSWORD | ./opencga.sh users login -u demo -p


echo "Creating demo@family:corpasome ...."
./opencga.sh projects create --id 'family' --name 'Family Studies GRCh37'  \
    --organism-scientific-name 'homo sapiens' \
    --organism-assembly 'GRCh37'
./opencga.sh studies create --project 'demo@family' --name 'Corpas Family' --id 'corpasome' \
    --description 'This study simulates two disorders and some phenotypes in the Corpas family for training purposes'
./opencga.sh files create --study 'demo@family:corpasome' --path 'data' --type 'DIRECTORY'
./opencga.sh files fetch --study 'demo@family:corpasome' --path 'data' --url 'http://resources.opencb.org/datasets/corpasome/data/quartet.variants.annotated.vcf.gz' \
    --job-id 'download_quartet.variants.annotated.vcf.gz'
./opencga.sh operations variant-index --file 'quartet.variants.annotated.vcf.gz' --family true \
     --job-id 'variant_index' --job-depends-on 'download_quartet.variants.annotated.vcf.gz'
./opencga.sh operations variant-stats-index --study 'demo@family:corpasome' --cohort 'ALL' \
     --job-id 'variant_stats' --job-depends-on 'variant_index'
./opencga.sh operations variant-annotation-index --project 'demo@family' \
     --job-id 'variant_annotation' --job-depends-on 'variant_index'
./opencga.sh operations variant-secondary-index --project 'demo@family' \
     --job-id 'variant_secondary_index' --job-depends-on 'variant_stats,variant_annotation'

TEMPLATE=$(./opencga.sh studies templates-upload -i ../misc/demo/corpasome/ --study 'demo@family:corpasome')
./opencga.sh studies templates-run --id "$TEMPLATE" --study 'demo@family:corpasome' --overwrite
