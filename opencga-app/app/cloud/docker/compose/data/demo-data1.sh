#!/bin/bash

echo "Login user demo ...."
echo "demoOpencga2021." | /opt/opencga/bin/opencga.sh users login -u demo -p

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
     --job-id 'variant_index' --execution-depends-on 'download_quartet.variants.annotated.vcf.gz'
./opencga.sh operations variant-stats-index --study 'demo@family:corpasome' --cohort 'ALL' \
     --job-id 'variant_stats' --execution-depends-on 'variant_index'
./opencga.sh operations variant-annotation-index --project 'demo@family' \
     --job-id 'variant_annotation' --execution-depends-on 'variant_index'
./opencga.sh operations variant-secondary-index --project 'demo@family' \
     --job-id 'variant_secondary_index' --execution-depends-on 'variant_stats,variant_annotation'

TEMPLATE=$(./opencga.sh studies templates-upload -i /opt/opencga/misc/demo/corpasome/ --study 'demo@family:corpasome')
./opencga.sh studies templates-run --id "$TEMPLATE" --study 'demo@family:corpasome' --overwrite