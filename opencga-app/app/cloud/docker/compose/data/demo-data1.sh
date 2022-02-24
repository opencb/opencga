#!/bin/bash

echo "Login user demo ...."
echo "demoOpencga2021." | /opt/opencga/bin/opencga.sh users login -u demo

echo "Creating demo@family:corpasome ...."
/opt/opencga/bin/opencga.sh projects create --id 'family' --name 'Family Studies GRCh37'  \
    --organism-scientific-name 'homo sapiens' \
    --organism-assembly 'GRCh37'
/opt/opencga/bin/opencga.sh studies create --project 'demo@family' --name 'Corpas Family' --id 'corpasome' \
    --description 'This study simulates two disorders and some phenotypes in the Corpas family for training purposes'
/opt/opencga/bin/opencga.sh files create --study 'demo@family:corpasome' --path 'data'
/opt/opencga/bin/opencga.sh files fetch --study 'demo@family:corpasome' --path 'data' --url 'http://resources.opencb.org/datasets/corpasome/data/quartet.variants.annotated.vcf.gz' \
    --job-id 'download_quartet.variants.annotated.vcf.gz'
/opt/opencga/bin/opencga.sh operations variant-index --file 'quartet.variants.annotated.vcf.gz' --family \
     --job-id 'variant_index' --job-depends-on 'download_quartet.variants.annotated.vcf.gz'
/opt/opencga/bin/opencga.sh operations variant-stats-index --study 'demo@family:corpasome' --cohort 'ALL' \
     --job-id 'variant_stats' --job-depends-on 'variant_index'
/opt/opencga/bin/opencga.sh operations variant-annotation-index --project 'demo@family' \
     --job-id 'variant_annotation' --job-depends-on 'variant_index'
/opt/opencga/bin/opencga.sh operations variant-secondary-index --project 'demo@family' \
     --job-id 'variant_secondary_index' --job-depends-on 'variant_stats,variant_annotation'

TEMPLATE=$(/opt/opencga/bin/opencga.sh studies template-upload -i /opt/opencga/misc/demo/corpasome/ --study 'demo@family:corpasome')
/opt/opencga/bin/opencga.sh studies template-run --id "$TEMPLATE" --study 'demo@family:corpasome' --overwrite
