#!/bin/bash

mongod --dbpath /data/opencga/mongodb &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start mongoDB: $status"
  exit $status
fi

sleep 2

/opt/solr-*/bin/solr start -force &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start Solr: $status"
  exit $status
fi

sleep 2

CONTAINER_ALREADY_STARTED="CONTAINER_ALREADY_STARTED"
if [ ! -e $CONTAINER_ALREADY_STARTED ]; then
    touch $CONTAINER_ALREADY_STARTED
    echo "-- Installing Catalog --"
    /opt/opencga/bin/opencga-admin.sh catalog install --secret-key any_string_you_want  <<< demo
    status=$?
        if [ $status -ne 0 ]; then
          echo "Failed to install Catalog : $status"
          exit $status
        fi
    sleep 5
    echo 'demo' | /opt/opencga/bin/opencga-admin.sh server rest --start &
    status=$?
    if [ $status -ne 0 ]; then
      echo "Failed to start REST server: $status"
      exit $status
    fi

    if [ "$load" != "false" ]; then
        echo Creating user for OpenCGA Catalog .....
        ./opencga-admin.sh users create -u demo --email demo@opencb.com --name "Demo User" --user-password demo <<< demo
        echo Login user demo ....
        ./opencga.sh users login -u demo <<< demo
        echo Creating Project ....
        ./opencga.sh projects create -a reference_grch37 -n "Reference studies GRCh37" --organism-scientific-name "Homo sapiens" --organism-assembly "GRCh37" --id "grch37"
        echo Creating Study ....
        ./opencga.sh studies create -a corpasome -n "corpasome Genomes Project" --project "grch37" --id "corpasome"
        echo Download and link Variant File
        wget  -O /opt/opencga/variants/quartet.variants.annotated.vcf https://ndownloader.figshare.com/files/3083423
        ./opencga.sh files link -i ../variants/quartet.variants.annotated.vcf -s corpasome
        echo Transforming, Loading, Annotating and Calculating Stats
        ./opencga.sh variant index --file quartet.variants.annotated.vcf --calculate-stats --annotate --index-search -o outDir -s "corpasome"
    fi
fi
if [ -e $CONTAINER_ALREADY_STARTED ]; then
    echo 'demo' | /opt/opencga/bin/opencga-admin.sh server rest --start &
fi

./opencga-admin.sh catalog daemon --start <<< demo

