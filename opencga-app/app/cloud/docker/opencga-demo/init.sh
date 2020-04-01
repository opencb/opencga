#!/bin/bash

mongod --dbpath /data/opencga/mongodb --replSet rs0  &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start mongoDB: $status"
  exit $status
fi
sleep 10

mongo /opt/scripts/mongo-cluster-init.js
sleep 20

/opt/solr-*/bin/solr start -force &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start Solr: $status"
  exit $status
fi

sleep 2

CONTAINER_ALREADY_STARTED="CONTAINER_ALREADY_STARTED"
if [ ! -e $CONTAINER_ALREADY_STARTED ] && [ "$installCatalog" != "false" ]; then
    echo "-- Installing Catalog --"
    /opt/opencga/bin/opencga-admin.sh catalog install --secret-key any_string_you_want  <<< demo
    status=$?
        if [ $status -ne 0 ]; then
          echo "Failed to install Catalog : $status"
          exit $status
        fi
    touch $CONTAINER_ALREADY_STARTED
    sleep 5
    echo 'demo' | /opt/opencga/bin/opencga-admin.sh server rest --start &
    status=$?
    if [ $status -ne 0 ]; then
      echo "Failed to start REST server: $status"
      exit $status
    fi

    if [ "$load" == "true" ]; then
        echo Creating user for OpenCGA Catalog .....
        ./opencga-admin.sh users create -u demo --email demo@opencb.com --name "Demo User" --user-password demo <<< demo
        echo Login user demo ....
        ./opencga.sh users login -u demo <<< demo
        echo Creating Project ....
        ./opencga.sh projects create --id "exomes_grch37" -n "Exomes GRCh37" --organism-scientific-name "Homo sapiens" --organism-assembly "GRCh37"
        echo Creating Study ....
        ./opencga.sh studies create -n "corpasome Genomes Project" --project "exomes_grch37" --id "corpasome"
        sessionId=$(grep token ~/.opencga/session.json | cut -d '"' -f 4)
	echo Creating Individuals ....
        curl -X POST --header "Content-Type: application/json" --header "Accept: application/json" --header "Authorization: Bearer $sessionId" -d "{
	  \"id\": \"ISDBM322016\",
	  \"name\": \"ISDBM322016\",
	  \"sex\": \"MALE\",
	  \"parentalConsanguinity\": false,
	  \"karyotypicSex\": \"XY\",
	  \"lifeStatus\": \"ALIVE\",
	  \"samples\": [
	    {
	      \"id\": \"ISDBM322016\"
	    }
	  ]
	}" "http://localhost:9090/opencga/webservices/rest/v1/individuals/create?study=corpasome"

	curl -X POST --header "Content-Type: application/json" --header "Accept: application/json" --header "Authorization: Bearer $sessionId" -d "{
          \"id\": \"ISDBM322018\",
          \"name\": \"ISDBM322018\",
          \"sex\": \"FEMALE\",
          \"parentalConsanguinity\": false,
          \"karyotypicSex\": \"XX\",
          \"lifeStatus\": \"ALIVE\",
          \"samples\": [
            {
              \"id\": \"ISDBM322018\"
            }
          ]
        }" "http://localhost:9090/opencga/webservices/rest/v1/individuals/create?study=corpasome"

	curl -X POST --header "Content-Type: application/json" --header "Accept: application/json" --header "Authorization: Bearer $sessionId" -d "{
	  \"id\": \"ISDBM322015\",
	  \"name\": \"ISDBM322015\",
	  \"sex\": \"MALE\",
	  \"mother\": \"ISDBM322018\",
	  \"father\": \"ISDBM322016\",
	  \"parentalConsanguinity\": false,
	  \"karyotypicSex\": \"XY\",
	  \"lifeStatus\": \"ALIVE\",
	  \"samples\": [
	    {
	      \"id\": \"ISDBM322015\"
	    }
	  ]
	}" "http://localhost:9090/opencga/webservices/rest/v1/individuals/create?study=corpasome"
 
	curl -X POST --header "Content-Type: application/json" --header "Accept: application/json" --header "Authorization: Bearer $sessionId" -d "{
 	 \"id\": \"ISDBM322017\",
	  \"name\": \"ISDBM322017\",
	  \"sex\": \"FEMALE\",
	  \"mother\": \"ISDBM322018\",
	  \"father\": \"ISDBM322016\",
	  \"parentalConsanguinity\": false,
	  \"karyotypicSex\": \"XX\",
	  \"lifeStatus\": \"ALIVE\",
	  \"samples\": [
	    {
	      \"id\": \"ISDBM322017\"
	    }
	  ]
	}" "http://localhost:9090/opencga/webservices/rest/v1/individuals/create?study=corpasome"

	curl -X POST --header "Content-Type: application/json" --header "Accept: application/json" --header "Authorization: Bearer $sessionId" -d "{
	  \"id\": \"corpas\",
	  \"name\": \"Corpas\",
	  \"members\": [
	    {
	      \"id\": \"ISDBM322015\"
	    },{
	      \"id\": \"ISDBM322016\"
	    },{
	      \"id\": \"ISDBM322017\"
	    },{
	      \"id\": \"ISDBM322018\"
	    }
	  ],
	  \"expectedSize\": 5
	}" "http://localhost:9090/opencga/webservices/rest/v1/families/create?study=corpasome"

	echo Download and link Variant File
        wget  -O /opt/opencga/variants/quartet.variants.annotated.vcf https://ndownloader.figshare.com/files/3083423
        ./opencga.sh files link -i ../variants/quartet.variants.annotated.vcf -s corpasome
        echo Transforming, Loading, Annotating and Calculating Stats
        ./opencga.sh variant index --file quartet.variants.annotated.vcf --calculate-stats --annotate --index-search -o outDir -s "corpasome"
fi
else
    echo 'demo' | /opt/opencga/bin/opencga-admin.sh server rest --start &
fi

./opencga-admin.sh catalog daemon --start <<< demo

