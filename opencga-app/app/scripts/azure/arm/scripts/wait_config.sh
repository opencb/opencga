#!/bin/bash

MAX_ATTEMPTS=${MAX_ATTEMPTS:-300}
for i in $(seq 0 "${MAX_ATTEMPTS}");
do
    if [ -d "/media/primarynfs/conf/" ];
    then
        echo "Configuration is ready";
        sleep 5; # Give the init configuration 5 seconds to run
        break;
    fi;
    echo "Waiting for configuration... [${i}/${MAX_ATTEMPTS}s]" ;
    sleep 1;
done