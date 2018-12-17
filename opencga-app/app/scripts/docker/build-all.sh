#!/bin/bash
cd $(dirname "$0")

for D in *; do
    if [ -d "${D}" ]; then
        docker build -t  "${D}"  "./${D}"  
    fi
done
