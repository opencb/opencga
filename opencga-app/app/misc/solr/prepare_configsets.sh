#!/bin/bash

if [ $# -eq 2 ]
then
    echo $0 $@
else
    echo "Error: invalid input parameters"
    echo "Usage: $0 <path to the opencga home directory> <opencga version>"
    exit 1
fi

OPENCGA_HOME=$1
VERSION=$2

OPENCGA_BUILD_DIR=$OPENCGA_HOME'/build/'
SOLR_DIR=$OPENCGA_BUILD_DIR'/misc/solr/'

# Change version in INSTALL.md file

sed -i s/VERSION/${VERSION}/g $SOLR_DIR/INSTALL.md

# Iterate over the different config sets

for name in variant cohort family file individual sample; do
    CONFIG_SET_NAME='opencga-'$name'-configset-'$VERSION
    CONFIG_SET_DIR=$SOLR_DIR$CONFIG_SET_NAME

    echo Preparing config set for $name: $CONFIG_SET_NAME

    mkdir $CONFIG_SET_DIR

    # Assuming misc/solr/conf directory exits with the default configuration XML files (except the managed-schema file)

    cp -r $OPENCGA_BUILD_DIR/misc/solr/conf $CONFIG_SET_DIR

    if [ $name == 'variant' ]
    then
        cp -v $OPENCGA_HOME/opencga-storage/opencga-storage-core/target/classes/managed-schema $CONFIG_SET_DIR/conf
    else
        cp -v $OPENCGA_HOME/opencga-catalog/target/classes/solr/$name-managed-schema $CONFIG_SET_DIR/conf/managed-schema
    fi

    # Should we compress each of them in a tar.gz file?

    #tar cfz $SOLR_DIR/$CONFIG_SET_NAME.tar.gz --directory=$SOLR_DIR $CONFIG_SET_NAME
    #rm -rf $CONFIG_SET_DIR
done




