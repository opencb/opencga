# Provide Solr configuration manually

To install Solr config you need to execute:

```
./bin/solr zk upconfig -n CONFIGSET-VERSION -d server/solr/configsets/CONFIGSET-VERSION -z localhost:9983

./bin/solr zk upconfig -n CONFIGSETFile-VERSION -d server/solr/configsets/CONFIGSETFile-VERSION -z localhost:9983
./bin/solr zk upconfig -n CONFIGSETSample-VERSION -d server/solr/configsets/CONFIGSETSample-VERSION -z localhost:9983

```