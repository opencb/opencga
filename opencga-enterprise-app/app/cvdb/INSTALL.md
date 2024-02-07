
Finally, upload the OpenCGA Enterprise Solr config sets that are located at `opencga/build/misc/solr`.
In order to upload all of them, you need to execute the following commands:

```
$ ./bin/solr zk upconfig -n opencga-ca-configset-REPLACEME_ENTERPRISE_VERSION -d ~/opencga/build/misc/solr/opencga-ca-configset-REPLACEME_ENTERPRISE_VERSION -z localhost:9983
$ ./bin/solr zk upconfig -n opencga-ci-configset-REPLACEME_ENTERPRISE_VERSION -d ~/opencga/build/misc/solr/opencga-ci-configset-REPLACEME_ENTERPRISE_VERSION -z localhost:9983
$ ./bin/solr zk upconfig -n opencga-cv-configset-REPLACEME_ENTERPRISE_VERSION -d ~/opencga/build/misc/solr/opencga-cv-configset-REPLACEME_ENTERPRISE_VERSION -z localhost:9983
$ ./bin/solr zk upconfig -n opencga-cve-configset-REPLACEME_ENTERPRISE_VERSION -d ~/opencga/build/misc/solr/opencga-cve-configset-REPLACEME_ENTERPRISE_VERSION -z localhost:9983
```
