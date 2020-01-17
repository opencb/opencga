# Provide Solr configuration manually

After installing Solr, you need to upload the six config sets used by OpenCGA. This can be done by running the bin/solr script
following the syntax:

```
$ ./bin/solr zk upconfig -n <name for configset> -d <path to directory with configset>
```

After compiling and installing OpenCGA, the six config sets are located at the OpenCGA build directory: opencga/build/misc/solr.
In order to upload all of them, you need to execute the following commands:

```
$ ./bin/solr zk upconfig -n opencga-variant-configset-VERSION -d ~/opencga/build/misc/solr/opencga-variant-configset-VERSION -z localhost:9983
$ ./bin/solr zk upconfig -n opencga-cohort-configset-VERSION -d ~/opencga/build/misc/solr/opencga-cohort-configset-VERSION -z localhost:9983
$ ./bin/solr zk upconfig -n opencga-family-configset-VERSION -d ~/opencga/build/misc/solr/opencga-family-configset-VERSION -z localhost:9983
$ ./bin/solr zk upconfig -n opencga-file-configset-VERSION -d ~/opencga/build/misc/solr/opencga-file-configset-VERSION -z localhost:9983
$ ./bin/solr zk upconfig -n opencga-individual-configset-VERSION -d ~/opencga/build/misc/solr/opencga-individual-configset-VERSION -z localhost:9983
$ ./bin/solr zk upconfig -n opencga-sample-configset-VERSION -d ~/opencga/build/misc/solr/opencga-sample-configset-VERSION -z localhost:9983

```