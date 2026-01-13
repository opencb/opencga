### Provide Solr configuration manually

After installing Solr, you need to upload the six config sets used by OpenCGA. This can be done by running the bin/solr script
following the syntax:

```
$ ./bin/solr zk upconfig -n <name for configset> -d <path to directory with configset>
```

After compiling and installing OpenCGA, the six Solr config sets are located at the OpenCGA build directory: `opencga/build/misc/solr`.
In order to upload all of them, you need to execute the following commands:

```
$ ./bin/solr zk upconfig -n opencga-variant-configset-XYZ -d ~/opencga/build/misc/solr/opencga-variant-configset-XYZ -z localhost:9983
$ ./bin/solr zk upconfig -n opencga-rga-configset-XYZ -d ~/opencga/build/misc/solr/opencga-rga-configset-XYZ -z localhost:9983
```