# Storage Engine Test Coverage

Core storage engine functionality is validated through abstract JUnit tests located in `opencga-storage-core`. Each abstract test that drives behaviour across storage backends is annotated with `@StorageEngineTest`. To avoid missing regressions when implementing a storage backend, we provide coverage tests that ensure every `@StorageEngineTest` has at least one concrete subclass in each backend module.

## Running the coverage checks

To verify MongoDB:

```bash
mvn -pl opencga-storage/opencga-storage-mongodb -Dtest=MongoVariantStorageTestCoverage test
```

To verify Hadoop:

```bash
mvn -pl opencga-storage/opencga-storage-hadoop/opencga-storage-hadoop-core -Dtest=HadoopVariantStorageTestCoverage test
```

To review the Dummy reference implementation (informational only, it will not fail the build):

```bash
mvn -pl opencga-storage/opencga-storage-core -Dtest=DummyVariantStorageTestCoverage test
```

Both tests run fast because they rely on `org.reflections` to scan only the relevant packages (`org.opencb.opencga.storage.mongodb` and `org.opencb.opencga.storage.hadoop`). Failures clearly indicate which core tests still need backend implementations.
