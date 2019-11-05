package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.opencb.opencga.storage.core.config.ConfigurationOption;

public enum HadoopVariantStorageEngineOptions implements ConfigurationOption {


    HADOOP_LOAD_BATCH_SIZE("hadoop.load.archive.batch.size", 2),
    HBASE_NAMESPACE("opencga.storage.hadoop.variant.hbase.namespace"),
    EXPECTED_FILES_NUMBER("expected_files_number", 5000),
    INTERMEDIATE_HDFS_DIRECTORY("opencga.storage.hadoop.intermediate.hdfs.directory"),
    FILL_MISSING_WRITE_MAPPERS_LIMIT_FACTOR("fill_missing.write.mappers.limit.factor", 1.5F),
    FILL_MISSING_SIMPLIFIED_MULTIALLELIC_VARIANTS("fill_missing.simplifiedMultiAllelicVariants", true),
    STATS_LOCAL("stats.local", false),
    DBADAPTOR_PHOENIX_FETCH_SIZE("dbadaptor.phoenix.fetch_size", -1),

    /////////////////////////
    // MapReduce configuration
    /////////////////////////
    /**
     * Hadoop binary to execute MapReduce jobs.
     */
    MR_HADOOP_BIN("opencga.hadoop.bin", "hadoop"),
    MR_HADOOP_ENV("opencga.hadoop.env"),

    /**
     * Jar with dependencies to run MapReduce jobs.
     */
    MR_JAR_WITH_DEPENDENCIES("opencga.storage.hadoop.jar-with-dependencies"),
    /**
     * upload HBase jars and jars for any of the configured job classes via the distributed cache (tmpjars).
     */
    MR_ADD_DEPENDENCY_JARS("opencga.mapreduce.addDependencyJars", true),
    /**
     * Increase the ScannerTimeoutPeriod to avoid ScannerTimeoutExceptions.
     * See opencb/opencga#352 for more info.
     */
    MR_HBASE_SCANNER_TIMEOUT("opencga.storage.hadoop.mapreduce.scanner.timeout"),
    /**
     * Overwrite server default (usually 1MB).
     */
    MR_HBASE_KEYVALUE_SIZE_MAX("hadoop.load.variant.hbase.client.keyvalue.maxsize", 10 * 1024 * 1024), // 10MB
    MR_HBASE_SCAN_CACHING("hadoop.load.variant.scan.caching", 50),

    /**
     * MapReduce executor. Could be either 'system' or 'ssh'.
     */
    MR_EXECUTOR("opencga.mr.executor", "system"),

    MR_EXECUTOR_SSH_HOST("opencga.mr.executor.ssh.host"),
    MR_EXECUTOR_SSH_USER("opencga.mr.executor.ssh.user"),
    /**
     * Ssh key file path.
     */
    MR_EXECUTOR_SSH_KEY("opencga.mr.executor.ssh.key"),
    MR_EXECUTOR_SSH_PASSWORD("opencga.mr.executor.ssh.password"),
    MR_EXECUTOR_SSH_REMOTE_OPENCGA_HOME("opencga.mr.executor.ssh.remote_opencga_home"),

    /////////////////////////
    // Variant table configuration
    /////////////////////////
    VARIANT_TABLE_COMPRESSION("opencga.variant.table.compression", Compression.Algorithm.SNAPPY.getName()),
    VARIANT_TABLE_PRESPLIT_SIZE("opencga.variant.table.presplit.size", 100),
    // Do not create phoenix indexes. Testing purposes only
    VARIANT_TABLE_INDEXES_SKIP("opencga.variant.table.indexes.skip"),
    VARIANT_TABLE_LOAD_REFERENCE("opencga.variant.table.load.reference", false),

    /////////////////////////
    // Archive table configuration
    /////////////////////////
    ARCHIVE_TABLE_COMPRESSION("opencga.archive.table.compression", Compression.Algorithm.SNAPPY.getName()),
    ARCHIVE_TABLE_PRESPLIT_SIZE("opencga.archive.table.presplit.size", 100),

    ARCHIVE_CHUNK_SIZE("opencga.archive.chunk_size", 1000),
    ARCHIVE_FILE_BATCH_SIZE("opencga.archive.file_batch_size", 1000),

    ARCHIVE_FIELDS("opencga.archive.fields"),
    ARCHIVE_NON_REF_FILTER("opencga.archive.non-ref.filter"),

    /////////////////////////
    // Sample index table configuration
    /////////////////////////
    SAMPLE_INDEX_TABLE_COMPRESSION("opencga.sample-index.table.compression", Compression.Algorithm.SNAPPY.getName()),
    SAMPLE_INDEX_TABLE_PRESPLIT_SIZE("opencga.sample-index.table.presplit.size", 15),

    /////////////////////////
    // Annotation index table  configuration
    /////////////////////////
    ANNOTATION_INDEX_TABLE_COMPRESSION("opencga.annotation-index.table.compression", Compression.Algorithm.SNAPPY.getName()),
    PENDING_ANNOTATION_TABLE_COMPRESSION("opencga.pending-annotation.table.compression", Compression.Algorithm.SNAPPY.getName());

    private final String key;
    private final Object value;

    HadoopVariantStorageEngineOptions(String key) {
        this(key, null);
    }

    HadoopVariantStorageEngineOptions(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return key;
    }

    public String key() {
        return key;
    }

    @SuppressWarnings("unchecked")
    public <T> T defaultValue() {
        return (T) value;
    }
}
