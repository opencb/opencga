package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.opencb.opencga.core.config.ConfigurationOption;

public enum HadoopVariantStorageOptions implements ConfigurationOption {


    HADOOP_LOAD_FILES_IN_PARALLEL("storage.hadoop.load.filesInParallel", 1),
    HBASE_NAMESPACE("storage.hadoop.hbase.namespace"),
    EXPECTED_FILES_NUMBER("expected_files_number", 5000),
    EXPECTED_SAMPLES_NUMBER("expected_samples_number"),
    DBADAPTOR_PHOENIX_FETCH_SIZE("storage.hadoop.phoenix.fetchSize", -1),
    DBADAPTOR_PHOENIX_QUERY_COMPLEXITY_THRESHOLD("storage.hadoop.phoenix.queryComplexityThreshold", 250),

    /////////////////////////
    // MapReduce configuration
    /////////////////////////
    /**
     * Hadoop binary to execute MapReduce jobs.
     */
    MR_HADOOP_BIN("storage.hadoop.bin", "hadoop"),
    MR_HADOOP_ENV("storage.hadoop.env"),

    /**
     * Jar with dependencies to run MapReduce jobs.
     */
    MR_JAR_WITH_DEPENDENCIES("storage.hadoop.mr.jarWithDependencies"),
    /**
     * upload HBase jars and jars for any of the configured job classes via the distributed cache (tmpjars).
     */
    MR_ADD_DEPENDENCY_JARS("storage.hadoop.mr.addDependencyJars", true),
    /**
     * Increase the ScannerTimeoutPeriod to avoid ScannerTimeoutExceptions.
     * See opencb/opencga#352 for more info.
     */
    MR_HBASE_SCANNER_TIMEOUT("storage.hadoop.mr.scanner.timeout"),
    /**
     * Overwrite server default (usually 1MB).
     */
    MR_HBASE_KEYVALUE_SIZE_MAX("storage.hadoop.mr.hbase.client.keyvalue.maxsize", 10 * 1024 * 1024), // 10MB
    MR_HBASE_SCAN_CACHING("storage.hadoop.mr.scan.caching", 50),
    MR_HBASE_SCAN_MAX_COLUMNS("storage.hadoop.mr.scan.maxColumns", 25000),
    MR_HBASE_SCAN_MAX_FILTERS("storage.hadoop.mr.scan.maxFilters", 2000),

    /**
     * MapReduce executor. Could be either 'system' or 'ssh'.
     */
    MR_EXECUTOR("storage.hadoop.mr.executor", "system"),

    MR_EXECUTOR_SSH_HOST("storage.hadoop.mr.executor.ssh.host"),
    MR_EXECUTOR_SSH_USER("storage.hadoop.mr.executor.ssh.user"),
    /**
     * Ssh key file path.
     */
    MR_EXECUTOR_SSH_KEY("storage.hadoop.mr.executor.ssh.key"),
    MR_EXECUTOR_SSH_PASSWORD("storage.hadoop.mr.executor.ssh.password"),
    MR_EXECUTOR_SSH_REMOTE_OPENCGA_HOME("storage.hadoop.mr.executor.ssh.remoteOpenCgaHome"),
    MR_EXECUTOR_SSH_HADOOP_SSH_BIN("storage.hadoop.mr.executor.ssh.hadoop-ssh.bin", "misc/scripts/hadoop-ssh.sh"),
    MR_EXECUTOR_SSH_HADOOP_SCP_BIN("storage.hadoop.mr.executor.ssh.hadoop-scp.bin", "misc/scripts/hadoop-scp.sh"),

    /////////////////////////
    // Variant table configuration
    /////////////////////////
    VARIANT_TABLE_COMPRESSION("storage.hadoop.variant.table.compression", Compression.Algorithm.SNAPPY.getName()),
    VARIANT_TABLE_PRESPLIT_SIZE("storage.hadoop.variant.table.preSplit.numSplits", 500),
    // Do not create phoenix indexes. Testing purposes only
    VARIANT_TABLE_INDEXES_SKIP("storage.hadoop.variant.table.indexes.skip"),
    VARIANT_TABLE_LOAD_REFERENCE("storage.hadoop.variant.table.load.reference", false),
    PENDING_SECONDARY_INDEX_TABLE_COMPRESSION("storage.hadoop.pendingSecondaryIndex.table.compression",
            Compression.Algorithm.SNAPPY.getName()),
    PENDING_SECONDARY_INDEX_PRUNE_TABLE_COMPRESSION("storage.hadoop.pendingSecondaryIndexPrune.table.compression",
            Compression.Algorithm.SNAPPY.getName()),


    /////////////////////////
    // Archive table configuration
    /////////////////////////
    ARCHIVE_TABLE_COMPRESSION("storage.hadoop.archive.table.compression", Compression.Algorithm.GZ.getName()),
    ARCHIVE_TABLE_PRESPLIT_SIZE("storage.hadoop.archive.table.preSplit.splitsPerBatch", 500),

    ARCHIVE_CHUNK_SIZE("storage.hadoop.archive.table.chunkSize", 1000),
    ARCHIVE_FILE_BATCH_SIZE("storage.hadoop.archive.table.fileBatchSize", 1000),

    ARCHIVE_SLICE_BUFFER_SIZE("storage.hadoop.archive.sliceBuffer.size", 5),

    ARCHIVE_FIELDS("storage.hadoop.archive.fields"),
    ARCHIVE_NON_REF_FILTER("storage.hadoop.archive.non-ref.filter"),

    /////////////////////////
    // Sample index table configuration
    /////////////////////////
    SAMPLE_INDEX_TABLE_COMPRESSION("storage.hadoop.sampleIndex.table.compression", Compression.Algorithm.SNAPPY.getName()),
    SAMPLE_INDEX_TABLE_PRESPLIT_SIZE("storage.hadoop.sampleIndex.table.preSplit.samplesPerSplit", 15),
    SAMPLE_INDEX_BUILD_MAX_SAMPLES_PER_MR("storage.hadoop.sampleIndex.build.maxSamplesPerMR", 2000),
    SAMPLE_INDEX_ANNOTATION_MAX_SAMPLES_PER_MR("storage.hadoop.sampleIndex.annotation.maxSamplesPerMR", 2000),
    SAMPLE_INDEX_FAMILY_MAX_TRIOS_PER_MR("storage.hadoop.sampleIndex.family.maxTriosPerMR", 1000),

    /////////////////////////
    // Annotation index table  configuration
    /////////////////////////
    ANNOTATION_INDEX_TABLE_COMPRESSION("storage.hadoop.annotationIndex.table.compression", Compression.Algorithm.SNAPPY.getName()),
    PENDING_ANNOTATION_TABLE_COMPRESSION("storage.hadoop.pendingAnnotation.table.compression", Compression.Algorithm.SNAPPY.getName()),

    /////////////////////////
    // Other
    /////////////////////////
    INTERMEDIATE_HDFS_DIRECTORY("storage.hadoop.intermediate.hdfs.directory"),
    FILL_MISSING_WRITE_MAPPERS_LIMIT_FACTOR("storage.hadoop.fill_missing.write.mappers.limit.factor", 1.5F),
    FILL_MISSING_SIMPLIFIED_MULTIALLELIC_VARIANTS("storage.hadoop.fill_missing.simplifiedMultiAllelicVariants", true),
    FILL_MISSING_GAP_GENOTYPE("storage.hadoop.fill_missing.gap_genotype", "0/0"),
    FILL_GAPS_GAP_GENOTYPE("storage.hadoop.fill_gaps.gap_genotype", "0/0"),

    WRITE_MAPPERS_LIMIT_FACTOR("storage.hadoop.write.mappers.limit.factor", 1.5F),

    STATS_LOCAL("storage.hadoop.stats.local", false);

    private final String key;
    private final Object value;

    HadoopVariantStorageOptions(String key) {
        this(key, null);
    }

    HadoopVariantStorageOptions(String key, Object value) {
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
