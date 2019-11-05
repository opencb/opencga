package org.opencb.opencga.storage.core.variant;

import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.storage.core.config.ConfigurationOption;

public enum VariantStorageOptions implements ConfigurationOption {
    INCLUDE_STATS("include.stats", true),              //Include existing stats on the original file.
    //        @Deprecated
//        INCLUDE_GENOTYPES("include.genotypes", true),      //Include sample information (genotypes)
    EXTRA_GENOTYPE_FIELDS("include.extra-fields", ""),  //Include other sample information (like DP, GQ, ...)
    EXTRA_GENOTYPE_FIELDS_TYPE("include.extra-fields-format", ""),  //Other sample information format (String, Integer, Float)
    EXTRA_GENOTYPE_FIELDS_COMPRESS("extra-fields.compress", true),    //Compress with gzip other sample information
    //        @Deprecated
//        INCLUDE_SRC("include.src", false),                  //Include original source file on the transformed file and the final db
//        COMPRESS_GENOTYPES ("compressGenotypes", true),    //Stores sample information as compressed genotypes
    EXCLUDE_GENOTYPES("exclude.genotypes", false),              //Do not store genotypes from samples

    STUDY_TYPE("studyType", SampleSetType.CASE_CONTROL),
    AGGREGATED_TYPE("aggregatedType", Aggregation.NONE),
    STUDY("study", null),
    OVERRIDE_FILE_ID("overrideFileId", false),
    GVCF("gvcf", false),
    ISOLATE_FILE_FROM_STUDY_CONFIGURATION("isolateStudyConfiguration", false),
    TRANSFORM_FAIL_ON_MALFORMED_VARIANT("transform.fail.on.malformed", false),

    COMPRESS_METHOD("compressMethod", "gzip"),
    AGGREGATION_MAPPING_PROPERTIES("aggregationMappingFile", null),
    @Deprecated
    DB_NAME("database.name", "opencga"),

    STDIN("stdin", false),
    STDOUT("stdout", false),
    TRANSFORM_BATCH_SIZE("transform.batch.size", 200),
    TRANSFORM_THREADS("transform.threads", 4),
    TRANSFORM_FORMAT("transform.format", "avro"),
    LOAD_BATCH_SIZE("load.batch.size", 100),
    LOAD_THREADS("load.threads", 6),
    LOAD_SPLIT_DATA("load.split-data", false),

    LOADED_GENOTYPES("loadedGenotypes", null),

    POST_LOAD_CHECK_SKIP("postLoad.check.skip", false),

    RELEASE("release", 1),

    MERGE_MODE("merge.mode", VariantStorageEngine.MergeMode.ADVANCED),

    CALCULATE_STATS("calculateStats", false),          //Calculate stats on the postLoad step
    OVERWRITE_STATS("overwriteStats", false),          //Overwrite stats already present
    UPDATE_STATS("updateStats", false),                //Calculate missing stats
    STATS_DEFAULT_GENOTYPE("stats.default-genotype", "0/0"), // Default genotype to be used for calculating stats.
    STATS_MULTI_ALLELIC("stats.multiallelic", false),  // Include secondary alternates in the variant stats calculation

    ANNOTATE("annotate", false),
    INDEX_SEARCH("indexSearch", false),

    RESUME("resume", false),
    FORCE("force", false),

    SEARCH_INDEX_LAST_TIMESTAMP("search.index.last.timestamp", 0),

    DEFAULT_TIMEOUT("dbadaptor.default_timeout", 10000), // Default timeout for DBAdaptor operations. Only used if none is provided.
    MAX_TIMEOUT("dbadaptor.max_timeout", 30000),         // Max allowed timeout for DBAdaptor operations
    LIMIT_DEFAULT("limit.default", 1000),
    LIMIT_MAX("limit.max", 5000),
    SAMPLE_LIMIT_DEFAULT("sample.limit.default", 100),
    SAMPLE_LIMIT_MAX("sample.limit.max", 1000),

    // Intersect options
    INTERSECT_ACTIVE("search.intersect.active", true),                       // Allow intersect queries with the SearchEngine (Solr)
    INTERSECT_ALWAYS("search.intersect.always", false),                      // Force intersect queries
    INTERSECT_PARAMS_THRESHOLD("search.intersect.params.threshold", 3),      // Minimum number of QueryParams in the query to intersect

    APPROXIMATE_COUNT_SAMPLING_SIZE("approximateCountSamplingSize", 1000),
    APPROXIMATE_COUNT("approximateCount", false);

    private final String key;
    private final Object value;

    VariantStorageOptions(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public String key() {
        return key;
    }

    @SuppressWarnings("unchecked")
    public <T> T defaultValue() {
        return (T) value;
    }

}
