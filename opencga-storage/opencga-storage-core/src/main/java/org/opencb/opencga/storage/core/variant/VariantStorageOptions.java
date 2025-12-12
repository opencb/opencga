package org.opencb.opencga.storage.core.variant;

import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.config.ConfigurationOption;

import java.util.Arrays;

public enum VariantStorageOptions implements ConfigurationOption {

    STUDY("study"),
    SPECIES("species"),
    ASSEMBLY("assembly"),
    GVCF("gvcf", false),

    RESUME("resume", false), // Resume step.
    FORCE("force", false), // Force execute step.

    STDIN("stdin", false),
    STDOUT("stdout", false),

    TRANSFORM_BATCH_SIZE("transform.batchSize", 200),
    TRANSFORM_THREADS("transform.numThreads", 4),
    TRANSFORM_FORMAT("transform.format", "avro"),
    TRANSFORM_FAIL_ON_MALFORMED_VARIANT("transform.failOnMalformed", true),
    TRANSFORM_COMPRESSION("transform.compression", "gzip"),
    TRANSFORM_ISOLATE("transform.isolate", false), // Do not store file in metadata
    NORMALIZATION_SKIP("normalization.skip", false), // Do not run normalization
    NORMALIZATION_REFERENCE_GENOME("normalization.referenceGenome"),
    NORMALIZATION_EXTENSIONS("normalization.extensions", Arrays.asList("VAF", "SV", "CUSTOM")),

    DEDUPLICATION_POLICY("deduplication.policy", "maxQual"),
    DEDUPLICATION_BUFFER_SIZE("deduplication.bufferSize", 100),

    FAMILY("family", false),
    SOMATIC("somatic", false),

    LOAD_BATCH_SIZE("load.batchSize", 100),
    LOAD_THREADS("load.numThreads", 6),
    LOAD_SPLIT_DATA("load.splitData"),
    LOAD_VIRTUAL_FILE("load.virtualFile"),
    LOAD_MULTI_FILE_DATA("load.multiFileData", false),
    LOAD_SAMPLE_INDEX("load.sampleIndex", YesNoAuto.AUTO),
    LOAD_ARCHIVE("load.archive", YesNoAuto.AUTO),
    LOAD_HOM_REF("load.homRef", YesNoAuto.AUTO),
    POST_LOAD_CHECK("load.postLoadCheck", YesNoAuto.AUTO),

    DELETE_PARALLEL("delete.parallel", false),

    STATS_DEFAULT_GENOTYPE("stats.defaultGenotype", "0/0"), // Default genotype to be used for calculating stats.
    STATS_MULTI_ALLELIC("stats.multiAllelic", false),  // Include secondary alternates in the variant stats calculation
    STATS_CALCULATE("stats.calculate", false),          //Calculate stats on the postLoad step
    STATS_CALCULATE_BATCH_SIZE("stats.calculate.batchSize", 100),
    STATS_CALCULATE_THREADS("stats.calculate.numThreads", 6),
    STATS_LOAD_THREADS("stats.load.numThreads", 4),
    STATS_LOAD_BATCH_SIZE("stats.load.batchSize", 100),
    STATS_OVERWRITE("stats.overwrite", false),          //Overwrite stats already present
    @Deprecated
    STATS_UPDATE("stats.update", false),                //Calculate missing stats
    STATS_AGGREGATION("stats.aggregation.type", Aggregation.NONE),
    STATS_AGGREGATION_MAPPING_FILE("stats.aggregation.mappingFile"),

    ANNOTATE("annotate", false), // Do annotate after step.
    ANNOTATION_CHECKPOINT_SIZE("annotation.checkpointSize", 1000000),
    ANNOTATION_BATCH_SIZE("annotation.batchSize", 100),
    ANNOTATION_FILE_FORMAT("annotation.file.format", "json"),
    ANNOTATION_FILE_DELETE_AFTER_LOAD("annotation.file.deleteAfterLoad", false),
    ANNOTATION_THREADS("annotation.numThreads", 8),
    ANNOTATION_TIMEOUT("annotation.timeout", 600000), // millis,
    ANNOTATION_LOAD_BATCH_SIZE("annotation.load.batchSize", 100),
    ANNOTATION_LOAD_THREADS("annotation.load.numThreads", 4),
    ANNOTATION_OVERWEITE("annotation.overwrite"),
    ANNOTATION_SAMPLE_INDEX("annotation.sampleIndex", YesNoAuto.YES),

    ANNOTATOR("annotator"),
    ANNOTATOR_CLASS("annotator.class"),
    ANNOTATOR_CELLBASE_USE_CACHE("annotator.cellbase.useCache"),
    ANNOTATOR_CELLBASE_INCLUDE("annotator.cellbase.include"),
    ANNOTATOR_CELLBASE_EXCLUDE("annotator.cellbase.exclude"),
    // by default, undefined, no limit
    ANNOTATOR_CELLBASE_VARIANT_LENGTH_THRESHOLD("annotator.cellbase.variantLengthThreshold", Integer.MAX_VALUE),
    ANNOTATOR_CELLBASE_IMPRECISE_VARIANTS("annotator.cellbase.impreciseVariants", true),
    ANNOTATOR_CELLBASE_STAR_ALTERNATE("annotator.cellbase.starAlternate", false),
    ANNOTATOR_EXTENSION_PREFIX("annotator.extension."),
    ANNOTATOR_EXTENSION_LIST("annotator.extension.list"),

    // Cosmic extension parameters
    ANNOTATOR_EXTENSION_COSMIC_FILE("annotator.extension.cosmic.file"),
    ANNOTATOR_EXTENSION_COSMIC_VERSION("annotator.extension.cosmic.version"),
    ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY("annotator.extension.cosmic.assembly"),
    ANNOTATOR_EXTENSION_COSMIC_INDEX_CREATION_DATE("annotator.extension.cosmic.indexCreationDate"),

    // Cosmic extension parameters
    ANNOTATOR_EXTENSION_HGMD_FILE("annotator.extension.hgmd.file"),
    ANNOTATOR_EXTENSION_HGMD_VERSION("annotator.extension.hgmd.version"),
    ANNOTATOR_EXTENSION_HGMD_ASSEMBLY("annotator.extension.hgmd.assembly"),
    ANNOTATOR_EXTENSION_HGMD_INDEX_CREATION_DATE("annotator.extension.hgmd.indexCreationDate"),

    INDEX_SEARCH("indexSearch", false), // Build secondary indexes using search engine.

    METADATA_LOCK_DURATION("metadata.lock.duration", 60000),
    METADATA_LOCK_TIMEOUT("metadata.lock.timeout", 600000),
    METADATA_LOAD_BATCH_SIZE("metadata.load.batchSize", 10),
    METADATA_LOAD_THREADS("metadata.load.numThreads", 4),

    QUERY_DEFAULT_TIMEOUT("query.timeout.default", 10000), // Default timeout for DBAdaptor operations. Only used if none is provided.
    QUERY_MAX_TIMEOUT("query.timeout.max", 30000),         // Max allowed timeout for DBAdaptor operations
    QUERY_LIMIT_DEFAULT("query.limit.default", 1000),
    QUERY_LIMIT_MAX("query.limit.max", 5000),
    QUERY_SAMPLE_LIMIT_DEFAULT("query.sample.limit.default", 100),
    QUERY_SAMPLE_LIMIT_MAX("query.sample.limit.max", 1000),

    WALKER_DOCKER_MEMORY("walker.docker.memory", "1024m", true),
    WALKER_DOCKER_CPU("walker.docker.cpu", "1", true),
    WALKER_DOCKER_USER("walker.docker.user", "", true),
    WALKER_DOCKER_ENV("walker.docker.env", "", true),
    WALKER_DOCKER_MOUNT("walker.docker.mount", "", true),
    WALKER_DOCKER_OPTS("walker.docker.opts", "", true),
    WALKER_DOCKER_MAX_BYTES_PER_MAP("walker.docker.maxBytesPerMap", null, true),

    // Search intersect options
    SEARCH_INTERSECT_ACTIVE("search.intersect.active", true),                  // Allow intersect queries with the SearchEngine (Solr)
    SEARCH_INTERSECT_ALWAYS("search.intersect.always", false),                 // Force intersect queries
    SEARCH_INTERSECT_PARAMS_THRESHOLD("search.intersect.params.threshold", 3), // Minimum number of QueryParams in the query to intersect
    SEARCH_LOAD_BATCH_SIZE("search.load.batchSize", 200),
    // Use blue-green deployment when overwriting an existing collection in the SearchEngine (Solr).
    // In this scenario, data is loaded in a new collection, and when finished, the old collection is deleted.
    // While loading, the old collection is still available for querying.
    SEARCH_LOAD_BLUE_GREEN_ON_OVERWRITE("search.load.blueGreenOnOverwrite", false),
    SEARCH_LOAD_SHARDS_PER_NODE("search.load.shardsPerNode", 2),             // Number of shards to create per solr node
    SEARCH_STATS_FUNCTIONAL_QUERIES_ENABLED("search.stats.functionalQueries.enabled", false),
    SEARCH_STATS_VARIANT_ID_VERSION("search.stats.variantId.version", "v2"),

    APPROXIMATE_COUNT_SAMPLING_SIZE("approximateCountSamplingSize", 1000),
    @Deprecated
    APPROXIMATE_COUNT("approximateCount", false),

    // Do not store genotypes from the current file.
    // Not stored anymore in StudyMetadata
    @Deprecated
    EXCLUDE_GENOTYPES("exclude.genotypes", false),
    INCLUDE_GENOTYPE("include.genotype", YesNoAuto.AUTO),

    /////////////
    // These params are stored in {@link org.opencb.opencga.storage.core.metadata.models.StudyMetadata#getAttributes}.
    // Need migration when renaming them
    /////////////

    LOADED_GENOTYPES("loadedGenotypes", null), // List of loaded genotypes.
    EXTRA_FORMAT_FIELDS("include.extra-fields", ""),  //Include other sample information (like DP, GQ, ...)
    EXTRA_FORMAT_FIELDS_TYPE("include.extra-fields-format", ""),  //Other sample information format (String, Integer, Float)

    RELEASE("release", 1),

    MERGE_MODE("merge.mode", VariantStorageEngine.MergeMode.ADVANCED);

    private final String key;
    private final Object value;
    private final boolean isProtected;

    VariantStorageOptions(String key) {
        this.key = key;
        this.value = null;
        this.isProtected = false;
    }

    VariantStorageOptions(String key, Object value) {
        this.key = key;
        this.value = value;
        this.isProtected = false;
    }

    VariantStorageOptions(String key, Object value, boolean isProtected) {
        this.key = key;
        this.value = value;
        this.isProtected = isProtected;
    }

    public String key() {
        return key;
    }

    @SuppressWarnings("unchecked")
    public <T> T defaultValue() {
        return (T) value;
    }

    @Override
    public boolean isProtected() {
        return isProtected;
    }


}
