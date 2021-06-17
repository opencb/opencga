package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.ConfigurationOption;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import java.util.Arrays;

public enum MongoDBVariantStorageOptions implements ConfigurationOption {
    COLLECTION_VARIANTS("storage.mongodb.collection.variants", "variants"),
    COLLECTION_PROJECT("storage.mongodb.collection.project", "project"),
    COLLECTION_STUDIES("storage.mongodb.collection.studies", "studies"),
    COLLECTION_FILES("storage.mongodb.collection.files", "files"),
    COLLECTION_SAMPLES("storage.mongodb.collection.samples", "samples"),
    COLLECTION_TASKS("storage.mongodb.collection.tasks", "tasks"),
    COLLECTION_COHORTS("storage.mongodb.collection.cohorts", "cohorts"),
    COLLECTION_STAGE("storage.mongodb.collection.stage", "stage"),
    COLLECTION_ANNOTATION("storage.mongodb.collection.annotation", "annot"),
    COLLECTION_TRASH("storage.mongodb.collection.trash", "trash"),

    ALREADY_LOADED_VARIANTS("storage.mongodb.alreadyLoadedVariants", 0),

    PARALLEL_WRITE("storage.mongodb.parallelWrite", false),

    STAGE("storage.mongodb.stage", false),
    STAGE_RESUME("storage.mongodb.stage.resume", false),
    STAGE_PARALLEL_WRITE("storage.mongodb.stage.parallelWrite", false),
    STAGE_CLEAN_WHILE_LOAD("storage.mongodb.stage.clean.while.load", true),

    DIRECT_LOAD("storage.mongodb.directLoad", false),
    DIRECT_LOAD_PARALLEL_WRITE("storage.mongodb.directLoad.parallelWrite", false),

    MERGE("storage.mongodb.merge", false),
    MERGE_SKIP("storage.mongodb.merge.skip", false), // Internal use only
    MERGE_RESUME("storage.mongodb.merge.resume", false),
    MERGE_IGNORE_OVERLAPPING_VARIANTS("storage.mongodb.merge.ignoreOverlappingVariants", false),   //Do not look for overlapping variants
    MERGE_PARALLEL_WRITE("storage.mongodb.merge.parallelWrite", false),
    MERGE_BATCH_SIZE("storage.mongodb.merge.batchSize", 10),          //Number of files to merge directly from first to second collection


    EXTRA_GENOTYPE_FIELDS_COMPRESS("extra-fields.compress", true),    //Compress with gzip other sample information
    DEFAULT_GENOTYPE("defaultGenotype", Arrays.asList("0/0", "0|0"));

    private final String key;
    private final Object value;

    MongoDBVariantStorageOptions(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public static boolean isResume(ObjectMap options) {
        return options.getBoolean(VariantStorageOptions.RESUME.key(), VariantStorageOptions.RESUME.defaultValue());
    }

    public static boolean isResumeStage(ObjectMap options) {
        return isResume(options) || options.getBoolean(STAGE_RESUME.key(), false);
    }

    public static boolean isResumeMerge(ObjectMap options) {
        return isResume(options) || options.getBoolean(MERGE_RESUME.key(), false);
    }

    public static boolean isDirectLoadParallelWrite(ObjectMap options) {
        return isParallelWrite(DIRECT_LOAD_PARALLEL_WRITE, options);
    }

    public static boolean isStageParallelWrite(ObjectMap options) {
        return isParallelWrite(STAGE_PARALLEL_WRITE, options);
    }

    public static boolean isMergeParallelWrite(ObjectMap options) {
        return isParallelWrite(MERGE_PARALLEL_WRITE, options);
    }

    private static boolean isParallelWrite(MongoDBVariantStorageOptions option, ObjectMap options) {
        return options.getBoolean(PARALLEL_WRITE.key(), PARALLEL_WRITE.defaultValue())
                || options.getBoolean(option.key(), option.defaultValue());
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T defaultValue() {
        return (T) value;
    }
}
