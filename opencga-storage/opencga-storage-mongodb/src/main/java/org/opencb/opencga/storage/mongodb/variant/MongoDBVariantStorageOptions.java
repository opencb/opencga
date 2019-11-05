package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.ConfigurationOption;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import java.util.Arrays;

public enum MongoDBVariantStorageOptions implements ConfigurationOption {
    COLLECTION_VARIANTS("collection.variants", "variants"),
    COLLECTION_PROJECT("collection.project",  "project"),
    COLLECTION_STUDIES("collection.studies",  "studies"),
    COLLECTION_FILES("collection.files", "files"),
    COLLECTION_SAMPLES("collection.samples",  "samples"),
    COLLECTION_TASKS("collection.tasks",  "tasks"),
    COLLECTION_COHORTS("collection.cohorts",  "cohorts"),
    COLLECTION_STAGE("collection.stage",  "stage"),
    COLLECTION_ANNOTATION("collection.annotation",  "annot"),
    COLLECTION_TRASH("collection.trash", "trash"),
    BULK_SIZE("bulkSize",  100),
    DEFAULT_GENOTYPE("defaultGenotype", Arrays.asList("0/0", "0|0")),
    ALREADY_LOADED_VARIANTS("alreadyLoadedVariants", 0),

    PARALLEL_WRITE("parallel.write", false),

    STAGE("stage", false),
    STAGE_RESUME("stage.resume", false),
    STAGE_PARALLEL_WRITE("stage.parallel.write", false),
    STAGE_CLEAN_WHILE_LOAD("stage.clean.while.load", true),

    DIRECT_LOAD("direct_load", false),
    DIRECT_LOAD_PARALLEL_WRITE("direct_load.parallel.write", false),

    MERGE("merge", false),
    MERGE_SKIP("merge.skip", false), // Internal use only
    MERGE_RESUME("merge.resume", false),
    MERGE_IGNORE_OVERLAPPING_VARIANTS("merge.ignore-overlapping-variants", false),   //Do not look for overlapping variants
    MERGE_PARALLEL_WRITE("merge.parallel.write", false),
    MERGE_BATCH_SIZE("merge.batch.size", 10);          //Number of files to merge directly from first to second collection

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
