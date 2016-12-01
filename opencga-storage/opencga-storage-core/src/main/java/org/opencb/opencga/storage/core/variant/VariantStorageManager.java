/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.FileStudyConfigurationManager;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageManager extends StorageManager<VariantDBAdaptor> {

    public enum Options {
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

        STUDY_CONFIGURATION("studyConfiguration", ""),      //
        STUDY_CONFIGURATION_MANAGER_CLASS_NAME("studyConfigurationManagerClassName", ""),

        STUDY_TYPE("studyType", VariantStudy.StudyType.CASE_CONTROL),
        AGGREGATED_TYPE("aggregatedType", VariantSource.Aggregation.NONE),
        STUDY_NAME("studyName", "default"),
        STUDY_ID("studyId", -1),
        FILE_ID("fileId", -1),
        OVERRIDE_FILE_ID("overrideFileId", false),
        SAMPLE_IDS("sampleIds", ""),
        GVCF("gvcf", false),
        ISOLATE_FILE_FROM_STUDY_CONFIGURATION("isolateStudyConfiguration", false),
        TRANSFORM_FAIL_ON_MALFORMED_VARIANT("transform.fail.on.malformed", false),

        COMPRESS_METHOD("compressMethod", "gzip"),
        AGGREGATION_MAPPING_PROPERTIES("aggregationMappingFile", null),
        DB_NAME("database.name", "opencga"),

        TRANSFORM_BATCH_SIZE("transform.batch.size", 200),
        TRANSFORM_THREADS("transform.threads", 4),
        TRANSFORM_FORMAT("transform.format", "avro"),
        LOAD_BATCH_SIZE("load.batch.size", 100),
        LOAD_THREADS("load.threads", 6),
        MERGE_BATCH_SIZE("merge.batch.size", 10),          //Number of files to merge directly from first to second table

        CALCULATE_STATS("calculateStats", false),          //Calculate stats on the postLoad step
        OVERWRITE_STATS("overwriteStats", false),          //Overwrite stats already present
        UPDATE_STATS("updateStats", false),                //Calculate missing stats
        ANNOTATE("annotate", false);

        private final String key;
        private final Object value;

        Options(String key, Object value) {
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

//    protected static Logger logger;

    public VariantStorageManager() {
        logger = LoggerFactory.getLogger(VariantStorageManager.class);
    }

    public VariantStorageManager(StorageConfiguration configuration) {
        super(configuration);
        logger = LoggerFactory.getLogger(VariantStorageManager.class);

    }

    public VariantStorageManager(String storageEngineId, StorageConfiguration configuration) {
        super(storageEngineId, configuration);
        logger = LoggerFactory.getLogger(VariantStorageManager.class);
    }

    @Override
    public List<StorageETLResult> index(List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean doTransform, boolean doLoad)
            throws StorageManagerException {
        List<StorageETLResult> results = super.index(inputFiles, outdirUri, doExtract, doTransform, doLoad);
        if (doLoad) {
            annotateLoadedFiles(outdirUri, inputFiles, results, getOptions());
        }
        return results;
    }

    @Override
    public abstract VariantStorageETL newStorageETL(boolean connected) throws StorageManagerException;

    /**
     * Given a dbName, calculates the annotation for all the variants that matches with a given query, and loads them into the database.
     *
     * @param dbName    database name to annotate.
     * @param query     Query to select variants to annotate
     * @param options   Other options
     * @throws VariantAnnotatorException    If the annotation goes wrong
     * @throws StorageManagerException      If there is any problem related with the StorageManager
     * @throws IOException                  If there is any IO problem
     */
    public void annotate(String dbName, Query query, QueryOptions options)
            throws VariantAnnotatorException, StorageManagerException, IOException {

        VariantAnnotator annotator = VariantAnnotatorFactory.buildVariantAnnotator(configuration, getStorageEngineId(), options);
        try (VariantDBAdaptor dbAdaptor = getDBAdaptor(dbName)) {
            VariantAnnotationManager annotationManager = newVariantAnnotationManager(annotator, dbAdaptor);
            annotationManager.annotate(query, options);
        }
    }

    /**
     * Annotate loaded files. Used only to annotate recently loaded files, after the {@link #index}.
     *
     * @param outdirUri     Index output directory
     * @param files         Indexed files
     * @param results       StorageETLResults
     * @param options       Other options
     * @throws StorageETLException  If there is any problem related with the StorageManager
     */
    protected void annotateLoadedFiles(URI outdirUri, List<URI> files, List<StorageETLResult> results, ObjectMap options)
            throws StorageETLException {

        if (!files.isEmpty() && options.getBoolean(Options.ANNOTATE.key(), Options.ANNOTATE.defaultValue())) {
            try (VariantDBAdaptor dbAdaptor = getDBAdaptor()) {

                int studyId = options.getInt(Options.STUDY_ID.key());
                StudyConfiguration studyConfiguration = dbAdaptor.getStudyConfigurationManager()
                        .getStudyConfiguration(studyId, new QueryOptions(options)).first();

                List<Integer> fileIds = new ArrayList<>();
                for (URI uri : files) {
                    String fileName = VariantReaderUtils.getOriginalFromTransformedFile(uri);
                    fileIds.add(studyConfiguration.getFileIds().get(fileName));
                }

                Query annotationQuery = new Query();
                if (!options.getBoolean(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, false)) {
                    annotationQuery.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(), false);
                }
                annotationQuery.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(),
                        Collections.singletonList(studyId));    // annotate just the indexed variants
                // annotate just the indexed variants
                annotationQuery.put(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileIds);

                String dbName = options.getString(Options.DB_NAME.key());
                QueryOptions annotationOptions = new QueryOptions()
                        .append(DefaultVariantAnnotationManager.OUT_DIR, outdirUri.getPath())
                        .append(DefaultVariantAnnotationManager.FILE_NAME, dbName + "." + TimeUtils.getTime());

                annotate(dbName, annotationQuery, annotationOptions);
            } catch (RuntimeException | StorageManagerException | VariantAnnotatorException | IOException e) {
                throw new StorageETLException("Error annotating.", e, results);
            }
        }
    }

    /**
     * Provide a new VariantAnnotationManager for creating and loading annotations.
     *
     * @param annotator     VariantAnnotator to use for creating the new annotations
     * @param dbAdaptor     VariantDBAdaptor
     * @return              A new instance of VariantAnnotationManager
     */
    protected VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator, VariantDBAdaptor dbAdaptor) {
        return new DefaultVariantAnnotationManager(annotator, dbAdaptor);
    }

    /**
     * Drops a file from the Variant Storage.
     *
     * @param study  StudyName or StudyId
     * @param fileId FileId
     * @throws StorageManagerException If the file can not be deleted or there was some problem deleting it.
     */
    public abstract void dropFile(String study, int fileId) throws StorageManagerException;

    public abstract void dropStudy(String studyName) throws StorageManagerException;

    @Override
    public void testConnection() throws StorageManagerException {
//        ObjectMap variantOptions = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();
//        logger.error("Connection to database '{}' failed", variantOptions.getString(VariantStorageManager.Options.DB_NAME.key()));
//        throw new StorageManagerException("Database connection test failed");
    }

    public ObjectMap getOptions() {
        return configuration.getStorageEngine(storageEngineId).getVariant().getOptions();
    }

    public VariantReaderUtils getVariantReaderUtils() {
        return new VariantReaderUtils();
    }

    /**
     * Get the StudyConfigurationManager.
     * <p>
     * If there is no StudyConfigurationManager, try to build by dependency injection.
     * If can't build, call to the method "buildStudyConfigurationManager", witch could be override.
     *
     * @param options Map-like object with the options
     * @return A StudyConfigurationManager object
     * @throws StorageManagerException If object is null
     */
    public final StudyConfigurationManager getStudyConfigurationManager(ObjectMap options) throws StorageManagerException {
        StudyConfigurationManager studyConfigurationManager = null;
        String studyConfigurationManagerClassName = null;
        if (options.containsKey(Options.STUDY_CONFIGURATION_MANAGER_CLASS_NAME.key())) {
            studyConfigurationManagerClassName = options.getString(Options.STUDY_CONFIGURATION_MANAGER_CLASS_NAME.key());
        } else {
            if (configuration.getStudyMetadataManager() != null && !configuration.getStudyMetadataManager().isEmpty()) {
                studyConfigurationManagerClassName = configuration.getStudyMetadataManager();
            }
        }

        if (studyConfigurationManagerClassName != null && !studyConfigurationManagerClassName.isEmpty()) {
            try {
                studyConfigurationManager = StudyConfigurationManager.build(studyConfigurationManagerClassName, options);
            } catch (ReflectiveOperationException e) {
                e.printStackTrace();
                logger.error("Error creating a StudyConfigurationManager. Creating default StudyConfigurationManager", e);
                throw new RuntimeException(e);
            }
        }
        // This method can be override in children methods
        if (studyConfigurationManager == null) {
            studyConfigurationManager = buildStudyConfigurationManager(options);
        }

        return studyConfigurationManager;
    }


    /**
     * Build the default StudyConfigurationManager. This method could be override by children classes if they want to use other class.
     *
     * @param options Map-like object with the options
     * @return A StudyConfigurationManager object
     * @throws StorageManagerException If object is null
     */
    protected StudyConfigurationManager buildStudyConfigurationManager(ObjectMap options) throws StorageManagerException {
        return new FileStudyConfigurationManager(options);
    }

    /**
     * @param input  Input variant file (avro, json, vcf)
     * @param source VariantSource to fill. Can be null
     * @return Read VariantSource
     * @throws StorageManagerException if the format is not valid or there is an error reading
     * @deprecated use {@link VariantReaderUtils#readVariantSource(java.net.URI)}
     */
    @Deprecated
    public static VariantSource readVariantSource(Path input, VariantSource source) throws StorageManagerException {
        return VariantReaderUtils.readVariantSource(input, source);
    }

    public static String buildFilename(String studyName, int fileId) {
        return VariantStorageETL.buildFilename(studyName, fileId);
    }

    public void insertVariantIntoSolr() throws StorageManagerException {

        VariantDBAdaptor dbAdaptor = getDBAdaptor();
        VariantDBIterator variantDBIterator = dbAdaptor.iterator();

        while (variantDBIterator.hasNext()) {
            searchManager.insert(variantDBIterator.next());
        }
    }

}
