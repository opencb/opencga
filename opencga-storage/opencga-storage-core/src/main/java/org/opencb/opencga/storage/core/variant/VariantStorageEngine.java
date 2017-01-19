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

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.metadata.ExportMetadata;
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
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
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
public abstract class VariantStorageEngine extends StorageEngine<VariantDBAdaptor> {

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
        ANNOTATE("annotate", false),

        RESUME("resume", false),

        DEFAULT_TIMEOUT("dbadaptor.default_timeout", 10000), // Default timeout for DBAdaptor operations. Only used if none is provided.
        MAX_TIMEOUT("dbadaptor.max_timeout", 30000);         // Max allowed timeout for DBAdaptor operations

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

    public VariantStorageEngine() {
        logger = LoggerFactory.getLogger(VariantStorageEngine.class);
    }

    public VariantStorageEngine(StorageConfiguration configuration) {
        super(configuration);
        logger = LoggerFactory.getLogger(VariantStorageEngine.class);

    }

    public VariantStorageEngine(String storageEngineId, StorageConfiguration configuration) {
        super(storageEngineId, configuration);
        logger = LoggerFactory.getLogger(VariantStorageEngine.class);
    }

    /**
     * Loads the given file into an empty database.
     *
     * The input file should have, in the same directory, a metadata file, with the same name ended with
     * {@link VariantExporter#METADATA_FILE_EXTENSION}
     *
     * @param inputFile     Variants input file in avro format.
     * @param dbName        Database name where to load the variants
     * @param options       Other options
     * @throws IOException      if there is any I/O error
     * @throws StorageEngineException  if there si any error loading the variants
     * */
    public void importData(URI inputFile, String dbName, ObjectMap options) throws StorageEngineException, IOException {
        try (VariantDBAdaptor dbAdaptor = getDBAdaptor(dbName)) {
            VariantImporter variantImporter = newVariantImporter(dbAdaptor);
            variantImporter.importData(inputFile);
        }
    }

    /**
     * Loads the given file into an empty database.
     *
     * @param inputFile     Variants input file in avro format.
     * @param metadata      Metadata related with the data to be loaded.
     * @param dbName        Database name where to load the variants
     * @param options       Other options
     * @throws IOException      if there is any I/O error
     * @throws StorageEngineException  if there si any error loading the variants
     * */
    public void importData(URI inputFile, ExportMetadata metadata, String dbName, ObjectMap options)
            throws StorageEngineException, IOException {
        try (VariantDBAdaptor dbAdaptor = getDBAdaptor(dbName)) {
            VariantImporter variantImporter = newVariantImporter(dbAdaptor);
            variantImporter.importData(inputFile, metadata);
        }
    }

    /**
     * Creates a new {@link VariantImporter} for the current backend.
     *
     * There is no default VariantImporter.
     *
     * @param dbAdaptor     DBAdaptor to the current database
     * @return              new VariantImporter
     */
    protected VariantImporter newVariantImporter(VariantDBAdaptor dbAdaptor) {
        throw new UnsupportedOperationException();
    }

    /**
     * Exports the result of the given query and the associated metadata.
     * @param outputFile    Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat  Variant output format
     * @param dbName        DBName for reading the variants
     * @param query         Query with the variants to export
     * @param queryOptions  Query options
     * @throws IOException  If there is any IO error
     * @throws StorageEngineException  If there is any error exporting variants
     */
    public void exportData(URI outputFile, VariantOutputFormat outputFormat, String dbName, Query query, QueryOptions queryOptions)
            throws IOException, StorageEngineException {
        try (VariantDBAdaptor dbAdaptor = getDBAdaptor(dbName)) {
            VariantExporter exporter = newVariantExporter(dbAdaptor);
            exporter.export(outputFile, outputFormat, query, queryOptions);
        }
    }

    /**
     * Creates a new {@link VariantExporter} for the current backend.
     * The default implementation iterates locally through the database.
     *
     * @param dbAdaptor     DBAdaptor to the current database
     * @return              new VariantExporter
     */
    protected VariantExporter newVariantExporter(VariantDBAdaptor dbAdaptor) {
        return new VariantExporter(dbAdaptor);
    }

    /**
     * Index the given input files. By default, executes the steps in {@link VariantStoragePipeline}.
     *
     * Will create a {@link #newStoragePipeline} for each input file.
     *
     * @param inputFiles    Input files to index
     * @param outdirUri     Output directory for possible intermediate files
     * @param doExtract     Execute extract step {@link VariantStoragePipeline#extract}
     * @param doTransform   Execute transform step {@link VariantStoragePipeline#transform}
     * @param doLoad        Execute load step {@link VariantStoragePipeline#load}
     * @return              List of {@link StoragePipelineResult}, one for each input file.
     * @throws StorageEngineException      If there is any problem related with the StorageEngine
     */
    @Override
    public List<StoragePipelineResult> index(List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean doTransform, boolean doLoad)
            throws StorageEngineException {
        List<StoragePipelineResult> results = super.index(inputFiles, outdirUri, doExtract, doTransform, doLoad);
        if (doLoad) {
            annotateLoadedFiles(outdirUri, inputFiles, results, getOptions());
            calculateStatsForLoadedFiles(outdirUri, inputFiles, results, getOptions());
        }
        return results;
    }

    @Override
    public abstract VariantStoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException;

    /**
     * Given a dbName, calculates the annotation for all the variants that matches with a given query, and loads them into the database.
     *
     * @param dbName    database name to annotate.
     * @param query     Query to select variants to annotate
     * @param params    Other params
     * @throws VariantAnnotatorException    If the annotation goes wrong
     * @throws StorageEngineException       If there is any problem related with the StorageEngine
     * @throws IOException                  If there is any IO problem
     */
    public void annotate(String dbName, Query query, ObjectMap params)
            throws VariantAnnotatorException, StorageEngineException, IOException {

        VariantAnnotator annotator = VariantAnnotatorFactory.buildVariantAnnotator(configuration, getStorageEngineId(), params);
        try (VariantDBAdaptor dbAdaptor = getDBAdaptor(dbName)) {
            VariantAnnotationManager annotationManager = newVariantAnnotationManager(annotator, dbAdaptor);
            annotationManager.annotate(query, params);
        }
    }

    /**
     * Annotate loaded files. Used only to annotate recently loaded files, after the {@link #index}.
     *
     * @param outdirUri     Index output directory
     * @param files         Indexed files
     * @param results       StorageETLResults
     * @param options       Other options
     * @throws StoragePipelineException  If there is any problem related with the StorageEngine
     */
    protected void annotateLoadedFiles(URI outdirUri, List<URI> files, List<StoragePipelineResult> results, ObjectMap options)
            throws StoragePipelineException {

        if (!files.isEmpty() && options.getBoolean(Options.ANNOTATE.key(), Options.ANNOTATE.defaultValue())) {
            String dbName = options.getString(Options.DB_NAME.key(), null);
            try (VariantDBAdaptor dbAdaptor = getDBAdaptor(dbName)) {
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

                QueryOptions annotationOptions = new QueryOptions()
                        .append(DefaultVariantAnnotationManager.OUT_DIR, outdirUri.getPath())
                        .append(DefaultVariantAnnotationManager.FILE_NAME, dbName + "." + TimeUtils.getTime());

                annotate(dbName, annotationQuery, annotationOptions);
            } catch (RuntimeException | StorageEngineException | VariantAnnotatorException | IOException e) {
                throw new StoragePipelineException("Error annotating.", e, results);
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
     *
     * @param study     Study
     * @param cohorts   Cohorts to calculate stats
     * @param dbName    database name to annotate.
     * @param options   Other options
     *                  {@link Options#AGGREGATION_MAPPING_PROPERTIES}
     *                  {@link Options#OVERWRITE_STATS}
     *                  {@link Options#UPDATE_STATS}
     *                  {@link Options#LOAD_THREADS}
     *                  {@link Options#LOAD_BATCH_SIZE}
     *                  {@link VariantDBAdaptor.VariantQueryParams#REGION}
     *
     * @throws StorageEngineException      If there is any problem related with the StorageEngine
     * @throws IOException                  If there is any IO problem
     */
    public void calculateStats(String study, List<String> cohorts, String dbName, QueryOptions options)
            throws StorageEngineException, IOException {

        try (VariantDBAdaptor dbAdaptor = getDBAdaptor(dbName)) {
            VariantStatisticsManager statisticsManager = newVariantStatisticsManager(dbAdaptor);
            statisticsManager.calculateStatistics(study, cohorts, options);
        }
    }

    /**
     * Calculate stats for loaded files. Used to calculate statistics for cohort ALL from recently loaded files, after the {@link #index}.
     *
     * @param output     Index output directory
     * @param files         Indexed files
     * @param results       StorageETLResults
     * @param options       Other options
     * @throws StoragePipelineException  If there is any problem related with the StorageEngine
     */
    protected void calculateStatsForLoadedFiles(URI output, List<URI> files, List<StoragePipelineResult> results, ObjectMap options)
            throws StoragePipelineException {

        if (options.getBoolean(Options.CALCULATE_STATS.key(), Options.CALCULATE_STATS.defaultValue())) {
            // TODO add filters
            String dbName = options.getString(Options.DB_NAME.key(), null);
            try (VariantDBAdaptor dbAdaptor = getDBAdaptor(dbName)) {
                logger.debug("about to calculate stats");

                int studyId = options.getInt(Options.STUDY_ID.key());
                QueryOptions statsOptions = new QueryOptions(options);
                StudyConfiguration studyConfiguration = dbAdaptor.getStudyConfigurationManager()
                        .getStudyConfiguration(studyId, new QueryOptions()).first();

                List<Integer> fileIds = new ArrayList<>();
                for (URI uri : files) {
                    String fileName = VariantReaderUtils.getOriginalFromTransformedFile(uri);
                    fileIds.add(studyConfiguration.getFileIds().get(fileName));
                }
                Integer defaultCohortId = studyConfiguration.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
                if (studyConfiguration.getCalculatedStats().contains(defaultCohortId)) {
                    logger.debug("Cohort \"{}\":{} was already calculated. Just update stats.", StudyEntry.DEFAULT_COHORT, defaultCohortId);
                    statsOptions.append(Options.UPDATE_STATS.key(), true);
                }
                URI statsOutputUri = output.resolve(buildFilename(studyConfiguration.getStudyName(), fileIds.get(0))
                        + "." + TimeUtils.getTime());
                statsOptions.put(DefaultVariantStatisticsManager.OUTPUT, statsOutputUri.toString());

                statsOptions.remove(Options.FILE_ID.key());

                List<String> cohorts = Collections.singletonList(StudyEntry.DEFAULT_COHORT);
                calculateStats(studyConfiguration.getStudyName(), cohorts, dbName, statsOptions);
            } catch (Exception e) {
                throw new StoragePipelineException("Can't calculate stats.", e, results);
            }
        }
    }

    /**
     * Provide a new VariantAnnotationManager for creating and loading annotations.
     *
     * @param dbAdaptor     VariantDBAdaptor
     * @return              A new instance of VariantAnnotationManager
     */
    public VariantStatisticsManager newVariantStatisticsManager(VariantDBAdaptor dbAdaptor) {
        return new DefaultVariantStatisticsManager(dbAdaptor);
    }


    /**
     * Drops a file from the Variant Storage.
     *
     * @param study  StudyName or StudyId
     * @param fileId FileId
     * @throws StorageEngineException If the file can not be deleted or there was some problem deleting it.
     */
    public abstract void dropFile(String study, int fileId) throws StorageEngineException;

    public abstract void dropStudy(String studyName) throws StorageEngineException;

    @Override
    public void testConnection() throws StorageEngineException {
//        ObjectMap variantOptions = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();
//        logger.error("Connection to database '{}' failed", variantOptions.getString(VariantStorageEngine.Options.DB_NAME.key()));
//        throw new StorageEngineException("Database connection test failed");
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
     * @throws StorageEngineException If object is null
     */
    public final StudyConfigurationManager getStudyConfigurationManager(ObjectMap options) throws StorageEngineException {
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
     * @throws StorageEngineException If object is null
     */
    protected StudyConfigurationManager buildStudyConfigurationManager(ObjectMap options) throws StorageEngineException {
        return new FileStudyConfigurationManager(options);
    }

    /**
     * @param input  Input variant file (avro, json, vcf)
     * @param source VariantSource to fill. Can be null
     * @return Read VariantSource
     * @throws StorageEngineException if the format is not valid or there is an error reading
     * @deprecated use {@link VariantReaderUtils#readVariantSource(java.net.URI)}
     */
    @Deprecated
    public static VariantSource readVariantSource(Path input, VariantSource source) throws StorageEngineException {
        return VariantReaderUtils.readVariantSource(input, source);
    }

    public static String buildFilename(String studyName, int fileId) {
        return VariantStoragePipeline.buildFilename(studyName, fileId);
    }

    public void insertVariantIntoSolr() throws StorageEngineException {

        VariantDBAdaptor dbAdaptor = getDBAdaptor();
        VariantDBIterator variantDBIterator = dbAdaptor.iterator();

        while (variantDBIterator.hasNext()) {
            searchManager.insert(variantDBIterator.next());
        }
    }

}
