/*
 * Copyright 2015-2017 OpenCB
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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleData;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleDataManager;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.core.variant.query.*;
import org.opencb.opencga.storage.core.variant.search.SamplesSearchIndexVariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.search.SearchIndexVariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadListener;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.addDefaultLimit;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName;
import static org.opencb.opencga.storage.core.variant.search.VariantSearchUtils.buildSamplesIndexCollectionName;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageEngine extends StorageEngine<VariantDBAdaptor> implements VariantIterable {

    private final AtomicReference<VariantSearchManager> variantSearchManager = new AtomicReference<>();
    private final List<VariantQueryExecutor> lazyVariantQueryExecutorsList = new ArrayList<>();
    private CellBaseUtils cellBaseUtils;

    public static final String REMOVE_OPERATION_NAME = TaskMetadata.Type.REMOVE.name().toLowerCase();

    private Logger logger = LoggerFactory.getLogger(VariantStorageEngine.class);

    public enum MergeMode {
        BASIC,
        ADVANCED;

        public static MergeMode from(ObjectMap options) {
            String mergeModeStr = options.getString(Options.MERGE_MODE.key(), Options.MERGE_MODE.defaultValue().toString());
            return MergeMode.valueOf(mergeModeStr.toUpperCase());
        }
    }

    public enum UseSearchIndex {
        YES, NO, AUTO;

        public static UseSearchIndex from(Map<String, Object> options) {
            return options == null || !options.containsKey(VariantSearchManager.USE_SEARCH_INDEX)
                    ? AUTO
                    : UseSearchIndex.valueOf(options.get(VariantSearchManager.USE_SEARCH_INDEX).toString().toUpperCase());
        }
    }

    public enum SyncStatus {
        SYNCHRONIZED("Y"), NOT_SYNCHRONIZED("N"), UNKNOWN("?");
        private final String c;

        SyncStatus(String c) {
            this.c = c;
        }

        public String key() {
            return c;
        }
    }

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

        MERGE_MODE("merge.mode", MergeMode.ADVANCED),

        CALCULATE_STATS("calculateStats", false),          //Calculate stats on the postLoad step
        OVERWRITE_STATS("overwriteStats", false),          //Overwrite stats already present
        UPDATE_STATS("updateStats", false),                //Calculate missing stats
        STATS_DEFAULT_GENOTYPE("stats.default-genotype", "0/0"), // Default genotype to be used for calculating stats.
        STATS_MULTI_ALLELIC("stats.multiallelic", false),  // Include secondary alternates in the variant stats calculation

        ANNOTATE("annotate", false),
        INDEX_SEARCH("indexSearch", false),

        RESUME("resume", false),

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

    @Deprecated
    public VariantStorageEngine() {}

    public VariantStorageEngine(StorageConfiguration configuration) {
        this(configuration.getDefaultStorageEngineId(), configuration);
    }

    public VariantStorageEngine(String storageEngineId, StorageConfiguration configuration) {
        super(storageEngineId, configuration);
    }

    /**
     * Loads the given file into an empty database.
     *
     * The input file should have, in the same directory, a metadata file, with the same name ended with
     * {@link VariantExporter#METADATA_FILE_EXTENSION}
     *
     * @param inputFile     Variants input file in avro format.
     * @param params       Other options
     * @throws IOException      if there is any I/O error
     * @throws StorageEngineException  if there si any error loading the variants
     * */
    public void importData(URI inputFile, ObjectMap params) throws StorageEngineException, IOException {
        VariantImporter variantImporter = newVariantImporter();
        variantImporter.importData(inputFile);
    }

    /**
     * Loads the given file into an empty database.
     *
     * @param inputFile     Variants input file in avro format.
     * @param metadata      Metadata related with the data to be loaded.
     * @param studies       Already processed StudyConfigurations
     * @param params        Other options
     * @throws IOException      if there is any I/O error
     * @throws StorageEngineException  if there si any error loading the variants
     * */
    public void importData(URI inputFile, VariantMetadata metadata, List<StudyConfiguration> studies, ObjectMap params)
            throws StorageEngineException, IOException {
        VariantImporter variantImporter = newVariantImporter();
        variantImporter.importData(inputFile, metadata, studies);
    }

    /**
     * Creates a new {@link VariantImporter} for the current backend.
     *
     * There is no default VariantImporter.
     *
     * @return              new VariantImporter
     * @throws StorageEngineException  if there is an error creating the VariantImporter
     */
    protected VariantImporter newVariantImporter() throws StorageEngineException {
        throw new UnsupportedOperationException();
    }

    /**
     * Exports the result of the given query and the associated metadata.
     * @param outputFile    Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat  Variant output format
     * @param variantsFile  Optional variants file.
     * @param query         Query with the variants to export
     * @param queryOptions  Query options
     * @throws IOException  If there is any IO error
     * @throws StorageEngineException  If there is any error exporting variants
     */
    public void exportData(URI outputFile, VariantOutputFormat outputFormat, URI variantsFile, Query query, QueryOptions queryOptions)
            throws IOException, StorageEngineException {
        exportData(outputFile, outputFormat, variantsFile, query, queryOptions, null);
    }

    /**
     * Exports the result of the given query and the associated metadata.
     *
     * @param outputFile       Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat     Variant output format
     * @param variantsFile     Optional variants file.
     * @param query            Query with the variants to export
     * @param queryOptions     Query options
     * @param metadataFactory  Metadata factory. Metadata will only be generated if the outputFile is defined.
     * @throws IOException            If there is any IO error
     * @throws StorageEngineException If there is any error exporting variants
     */
    public void exportData(URI outputFile, VariantOutputFormat outputFormat, URI variantsFile, Query query, QueryOptions queryOptions,
                           VariantMetadataFactory metadataFactory)
            throws IOException, StorageEngineException {
        if (metadataFactory == null) {
            metadataFactory = new VariantMetadataFactory(getMetadataManager());
        }
        VariantExporter exporter = newVariantExporter(metadataFactory);
        query = preProcessQuery(query, queryOptions);
        exporter.export(outputFile, outputFormat, variantsFile, query, queryOptions);
    }

    /**
     * Creates a new {@link VariantExporter} for the current backend.
     * The default implementation iterates locally through the database.
     *
     * @param metadataFactory metadataFactory
     * @return              new VariantExporter
     * @throws StorageEngineException  if there is an error creating the VariantExporter
     */
    protected VariantExporter newVariantExporter(VariantMetadataFactory metadataFactory) throws StorageEngineException {
        return new VariantExporter(this, metadataFactory, ioManagerProvider);
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
            searchIndexLoadedFiles(inputFiles, getOptions());
        }
        return results;
    }

    @Override
    public abstract VariantStoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException;

    /**
     * Given a dbName, calculates the annotation for all the variants that matches with a given query, and loads them into the database.
     *
     * @param query     Query to select variants to annotate
     * @param params    Other params
     * @throws VariantAnnotatorException    If the annotation goes wrong
     * @throws StorageEngineException       If there is any problem related with the StorageEngine
     * @return number of annotated variants
     * @throws IOException                  If there is any IO problem
     */
    public long annotate(Query query, ObjectMap params) throws VariantAnnotatorException, StorageEngineException, IOException {
        // Merge with configuration
        ObjectMap options = getMergedOptions(params);
        VariantAnnotationManager annotationManager = newVariantAnnotationManager(options);
        return annotationManager.annotate(query, options);
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
        if (files != null && !files.isEmpty() && options.getBoolean(Options.ANNOTATE.key(), Options.ANNOTATE.defaultValue())) {
            try {

                String studyName = options.getString(Options.STUDY.key());
                VariantStorageMetadataManager metadataManager = getMetadataManager();
                int studyId = metadataManager.getStudyId(studyName);

                List<Integer> fileIds = new ArrayList<>(files.size());
                for (URI uri : files) {
                    String fileName = VariantReaderUtils.getOriginalFromTransformedFile(uri);
                    fileIds.add(metadataManager.getFileId(studyId, fileName));
                }

                // Annotate only the new indexed variants
                Query annotationQuery = new Query();
                if (!options.getBoolean(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, false)) {
                    annotationQuery.put(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
                }
                annotationQuery.put(VariantQueryParam.STUDY.key(), Collections.singletonList(studyId));
                annotationQuery.put(VariantQueryParam.FILE.key(), fileIds);

                ObjectMap annotationOptions = new ObjectMap(options)
                        .append(DefaultVariantAnnotationManager.OUT_DIR, outdirUri.toString())
                        .append(DefaultVariantAnnotationManager.FILE_NAME, dbName + "." + TimeUtils.getTime());

                annotate(annotationQuery, annotationOptions);
            } catch (RuntimeException | StorageEngineException | VariantAnnotatorException | IOException e) {
                throw new StoragePipelineException("Error annotating.", e, results);
            }
        }
    }

    public void saveAnnotation(String name, ObjectMap params) throws StorageEngineException, VariantAnnotatorException {
        newVariantAnnotationManager(params).saveAnnotation(name, params);
    }

    public void deleteAnnotation(String name, ObjectMap params) throws StorageEngineException, VariantAnnotatorException {
        newVariantAnnotationManager(params).deleteAnnotation(name, params);
    }

    public QueryResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options) throws StorageEngineException {
        options = addDefaultLimit(options, getOptions());
        return getDBAdaptor().getAnnotation(name, query, options);
    }

    public QueryResult<ProjectMetadata.VariantAnnotationMetadata> getAnnotationMetadata(String name) throws StorageEngineException {
        StopWatch started = StopWatch.createStarted();
        ProjectMetadata projectMetadata = getMetadataManager().getProjectMetadata();
        ProjectMetadata.VariantAnnotationSets annotation = projectMetadata.getAnnotation();
        List<ProjectMetadata.VariantAnnotationMetadata> list;
        if (StringUtils.isEmpty(name) || VariantQueryUtils.ALL.equals(name)) {
            list = new ArrayList<>(annotation.getSaved().size() + 1);
            if (annotation.getCurrent() != null) {
                list.add(annotation.getCurrent());
            }
            list.addAll(annotation.getSaved());
        } else {
            list = new ArrayList<>();
            for (String annotationName : name.split(",")) {
                if (VariantAnnotationManager.CURRENT.equalsIgnoreCase(annotationName)) {
                    if (annotation.getCurrent() != null) {
                        list.add(annotation.getCurrent());
                    }
                } else {
                    list.add(annotation.getSaved(annotationName));
                }
            }
        }
        return new QueryResult<>(name, ((int) started.getTime(TimeUnit.MILLISECONDS)), list.size(), list.size(), null, null, list);
    }

    /**
     * Provide a new VariantAnnotationManager for creating and loading annotations.
     *
     * @param params        Other params
     * @return              A new instance of VariantAnnotationManager
     * @throws StorageEngineException  if there is an error creating the VariantAnnotationManager
     * @throws VariantAnnotatorException  if there is an error creating the VariantAnnotator
     */
    protected final VariantAnnotationManager newVariantAnnotationManager(ObjectMap params)
            throws StorageEngineException, VariantAnnotatorException {
        ProjectMetadata projectMetadata = getMetadataManager().getProjectMetadata();
        VariantAnnotator annotator = VariantAnnotatorFactory.buildVariantAnnotator(
                configuration, getStorageEngineId(), projectMetadata, params);
        return newVariantAnnotationManager(annotator);
    }

    /**
     * Provide a new VariantAnnotationManager for creating and loading annotations.
     *
     * @param annotator     VariantAnnotator to use for creating the new annotations
     * @return              A new instance of VariantAnnotationManager
     * @throws StorageEngineException  if there is an error creating the VariantAnnotationManager
     */
    protected VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator) throws StorageEngineException {
        return new DefaultVariantAnnotationManager(annotator, getDBAdaptor(), ioManagerProvider);
    }

    /**
     *
     * @param study     Study
     * @param cohorts   Cohorts to calculate stats
     * @param options   Other options
     *                  {@link Options#AGGREGATION_MAPPING_PROPERTIES}
     *                  {@link Options#OVERWRITE_STATS}
     *                  {@link Options#UPDATE_STATS}
     *                  {@link Options#LOAD_THREADS}
     *                  {@link Options#LOAD_BATCH_SIZE}
     *                  {@link VariantQueryParam#REGION}
     *
     * @throws StorageEngineException      If there is any problem related with the StorageEngine
     * @throws IOException                  If there is any IO problem
     */
    public void calculateStats(String study, List<String> cohorts, QueryOptions options) throws StorageEngineException, IOException {
        VariantStatisticsManager statisticsManager = newVariantStatisticsManager();
        statisticsManager.calculateStatistics(study, cohorts, options);
    }

    public void calculateStats(String study, Map<String, ? extends Collection<String>> cohorts, QueryOptions options)
            throws StorageEngineException, IOException {
        VariantStatisticsManager statisticsManager = newVariantStatisticsManager();

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        metadataManager.registerCohorts(study, cohorts);

        statisticsManager.calculateStatistics(study, new ArrayList<>(cohorts.keySet()), options);
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
        if (files != null && !files.isEmpty() && options != null
                && options.getBoolean(Options.CALCULATE_STATS.key(), Options.CALCULATE_STATS.defaultValue())) {
            // TODO add filters
            try {
                VariantDBAdaptor dbAdaptor = getDBAdaptor();
                logger.debug("Calculating stats for files: '{}'...", files.toString());

                String studyName = options.getString(Options.STUDY.key());
                QueryOptions statsOptions = new QueryOptions(options);
                VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
                StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyName);

                List<Integer> fileIds = new ArrayList<>(files.size());
                for (URI uri : files) {
                    String fileName = VariantReaderUtils.getOriginalFromTransformedFile(uri);
                    fileIds.add(metadataManager.getFileId(studyMetadata.getId(), fileName));
                }
                Integer defaultCohortId = metadataManager.getCohortId(studyMetadata.getId(), StudyEntry.DEFAULT_COHORT);
                CohortMetadata defaultCohort = metadataManager.getCohortMetadata(studyMetadata.getId(), defaultCohortId);
                if (defaultCohort.isStatsReady()) {
                    logger.debug("Cohort '{}':{} was already calculated. Just update stats.", StudyEntry.DEFAULT_COHORT, defaultCohortId);
                    statsOptions.append(Options.UPDATE_STATS.key(), true);
                }
                URI statsOutputUri = output.resolve(VariantStoragePipeline
                        .buildFilename(studyMetadata.getName(), fileIds.get(0)) + "." + TimeUtils.getTime());
                statsOptions.put(DefaultVariantStatisticsManager.OUTPUT, statsOutputUri.toString());

                List<String> cohorts = Collections.singletonList(StudyEntry.DEFAULT_COHORT);
                calculateStats(studyMetadata.getName(), cohorts, statsOptions);
            } catch (Exception e) {
                throw new StoragePipelineException("Can't calculate stats.", e, results);
            }
        }
    }

    public void calculateMendelianErrors(String study, List<List<String>> trios, ObjectMap options) throws StorageEngineException {
        throw new UnsupportedOperationException("Unsupported calculateMendelianErrors");
    }

    /**
     * Provide a new VariantStatisticsManager for creating and loading statistics.
     *
     * @return              A new instance of VariantStatisticsManager
     * @throws StorageEngineException  if there is an error creating the VariantStatisticsManager
     */
    public VariantStatisticsManager newVariantStatisticsManager() throws StorageEngineException {
        return new DefaultVariantStatisticsManager(getDBAdaptor(), ioManagerProvider);
    }

    /**
     *
     * @param study     Study
     * @param samples   Samples to fill gaps
     * @param options   Other options
     * @throws StorageEngineException if there is any error
     */
    public void fillGaps(String study, List<String> samples, ObjectMap options) throws StorageEngineException {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @param study     Study
     * @param options   Other options
     * @param overwrite Overwrite gaps for all files and variants
     * @throws StorageEngineException if there is any error
     */
    public void fillMissing(String study, ObjectMap options, boolean overwrite) throws StorageEngineException {
        throw new UnsupportedOperationException();
    }

    public VariantSearchLoadResult searchIndex() throws StorageEngineException, IOException, VariantSearchException {
        return searchIndex(new Query(), new QueryOptions(), false);
    }

    public VariantSearchLoadResult searchIndex(Query inputQuery, QueryOptions inputQueryOptions, boolean overwrite)
            throws StorageEngineException, IOException, VariantSearchException {
        Query query = inputQuery == null ? new Query() : new Query(inputQuery);
        QueryOptions queryOptions = inputQueryOptions == null ? new QueryOptions() : new QueryOptions(inputQueryOptions);

        VariantDBAdaptor dbAdaptor = getDBAdaptor();

        VariantSearchManager variantSearchManager = getVariantSearchManager();
        // first, create the collection it it does not exist
        variantSearchManager.create(dbName);
        if (!configuration.getSearch().isActive() || !variantSearchManager.isAlive(dbName)) {
            throw new StorageEngineException("Solr is not alive!");
        }

        // then, load variants
        queryOptions.put(QueryOptions.EXCLUDE, Arrays.asList(VariantField.STUDIES_SAMPLES_DATA, VariantField.STUDIES_FILES));
        try (VariantDBIterator iterator = getVariantsToIndex(overwrite, query, queryOptions, dbAdaptor)) {
            ProgressLogger progressLogger = new ProgressLogger("Variants loaded in Solr:", () -> dbAdaptor.count(query).first(), 200);
            VariantSearchLoadResult load = variantSearchManager.load(dbName, iterator, progressLogger, newVariantSearchLoadListener());

            long value = System.currentTimeMillis();
            getMetadataManager().updateProjectMetadata(projectMetadata -> {
                projectMetadata.getAttributes().put(SEARCH_INDEX_LAST_TIMESTAMP.key(), value);
                return projectMetadata;
            });

            return load;
        } catch (StorageEngineException | IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageEngineException("Exception closing VariantDBIterator", e);
        }
    }

    protected VariantDBIterator getVariantsToIndex(boolean overwrite, Query query, QueryOptions queryOptions, VariantDBAdaptor dbAdaptor)
            throws StorageEngineException {
        if (!overwrite) {
            query.put(VariantQueryUtils.VARIANTS_TO_INDEX.key(), true);
        }
        return dbAdaptor.iterator(query, queryOptions);
    }

    protected void searchIndexLoadedFiles(List<URI> inputFiles, ObjectMap options) throws StorageEngineException {
        try {
            if (options.getBoolean(INDEX_SEARCH.key())) {
                searchIndex(new Query(), new QueryOptions(), false);
            }
        } catch (IOException | VariantSearchException e) {
            throw new StorageEngineException("Error indexing in search", e);
        }
    }

    protected VariantSearchLoadListener newVariantSearchLoadListener() throws StorageEngineException {
        return VariantSearchLoadListener.empty();
    }

    public void secondaryIndexSamples(String study, List<String> samples)
            throws StorageEngineException, IOException, VariantSearchException {
        VariantDBAdaptor dbAdaptor = getDBAdaptor();

        VariantSearchManager variantSearchManager = getVariantSearchManager();
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        // first, create the collection it it does not exist

        AtomicInteger atomicId = new AtomicInteger();
        StudyMetadata studyMetadata = metadataManager.updateStudyMetadata(study, sm -> {
            boolean resume = getOptions().getBoolean(RESUME.key(), RESUME.defaultValue());
            atomicId.set(metadataManager.registerSecondaryIndexSamples(sm.getId(), samples, resume));
            return sm;
        });
        int id = atomicId.intValue();

        String collectionName = buildSamplesIndexCollectionName(this.dbName, studyMetadata, id);

        try {
            variantSearchManager.create(collectionName);
            if (configuration.getSearch().isActive() && variantSearchManager.isAlive(collectionName)) {
                // then, load variants
                QueryOptions queryOptions = new QueryOptions();
                Query query = new Query(VariantQueryParam.STUDY.key(), study)
                        .append(VariantQueryParam.SAMPLE.key(), samples);

                VariantDBIterator iterator = dbAdaptor.iterator(query, queryOptions);

                ProgressLogger progressLogger = new ProgressLogger("Variants loaded in Solr:", () -> dbAdaptor.count(query).first(), 200);
                variantSearchManager.load(collectionName, iterator, progressLogger, VariantSearchLoadListener.empty());
            } else {
                throw new StorageEngineException("Solr is not alive!");
            }
            dbAdaptor.close();
        } catch (Exception e) {
            metadataManager.updateCohortMetadata(studyMetadata.getId(), id,
                    cohortMetadata -> cohortMetadata.setSecondaryIndexStatus(TaskMetadata.Status.ERROR));
            throw e;
        }

        metadataManager.updateCohortMetadata(studyMetadata.getId(), id,
                cohortMetadata -> cohortMetadata.setSecondaryIndexStatus(TaskMetadata.Status.READY));
    }

    public void removeSecondaryIndexSamples(String study, List<String> samples) throws StorageEngineException, VariantSearchException {
        VariantSearchManager variantSearchManager = getVariantSearchManager();

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        StudyMetadata sm = metadataManager.getStudyMetadata(study);

        // Check that all samples are from the same secondary index
        Set<Integer> sampleIds = new HashSet<>();
        Set<Integer> secIndexIdSet = new HashSet<>();
        for (String sample : samples) {
            Integer sampleId = metadataManager.getSampleId(sm.getId(), sample);
            if (sampleId == null) {
                throw VariantQueryException.sampleNotFound(sample, study);
            }
            Set<Integer> secondaryIndexCohorts = metadataManager.getSampleMetadata(sm.getId(), sampleId).getSecondaryIndexCohorts();
            if (secondaryIndexCohorts.isEmpty()) {
                throw new StorageEngineException("Samples not in a secondary index");
            }
            sampleIds.add(sampleId);
            secIndexIdSet.addAll(secondaryIndexCohorts);
        }
        if (secIndexIdSet.isEmpty() || secIndexIdSet.contains(null)) {
            throw new StorageEngineException("Samples not in a secondary index");
        } else if (secIndexIdSet.size() != 1) {
            throw new StorageEngineException("Samples in multiple secondary indexes");
        }
        Integer secIndexId = secIndexIdSet.iterator().next();
        CohortMetadata secIndex = metadataManager.getCohortMetadata(sm.getId(), secIndexId);
        // Check that all samples from the secondary index are provided
        List<Integer> samplesInSecIndex = secIndex.getSamples();
        if (samplesInSecIndex.size() != sampleIds.size()) {
            throw new StorageEngineException("Must provide all the samples from the secondary index: " + samplesInSecIndex
                    .stream()
                    .map(id -> metadataManager.getSampleName(sm.getId(), id))
                    .collect(Collectors.joining("\", \"", "\"", "\"")));
        }


        // Invalidate secondary index
        metadataManager.updateCohortMetadata(sm.getId(), secIndexId,
                cohortMetadata -> cohortMetadata.setSecondaryIndexStatus(TaskMetadata.Status.RUNNING));

        // Remove secondary index
        String collection = buildSamplesIndexCollectionName(dbName, sm, secIndexId);
        variantSearchManager.getSolrManager().remove(collection);

        // Remove secondary index metadata
        metadataManager.updateCohortMetadata(sm.getId(), secIndexId,
                cohortMetadata -> cohortMetadata.setSecondaryIndexStatus(TaskMetadata.Status.NONE));
        metadataManager.setSamplesToCohort(sm.getId(), secIndex.getName(), Collections.emptyList());
//        metadataManager.removeCohort(sm.getId(), secIndex.getName());
    }

    /**
     * Removes a file from the Variant Storage.
     *
     * @param study  StudyName or StudyId
     * @param fileId FileId
     * @throws StorageEngineException If the file can not be removed or there was some problem deleting it.
     */
    public void removeFile(String study, int fileId) throws StorageEngineException {
        removeFiles(study, Collections.singletonList(String.valueOf(fileId)));
    }

    /**
     * Removes a file from the Variant Storage.
     *
     * @param study  StudyName or StudyId
     * @param files Files to remove
     * @throws StorageEngineException If the file can not be removed  or there was some problem deleting it.
     */
    public abstract void removeFiles(String study, List<String> files) throws StorageEngineException;

    /**
     * Atomically updates the storage metadata before removing samples.
     *
     * @param study    Study
     * @param files    Files to remove
     * @return FileIds to remove
     * @throws StorageEngineException StorageEngineException
     */
    protected TaskMetadata preRemoveFiles(String study, List<String> files) throws StorageEngineException {
        AtomicReference<TaskMetadata> batchFileOperation = new AtomicReference<>();
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        metadataManager.updateStudyMetadata(study, studyMetadata -> {
            List<Integer> fileIds = new ArrayList<>(files.size());
            for (String file : files) {
                FileMetadata fileMetadata = metadataManager.getFileMetadata(studyMetadata.getId(), file);
                if (fileMetadata == null) {
                    throw VariantQueryException.fileNotFound(file, study);
                }
                fileIds.add(fileMetadata.getId());
                if (!fileMetadata.isIndexed()) {
                    throw new StorageEngineException("Unable to remove non indexed file: " + fileMetadata.getName());
                }
            }

            boolean resume = getOptions().getBoolean(RESUME.key(), RESUME.defaultValue());

            batchFileOperation.set(metadataManager.addRunningTask(
                    studyMetadata.getId(),
                    REMOVE_OPERATION_NAME,
                    fileIds,
                    resume,
                    TaskMetadata.Type.REMOVE));

            return studyMetadata;
        });
        return batchFileOperation.get();
    }

    /**
     * Atomically updates the storage metadata after removing samples.
     *
     * If success:
     *    Updates remove status with READY
     *    Removes the files from indexed files list
     *    Removes the samples removed from the default cohort {@link StudyEntry#DEFAULT_COHORT}
     *      * Be aware that some samples can be in multiple files.
     *    Invalidates the cohorts with removed samples
     * If error:
     *    Updates remove status with ERROR
     *
     * @param study    Study
     * @param fileIds  Removed file ids
     * @param taskId   Remove task id
     * @param error    If the remove operation succeeded
     * @throws StorageEngineException StorageEngineException
     */
    protected void postRemoveFiles(String study, List<Integer> fileIds, int taskId, boolean error) throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        metadataManager.updateStudyMetadata(study, studyMetadata -> {
            if (error) {
                metadataManager.setStatus(studyMetadata.getId(), taskId, TaskMetadata.Status.ERROR);
            } else {
                metadataManager.setStatus(studyMetadata.getId(), taskId, TaskMetadata.Status.READY);
                metadataManager.removeIndexedFiles(studyMetadata.getId(), fileIds);

                Set<Integer> removedSamples = new HashSet<>();
                for (Integer fileId : fileIds) {
                    removedSamples.addAll(metadataManager.getFileMetadata(studyMetadata.getId(), fileId).getSamples());
                }
                List<Integer> cohortsToInvalidate = new LinkedList<>();
                for (CohortMetadata cohort : metadataManager.getCalculatedCohorts(studyMetadata.getId())) {
                    for (Integer removedSample : removedSamples) {
                        if (cohort.getSamples().contains(removedSample)) {
                            logger.info("Invalidating statistics of cohort "
                                    + cohort.getName()
                                    + " (" + cohort.getId() + ')');
                            cohortsToInvalidate.add(cohort.getId());
                            break;
                        }
                    }
                }
                for (Integer cohortId : cohortsToInvalidate) {
                    metadataManager.updateCohortMetadata(studyMetadata.getId(), cohortId,
                            cohort -> cohort.setStatsStatus(TaskMetadata.Status.ERROR));
                }

                // Restore default cohort with indexed samples
                Integer cohortId = metadataManager.getCohortId(studyMetadata.getId(), StudyEntry.DEFAULT_COHORT);
                metadataManager.updateCohortMetadata(studyMetadata.getId(), cohortId,
                        defaultCohort -> defaultCohort.setSamples(metadataManager.getIndexedSamples(studyMetadata.getId())));


                for (Integer fileId : fileIds) {
                    getDBAdaptor().getMetadataManager().removeVariantFileMetadata(studyMetadata.getId(), fileId);
                }
            }
            return studyMetadata;
        });
    }

    /**
     * Remove a whole study from the Variant Storage.
     *
     * @param study  StudyName or StudyId
     * @throws StorageEngineException If the file can not be removed or there was some problem deleting it.
     */
    public abstract void removeStudy(String study) throws StorageEngineException;

    @Override
    public void testConnection() throws StorageEngineException {}

    public CellBaseUtils getCellBaseUtils() throws StorageEngineException {
        if (cellBaseUtils == null) {
            final ProjectMetadata metadata = getMetadataManager().getProjectMetadata(getOptions());

            String species = metadata.getSpecies();
            String assembly = metadata.getAssembly();

            ClientConfiguration clientConfiguration = configuration.getCellbase().toClientConfiguration();
            if (StringUtils.isEmpty(species)) {
                species = clientConfiguration.getDefaultSpecies();
            }
            species = toCellBaseSpeciesName(species);
            cellBaseUtils = new CellBaseUtils(new CellBaseClient(species, assembly, clientConfiguration), assembly);
        }
        return cellBaseUtils;
    }

    public ObjectMap getOptions() {
        return configuration.getStorageEngine(storageEngineId).getVariant().getOptions();
    }

    public final ObjectMap getMergedOptions(Map<? extends String, ?> params) {
        ObjectMap options = new ObjectMap(getOptions());
        if (params != null) {
            params.forEach(options::putIfNotNull);
        }
        return options;
    }

    public VariantReaderUtils getVariantReaderUtils() {
        return new VariantReaderUtils(ioManagerProvider);
    }

    /**
     * Build the default VariantStorageMetadataManager. This method could be override by children classes if they want to use other class.
     *
     * @return VariantStorageMetadataManager
     * @throws StorageEngineException If object is null
     */
    public abstract VariantStorageMetadataManager getMetadataManager() throws StorageEngineException;

    public VariantSearchManager getVariantSearchManager() throws StorageEngineException {
        if (variantSearchManager.get() == null) {
            synchronized (variantSearchManager) {
                if (variantSearchManager.get() == null) {
                    // TODO One day we should use reflection here reading from storage-configuration.yml
                    variantSearchManager.set(new VariantSearchManager(getMetadataManager(), configuration));
                }
            }
        }
        return variantSearchManager.get();
    }

    public VariantQueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options, int windowsSize)
            throws StorageEngineException {
        VariantQueryExecutor.setDefaultTimeout(options, getOptions());
        return getDBAdaptor().getPhased(variant, studyName, sampleName, options, windowsSize);
    }

    public VariantQueryResult<Variant> getCompoundHeterozygous(String study, String child, String father, String mother,
                                                               Query query, QueryOptions options) {
        father = StringUtils.isEmpty(father) ? CompoundHeterozygousQueryExecutor.MISSING_SAMPLE : father;
        mother = StringUtils.isEmpty(mother) ? CompoundHeterozygousQueryExecutor.MISSING_SAMPLE : mother;
        query = new Query(query)
                .append(VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS.key(), Arrays.asList(child, father, mother))
                .append(VariantQueryParam.STUDY.key(), study);

        return get(query, options);
    }

    public QueryResult<VariantSampleData> getSampleData(String variant, String study, QueryOptions options) throws StorageEngineException {
        return new VariantSampleDataManager(getDBAdaptor()).getSampleData(variant, study, options);
    }

    public VariantQueryResult<Variant> get(Query query, QueryOptions options) {
        query = preProcessQuery(query, options);
        return getVariantQueryExecutor(query, options).get(query, options);
    }

    @Override
    public MultiVariantDBIterator iterator(Iterator<?> variants, Query query, QueryOptions options, int batchSize) {
        query = preProcessQuery(query, options);
        try {
            return getDBAdaptor().iterator(variants, query, options, batchSize);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
        query = preProcessQuery(query, options);
        return getVariantQueryExecutor(query, options).iterator(query, options);
    }

    protected final List<VariantQueryExecutor> getVariantQueryExecutors() throws StorageEngineException {
        if (lazyVariantQueryExecutorsList.isEmpty()) {
            synchronized (lazyVariantQueryExecutorsList) {
                if (lazyVariantQueryExecutorsList.isEmpty()) {
                    lazyVariantQueryExecutorsList.addAll(initVariantQueryExecutors());
                }
            }
        }
        return lazyVariantQueryExecutorsList;
    }

    protected List<VariantQueryExecutor> initVariantQueryExecutors() throws StorageEngineException {
        List<VariantQueryExecutor> executors = new ArrayList<>(3);

        executors.add(new CompoundHeterozygousQueryExecutor(
                getMetadataManager(), getStorageEngineId(), getOptions(), this));
        executors.add(new SamplesSearchIndexVariantQueryExecutor(
                getDBAdaptor(), getVariantSearchManager(), getStorageEngineId(), dbName, configuration, getOptions()));
        executors.add(new SearchIndexVariantQueryExecutor(
                getDBAdaptor(), getVariantSearchManager(), getStorageEngineId(), dbName, configuration, getOptions()));
        executors.add(new DBAdaptorVariantQueryExecutor(
                getDBAdaptor(), getStorageEngineId(), getOptions()));
        return executors;
    }

    /**
     * Determine which {@link VariantQueryExecutor} should be used to execute the given query.
     *
     * @param query   Query to execute
     * @param options Options for the query
     * @return VariantQueryExecutor to use
     */
    public VariantQueryExecutor getVariantQueryExecutor(Query query, QueryOptions options) {
        try {
            for (VariantQueryExecutor executor : getVariantQueryExecutors()) {
                if (executor.canUseThisExecutor(query, options)) {
                    return executor;
                }
            }
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }
        // This should never happen, as the DBAdaptorVariantQueryExecutor can always run the query
        throw new IllegalStateException("No VariantQueryExecutor found to run the query!");
    }

    public Query preProcessQuery(Query originalQuery, QueryOptions options) {
        try {
            return getVariantQueryParser().preProcessQuery(originalQuery, options);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected VariantQueryParser getVariantQueryParser() throws StorageEngineException {
        return new VariantQueryParser(getCellBaseUtils(), getMetadataManager());
    }

    public QueryResult distinct(Query query, String field) throws StorageEngineException {
        return getDBAdaptor().distinct(query, field);
    }

    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws StorageEngineException {
        return getDBAdaptor().rank(query, field, numResults, asc);
    }

    public QueryResult getFrequency(Query query, Region region, int regionIntervalSize) throws StorageEngineException {
        return getDBAdaptor().getFrequency(query, region, regionIntervalSize);
    }

    public QueryResult groupBy(Query query, String field, QueryOptions options) throws StorageEngineException {
        return getDBAdaptor().groupBy(query, field, options);
    }

    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws StorageEngineException {
        return getDBAdaptor().groupBy(query, fields, options);
    }

    public QueryResult<Long> count(Query query) throws StorageEngineException {
        query = preProcessQuery(query, null);
        VariantQueryExecutor variantQueryExecutor = getVariantQueryExecutor(query, new QueryOptions(QueryOptions.COUNT, true));
        return variantQueryExecutor.count(query);
    }

    /**
     * Fetch facet (i.e., counts) resulting of executing the query in the database.
     *
     * @param query          Query to be executed in the database to filter variants
     * @param options        Query modifiers, accepted values are: facet fields and facet ranges
     * @return               A FacetedQueryResult with the result of the query
     */
    public FacetQueryResult facet(Query query, QueryOptions options) {
        try {
            return new VariantAggregationExecutor(getVariantSearchManager(), dbName, this, getMetadataManager()).facet(query, options);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    @Override
    public void close() throws IOException {
        cellBaseUtils = null;
        if (variantSearchManager.get() != null) {
            try {
                variantSearchManager.get().close();
            } finally {
                variantSearchManager.set(null);
            }
        }
        lazyVariantQueryExecutorsList.clear();
    }
}

