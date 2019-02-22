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

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
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
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadListener;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchSolrIterator;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName;
import static org.opencb.opencga.storage.core.variant.search.VariantSearchUtils.*;
import static org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager.SEARCH_ENGINE_ID;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageEngine extends StorageEngine<VariantDBAdaptor> implements VariantIterable {

    private final AtomicReference<VariantSearchManager> variantSearchManager = new AtomicReference<>();
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
     * @param query         Query with the variants to export
     * @param queryOptions  Query options
     * @throws IOException  If there is any IO error
     * @throws StorageEngineException  If there is any error exporting variants
     */
    public void exportData(URI outputFile, VariantOutputFormat outputFormat, Query query, QueryOptions queryOptions)
            throws IOException, StorageEngineException {
        exportData(outputFile, outputFormat, new VariantMetadataFactory(getMetadataManager()
        ), query, queryOptions);
    }

    /**
     * Exports the result of the given query and the associated metadata.
     * @param outputFile       Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat     Variant output format
     * @param metadataFactory  Metadata factory. Metadata will only be generated if the outputFile is defined.
     * @param query            Query with the variants to export
     * @param queryOptions     Query options
     * @throws IOException  If there is any IO error
     * @throws StorageEngineException  If there is any error exporting variants
     */
    public void exportData(URI outputFile, VariantOutputFormat outputFormat, VariantMetadataFactory metadataFactory,
                           Query query, QueryOptions queryOptions)
            throws IOException, StorageEngineException {
        VariantExporter exporter = newVariantExporter(metadataFactory);
        query = preProcessQuery(query, queryOptions);
        exporter.export(outputFile, outputFormat, query, queryOptions);
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
        return new VariantExporter(this, metadataFactory);
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
                        .append(DefaultVariantAnnotationManager.OUT_DIR, outdirUri.getPath())
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
        options = addDefaultLimit(options);
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
        return new DefaultVariantAnnotationManager(annotator, getDBAdaptor());
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

    /**
     * Provide a new VariantStatisticsManager for creating and loading statistics.
     *
     * @return              A new instance of VariantStatisticsManager
     * @throws StorageEngineException  if there is an error creating the VariantStatisticsManager
     */
    public VariantStatisticsManager newVariantStatisticsManager() throws StorageEngineException {
        return new DefaultVariantStatisticsManager(getDBAdaptor());
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
     * Atomically updates the studyConfiguration before removing samples.
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
     * Atomically updates the StudyConfiguration after removing samples.
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
            cellBaseUtils = new CellBaseUtils(new CellBaseClient(species, assembly, clientConfiguration));
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
        return new VariantReaderUtils();
    }

    /**
     * Build the default StudyConfigurationManager. This method could be override by children classes if they want to use other class.
     *
     * @return A StudyConfigurationManager object
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
        setDefaultTimeout(options);
        return getDBAdaptor().getPhased(variant, studyName, sampleName, options, windowsSize);
    }

    protected void setDefaultTimeout(QueryOptions options) {
        int defaultTimeout = getOptions().getInt(DEFAULT_TIMEOUT.key(), DEFAULT_TIMEOUT.defaultValue());
        int maxTimeout = getOptions().getInt(MAX_TIMEOUT.key(), MAX_TIMEOUT.defaultValue());
        int timeout = options.getInt(QueryOptions.TIMEOUT, defaultTimeout);
        if (timeout > maxTimeout) {
            throw new VariantQueryException("Invalid timeout '" + timeout + "'. Max timeout is " + maxTimeout);
        } else if (timeout < 0) {
            throw new VariantQueryException("Invalid timeout '" + timeout + "'. Timeout must be positive");
        }
        options.put(QueryOptions.TIMEOUT, timeout);
    }

    public VariantQueryResult<Variant> get(Query query, QueryOptions options) {
        try {
            return (VariantQueryResult<Variant>) getOrIterator(query, options, false);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
        try {
            return (VariantDBIterator) getOrIterator(query, options, true);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected Object getOrIterator(Query query, QueryOptions options, boolean iterator) throws StorageEngineException {
        if (options == null) {
            options = QueryOptions.empty();
        }
        // TODO: Use CacheManager ?
        query = preProcessQuery(query, options);


        String specificSearchIndexSamples = inferSpecificSearchIndexSamplesCollection(
                query, options, getMetadataManager(), dbName);
        if (specificSearchIndexSamples != null) {
            try {
                if (iterator) {
                    return getVariantSearchManager().iterator(specificSearchIndexSamples, query, options);
                } else {
                    return getVariantSearchManager().query(specificSearchIndexSamples, query, options);
                }
            } catch (IOException | VariantSearchException e) {
                throw new VariantQueryException("Error querying Solr", e);
            }
        } else if (doQuerySearchManager(query, options)) {
            try {
                if (iterator) {
                    return getVariantSearchManager().iterator(dbName, query, options);
                } else {
                    return getVariantSearchManager().query(dbName, query, options);
                }
            } catch (IOException | VariantSearchException e) {
                throw new VariantQueryException("Error querying Solr", e);
            }
        } else {
            if (doIntersectWithSearch(query, options)) {
                // Intersect Solr+Engine

                int limit = options.getInt(QueryOptions.LIMIT, 0);
                int skip = options.getInt(QueryOptions.SKIP, 0);
                boolean pagination = skip > 0 || limit > 0;

                Iterator<?> variantsIterator;
                Number numTotalResults = null;
                AtomicLong searchCount = null;
                Boolean approxCount = null;
                Integer approxCountSamplingSize = null;

                Query searchEngineQuery = getSearchEngineQuery(query);
                Query engineQuery = getEngineQuery(query, options, getMetadataManager());

                // Do not count for iterator
                if (!iterator) {
                    if (isQueryCovered(query)) {
                        // If the query is fully covered, the numTotalResults from solr is correct.
                        searchCount = new AtomicLong();
                        numTotalResults = searchCount;
                        // Skip count in storage. We already know the numTotalResults
                        options.put(QueryOptions.SKIP_COUNT, true);
                        approxCount = false;
                    } else if (options.getBoolean(APPROXIMATE_COUNT.key(), APPROXIMATE_COUNT.defaultValue())) {
                        options.put(QueryOptions.SKIP_COUNT, true);
                        VariantQueryResult<Long> result = approximateCount(query, options);
                        numTotalResults = result.first();
                        approxCount = result.getApproximateCount();
                        approxCountSamplingSize = result.getApproximateCountSamplingSize();
                    }
                }

                if (pagination) {
                    if (isQueryCovered(query)) {
                        // We can use limit+skip directly in solr
                        variantsIterator = variantIdIteratorFromSearch(searchEngineQuery, limit, skip, searchCount);

                        // Remove limit and skip from Options for storage. The Search Engine already knows the pagination.
                        options = new QueryOptions(options);
                        options.remove(QueryOptions.LIMIT);
                        options.remove(QueryOptions.SKIP);
                    } else {
                        logger.debug("Client side pagination. limit : {} , skip : {}", limit, skip);
                        // Can't limit+skip only from solr. Need to limit+skip also in client side
                        variantsIterator = variantIdIteratorFromSearch(searchEngineQuery);
                    }
                } else {
                    variantsIterator = variantIdIteratorFromSearch(searchEngineQuery, Integer.MAX_VALUE, 0, searchCount);
                }

                VariantDBAdaptor dbAdaptor = getDBAdaptor();
                logger.debug("Intersect query " + engineQuery.toJson() + " options " + options.toJson());
                if (iterator) {
                    return dbAdaptor.iterator(variantsIterator, engineQuery, options);
                } else {
                    setDefaultTimeout(options);
                    VariantQueryResult<Variant> queryResult = dbAdaptor.get(variantsIterator, engineQuery, options);
                    if (numTotalResults != null) {
                        queryResult.setApproximateCount(approxCount);
                        queryResult.setApproximateCountSamplingSize(approxCountSamplingSize);
                        queryResult.setNumTotalResults(numTotalResults.longValue());
                    }
                    queryResult.setSource(SEARCH_ENGINE_ID + '+' + getStorageEngineId());
                    return queryResult;
                }
            } else {
                return getOrIteratorNotSearchIndex(query, options, iterator);
            }
        }
    }

    /**
     * The query won't use the search index, either because is not available, not necessary, or forbidden.
     *
     * @param query     Query
     * @param options   QueryOptions
     * @param iterator  Shall the resulting object be an iterator instead of a QueryResult
     * @return          QueryResult or Iterator with the variants that matches the query
     * @throws StorageEngineException StorageEngineException
     */
    protected Object getOrIteratorNotSearchIndex(Query query, QueryOptions options, boolean iterator) throws StorageEngineException {
        VariantDBAdaptor dbAdaptor = getDBAdaptor();
        if (iterator) {
            return dbAdaptor.iterator(query, options);
        } else {
            setDefaultTimeout(options);
            return dbAdaptor.get(query, options).setSource(getStorageEngineId());
        }
    }

    public Query preProcessQuery(Query originalQuery, QueryOptions options) throws StorageEngineException {
        // Copy input query! Do not modify original query!
        Query query = originalQuery == null ? new Query() : new Query(originalQuery);

        if (VariantQueryUtils.isValidParam(query, ANNOT_CLINICAL_SIGNIFICANCE)) {
            String v = query.getString(ANNOT_CLINICAL_SIGNIFICANCE.key());
            VariantQueryUtils.QueryOperation operator = VariantQueryUtils.checkOperator(v);
            List<String> values = VariantQueryUtils.splitValue(v, operator);
            List<String> clinicalSignificanceList = new ArrayList<>(values.size());
            for (String clinicalSignificance : values) {
                ClinicalSignificance enumValue = EnumUtils.getEnum(ClinicalSignificance.class, clinicalSignificance);
                if (enumValue == null) {
                    String key = clinicalSignificance.toLowerCase().replace(' ', '_');
                    enumValue = EnumUtils.getEnum(ClinicalSignificance.class, key);
                }
                if (enumValue == null) {
                    String key = clinicalSignificance.toLowerCase();
                    if (VariantAnnotationUtils.CLINVAR_CLINSIG_TO_ACMG.containsKey(key)) {
                        // No value set
                        enumValue = VariantAnnotationUtils.CLINVAR_CLINSIG_TO_ACMG.get(key);
                    }
                }
                if (enumValue != null) {
                    clinicalSignificance = enumValue.toString();
                } // else should throw exception?

                clinicalSignificanceList.add(clinicalSignificance);
            }
            query.put(ANNOT_CLINICAL_SIGNIFICANCE.key(), clinicalSignificanceList);
        }

        if (isValidParam(query, ANNOT_SIFT)) {
            String sift = query.getString(ANNOT_SIFT.key());
            String[] split = splitOperator(sift);
            if (StringUtils.isNotEmpty(split[0])) {
                throw VariantQueryException.malformedParam(ANNOT_SIFT, sift);
            }
            if (isValidParam(query, ANNOT_PROTEIN_SUBSTITUTION)) {
                String proteinSubstitution = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
                if (proteinSubstitution.contains("sift")) {
                    throw VariantQueryException.malformedParam(ANNOT_SIFT,
                            "Conflict with parameter \"" + ANNOT_PROTEIN_SUBSTITUTION.key() + "\"");
                }
                query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), proteinSubstitution + AND + "sift" + split[1] + split[2]);
            } else {
                query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift" + split[1] + split[2]);
            }
            query.remove(ANNOT_SIFT);
        }

        if (isValidParam(query, ANNOT_POLYPHEN)) {
            String polyphen = query.getString(ANNOT_POLYPHEN.key());
            String[] split = splitOperator(polyphen);
            if (StringUtils.isNotEmpty(split[0])) {
                throw VariantQueryException.malformedParam(ANNOT_POLYPHEN, polyphen);
            }
            if (isValidParam(query, ANNOT_PROTEIN_SUBSTITUTION)) {
                String proteinSubstitution = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
                if (proteinSubstitution.contains("sift")) {
                    throw VariantQueryException.malformedParam(ANNOT_SIFT,
                            "Conflict with parameter \"" + ANNOT_PROTEIN_SUBSTITUTION.key() + "\"");
                }
                query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), proteinSubstitution + AND + "polyphen" + split[1] + split[2]);
            } else {
                query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen" + split[1] + split[2]);
            }
            query.remove(ANNOT_POLYPHEN);
        }

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        StudyMetadata defaultStudy = getDefaultStudy(query, options, metadataManager);
        QueryOperation formatOperator = null;
        if (isValidParam(query, FORMAT)) {
            extractGenotypeFromFormatFilter(query);

            Pair<QueryOperation, Map<String, String>> pair = parseFormat(query);
            formatOperator = pair.getKey();

            for (Map.Entry<String, String> entry : pair.getValue().entrySet()) {
                String sampleName = entry.getKey();
                if (defaultStudy == null) {
                    throw VariantQueryException.missingStudyForSample(sampleName, metadataManager.getStudyNames(null));
                }
                Integer sampleId = metadataManager.getSampleId(defaultStudy.getId(), sampleName, true);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sampleName, defaultStudy.getName());
                }
                List<String> formats = splitValue(entry.getValue()).getValue();
                for (String format : formats) {
                    String[] split = splitOperator(format);
                    VariantFileHeaderComplexLine line = defaultStudy.getVariantHeaderLine("FORMAT", split[0]);
                    if (line == null) {
                        throw VariantQueryException.malformedParam(FORMAT, query.getString(FORMAT.key()),
                                "FORMAT field \"" + split[0] + "\" not found. Available keys in study: "
                                        + defaultStudy.getVariantHeaderLines("FORMAT").keySet());
                    }
                }
            }
        }

        if (isValidParam(query, INFO)) {
            Pair<QueryOperation, Map<String, String>> pair = parseInfo(query);
            if (isValidParam(query, FILE) && pair.getKey() != null) {
                QueryOperation fileOperator = checkOperator(query.getString(FILE.key()));
                if (fileOperator != null && pair.getKey() != fileOperator) {
                    throw VariantQueryException.mixedAndOrOperators(FILE, INFO);
                }
            }
            for (Map.Entry<String, String> entry : pair.getValue().entrySet()) {
                String fileName = entry.getKey();
                if (defaultStudy == null) {
                    throw VariantQueryException.missingStudyForFile(fileName, metadataManager.getStudyNames(null));
                }
                Integer fileId = metadataManager.getFileId(defaultStudy.getId(), fileName, true);
                if (fileId == null) {
                    throw VariantQueryException.fileNotFound(fileName, defaultStudy.getName());
                }
                List<String> infos = splitValue(entry.getValue()).getValue();
                for (String info : infos) {
                    String[] split = splitOperator(info);
                    VariantFileHeaderComplexLine line = defaultStudy.getVariantHeaderLine("INFO", split[0]);
                    if (line == null) {
                        throw VariantQueryException.malformedParam(INFO, query.getString(INFO.key()),
                                "INFO field \"" + split[0] + "\" not found. Available keys in study: "
                                        + defaultStudy.getVariantHeaderLines("INFO").keySet());
                    }
                }
            }
        }

        QueryOperation genotypeOperator = null;
        VariantQueryParam genotypeParam = null;
        if (isValidParam(query, SAMPLE)) {
            if (isValidParam(query, GENOTYPE)) {
                throw VariantQueryException.malformedParam(SAMPLE, query.getString(SAMPLE.key()),
                        "Can not be used along with filter \"" + GENOTYPE.key() + '"');
            }
            genotypeParam = SAMPLE;

            if (defaultStudy == null) {
                throw VariantQueryException.missingStudyForSamples(query.getAsStringList(SAMPLE.key()),
                        metadataManager.getStudyNames(null));
            }
            List<String> loadedGenotypes = defaultStudy.getAttributes().getAsStringList(LOADED_GENOTYPES.key());
            if (CollectionUtils.isEmpty(loadedGenotypes)) {
                loadedGenotypes = Arrays.asList(
                        "0/0", "0|0",
                        "0/1", "1/0", "1/1", "./.",
                        "0|1", "1|0", "1|1", ".|.",
                        "0|2", "2|0", "2|1", "1|2", "2|2",
                        "0/2", "2/0", "2/1", "1/2", "2/2",
                        GenotypeClass.UNKNOWN_GENOTYPE);
            }
            String genotypes;
            if (loadedGenotypes.contains(GenotypeClass.NA_GT_VALUE)
                    || defaultStudy.getAttributes().getBoolean(EXCLUDE_GENOTYPES.key(), EXCLUDE_GENOTYPES.defaultValue())) {
                genotypes = GenotypeClass.NA_GT_VALUE;
            } else {
                genotypes = loadedGenotypes.stream().filter(gt -> {
                    Genotype genotype;
                    try {
                        genotype = new Genotype(gt);
                    } catch (IllegalArgumentException e) {
                        return false;
                    }
                    for (int i : genotype.getAllelesIdx()) {
                        if (i == 1) {
                            return true;
                        }
                    }
                    return false;
                }).collect(Collectors.joining(","));
            }

            Pair<QueryOperation, List<String>> pair = VariantQueryUtils.splitValue(query.getString(SAMPLE.key()));
            genotypeOperator = pair.getLeft();

            StringBuilder sb = new StringBuilder();
            for (String sample : pair.getValue()) {
                if (sb.length() > 0) {
                    sb.append(genotypeOperator.separator());
                }
                sb.append(sample).append(IS).append(genotypes);
            }
            query.remove(SAMPLE.key());
            query.put(GENOTYPE.key(), sb.toString());
        } else if (isValidParam(query, GENOTYPE)) {
            genotypeParam = GENOTYPE;

            List<String> loadedGenotypes = defaultStudy.getAttributes().getAsStringList(LOADED_GENOTYPES.key());
            if (CollectionUtils.isEmpty(loadedGenotypes)) {
                loadedGenotypes = Arrays.asList(
                        "0/0", "0|0",
                        "0/1", "1/0", "1/1", "./.",
                        "0|1", "1|0", "1|1", ".|.",
                        "0|2", "2|0", "2|1", "1|2", "2|2",
                        "0/2", "2/0", "2/1", "1/2", "2/2",
                        GenotypeClass.UNKNOWN_GENOTYPE);
            }

            Map<Object, List<String>> map = new LinkedHashMap<>();
            genotypeOperator = VariantQueryUtils.parseGenotypeFilter(query.getString(GENOTYPE.key()), map);

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Object, List<String>> entry : map.entrySet()) {
//                List<String> genotypes = GenotypeClass.filter(entry.getValue(), loadedGenotypes, defaultGenotypes);
                List<String> genotypes = GenotypeClass.filter(entry.getValue(), loadedGenotypes);

                if (genotypes.isEmpty()) {
                    // TODO: Do fast fail, NO RESULTS!
                    genotypes = Collections.singletonList("x/x");
                }

                if (sb.length() > 0) {
                    sb.append(genotypeOperator.separator());
                }
                sb.append(entry.getKey()).append(IS);
                for (int i = 0; i < genotypes.size(); i++) {
                    if (i > 0) {
                        sb.append(OR);
                    }
                    sb.append(genotypes.get(i));
                }
            }
            query.put(GENOTYPE.key(), sb.toString());
        }

        if (formatOperator != null && genotypeOperator != null && formatOperator != genotypeOperator) {
            throw VariantQueryException.mixedAndOrOperators(FORMAT, genotypeParam);
        }

        if (!isValidParam(query, INCLUDE_STUDY) || !isValidParam(query, INCLUDE_SAMPLE) || !isValidParam(query, INCLUDE_FILE)) {
            VariantQueryFields selectVariantElements =
                    parseVariantQueryFields(query, options, metadataManager);
            if (!isValidParam(query, INCLUDE_STUDY)) {
                List<String> includeStudy = new ArrayList<>();
                for (Integer studyId : selectVariantElements.getStudies()) {
                    includeStudy.add(selectVariantElements.getStudyMetadatas().get(studyId).getName());
                }
                if (includeStudy.isEmpty()) {
                    query.put(INCLUDE_STUDY.key(), NONE);
                } else {
                    query.put(INCLUDE_STUDY.key(), includeStudy);
                }
            }
            if (!isValidParam(query, INCLUDE_SAMPLE)) {
                List<String> includeSample = selectVariantElements.getSamples()
                        .entrySet()
                        .stream()
                        .flatMap(e -> e.getValue()
                                .stream()
                                .map(s -> metadataManager.getSampleName(e.getKey(), s)))
                        .collect(Collectors.toList());
                if (includeSample.isEmpty()) {
                    query.put(INCLUDE_SAMPLE.key(), NONE);
                } else {
                    query.put(INCLUDE_SAMPLE.key(), includeSample);
                }
            }
            if (!isValidParam(query, INCLUDE_FILE)) {
                List<String> includeFile = selectVariantElements.getFiles()
                        .entrySet()
                        .stream()
                        .flatMap(e -> e.getValue()
                                .stream()
                                .map(f -> metadataManager.getFileName(e.getKey(), f)))
                        .collect(Collectors.toList());
                if (includeFile.isEmpty()) {
                    query.put(INCLUDE_FILE.key(), NONE);
                } else {
                    query.put(INCLUDE_FILE.key(), includeFile);
                }
            }
        }

        List<String> formats = getIncludeFormats(query);
        if (formats == null) {
            query.put(INCLUDE_FORMAT.key(), ALL);
        } else if (formats.isEmpty()) {
            query.put(INCLUDE_FORMAT.key(), NONE);
        } else {
            query.put(INCLUDE_FORMAT.key(), formats);
        }
        query.remove(INCLUDE_GENOTYPE.key(), formats);

        return query;
    }

    /**
     * Decide if a query should be resolved using SearchManager or not.
     *
     * @param query     Query
     * @param options   QueryOptions
     * @return          true if should resolve only with SearchManager
     * @throws StorageEngineException StorageEngineException
     */
    protected boolean doQuerySearchManager(Query query, QueryOptions options) throws StorageEngineException {
        return !UseSearchIndex.from(options).equals(UseSearchIndex.NO) // YES or AUTO
                && isQueryCovered(query)
                && (options.getBoolean(QueryOptions.COUNT) || isIncludeCovered(options))
                && searchActiveAndAlive();
    }

    /**
     * Decide if a query should be resolved intersecting with SearchManager or not.
     *
     * @param query       Query
     * @param options     QueryOptions
     * @return            true if should intersect
     * @throws StorageEngineException StorageEngineException
     */
    protected boolean doIntersectWithSearch(Query query, QueryOptions options) throws StorageEngineException {
        UseSearchIndex useSearchIndex = UseSearchIndex.from(options);

        final boolean intersect;
        boolean active = searchActiveAndAlive();

        if (!getOptions().getBoolean(INTERSECT_ACTIVE.key(), INTERSECT_ACTIVE.defaultValue()) || useSearchIndex.equals(UseSearchIndex.NO)) {
            // If intersect is not active, do not intersect.
            intersect = false;
        } else if (getOptions().getBoolean(INTERSECT_ALWAYS.key(), INTERSECT_ALWAYS.defaultValue())) {
            // If always intersect, intersect if available
            intersect = active;
        } else if (!active) {
            intersect = false;
        } else if (useSearchIndex.equals(UseSearchIndex.YES) || VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_TRAIT)) {
            intersect = true;
        } else {
            // TODO: Improve this heuristic
            // Count only real params
            Collection<VariantQueryParam> coveredParams = coveredParams(query);
            coveredParams.removeAll(MODIFIER_QUERY_PARAMS);
            int intersectParamsThreshold = getOptions().getInt(INTERSECT_PARAMS_THRESHOLD.key(), INTERSECT_PARAMS_THRESHOLD.defaultValue());
            intersect = coveredParams.size() >= intersectParamsThreshold;
        }

        if (!intersect) {
            if (useSearchIndex.equals(UseSearchIndex.YES)) {
                throw new VariantQueryException("Unable to use search index. SearchEngine is not available");
            } else if (VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_TRAIT)) {
                throw VariantQueryException.unsupportedVariantQueryFilter(VariantQueryParam.ANNOT_TRAIT, getStorageEngineId(),
                        "Search engine is required.");
            }
        }
        return intersect;
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
        if (!doQuerySearchManager(query, new QueryOptions(QueryOptions.COUNT, true))) {
            return getDBAdaptor().count(query);
        } else {
            try {
                StopWatch watch = StopWatch.createStarted();
                long count = getVariantSearchManager().count(dbName, query);
                int time = (int) watch.getTime(TimeUnit.MILLISECONDS);
                return new QueryResult<>("count", time, 1, 1, "", "", Collections.singletonList(count));
            } catch (IOException | VariantSearchException e) {
                throw new VariantQueryException("Error querying Solr", e);
            }
        }
    }

    public VariantQueryResult<Long> approximateCount(Query query, QueryOptions options) throws StorageEngineException {
        long count;
        boolean approxCount = true;
        int sampling = 0;
        StopWatch watch = StopWatch.createStarted();
        try {
            if (doQuerySearchManager(query, new QueryOptions(QueryOptions.COUNT, true))) {
                approxCount = false;
                count = getVariantSearchManager().count(dbName, query);
            } else {
                sampling = options.getInt(APPROXIMATE_COUNT_SAMPLING_SIZE.key(),
                        getOptions().getInt(APPROXIMATE_COUNT_SAMPLING_SIZE.key(), APPROXIMATE_COUNT_SAMPLING_SIZE.defaultValue()));
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, sampling);

                Query searchEngineQuery = getSearchEngineQuery(query);
                Query engineQuery = getEngineQuery(query, options, getMetadataManager());

                VariantQueryResult<VariantSearchModel> nativeResult = getVariantSearchManager()
                        .nativeQuery(dbName, searchEngineQuery, queryOptions);
                List<String> variantIds = nativeResult.getResult().stream().map(VariantSearchModel::getId).collect(Collectors.toList());
                // Adjust numSamples if the results from SearchManager is smaller than numSamples
                // If this happens, the count is not approximated
                if (variantIds.size() < sampling) {
                    approxCount = false;
                    sampling = variantIds.size();
                }
                long numSearchResults = nativeResult.getNumTotalResults();

                engineQuery.put(ID.key(), variantIds);
                long numResults = getDBAdaptor().count(engineQuery).first();
                logger.debug("NumResults: {}, NumSearchResults: {}, NumSamples: {}", numResults, numSearchResults, sampling);
                if (approxCount) {
                    count = (long) ((numResults / (float) sampling) * numSearchResults);
                } else {
                    count = numResults;
                }
            }
        } catch (IOException | VariantSearchException e) {
            throw new VariantQueryException("Error querying Solr", e);
        }
        int time = (int) watch.getTime(TimeUnit.MILLISECONDS);
        return new VariantQueryResult<>("count", time, 1, 1, "", "", Collections.singletonList(count), null,
                SEARCH_ENGINE_ID + '+' + getStorageEngineId(), approxCount, approxCount ? sampling : null);
    }

    /**
     * Fetch facet (i.e., counts) resulting of executing the query in the database.
     *
     * @param query          Query to be executed in the database to filter variants
     * @param options        Query modifiers, accepted values are: facet fields and facet ranges
     * @return               A FacetedQueryResult with the result of the query
     */
    public FacetQueryResult facet(Query query, QueryOptions options) {
        if (query == null) {
            query = new Query();
        }
        if (options == null) {
            options = new QueryOptions();
        }

        FacetQueryResult facetedQueryResult;
        try {
            facetedQueryResult = getVariantSearchManager().facetedQuery(dbName, query, options);
        } catch (IOException | SolrException | StorageEngineException | VariantSearchException e) {
            throw Throwables.propagate(e);
        }

        return facetedQueryResult;
    }

    protected boolean searchActiveAndAlive() throws StorageEngineException {
        return configuration.getSearch().isActive() && getVariantSearchManager() != null && getVariantSearchManager().isAlive(dbName);
    }


    protected Iterator<String> variantIdIteratorFromSearch(Query query) throws StorageEngineException {
        return variantIdIteratorFromSearch(query, Integer.MAX_VALUE, 0, null);
    }

    protected Iterator<String> variantIdIteratorFromSearch(Query query, int limit, int skip, AtomicLong numTotalResults)
            throws StorageEngineException {
        Iterator<String> variantsIterator;
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.LIMIT, limit)
                .append(QueryOptions.SKIP, skip)
                .append(QueryOptions.INCLUDE, VariantField.ID.fieldName());
        try {
            // Do not iterate for small queries
            if (limit < 10000) {
                VariantQueryResult<VariantSearchModel> nativeResult = getVariantSearchManager().nativeQuery(dbName, query, queryOptions);
                if (numTotalResults != null) {
                    numTotalResults.set(nativeResult.getNumTotalResults());
                }
                variantsIterator = nativeResult.getResult()
                        .stream()
                        .map(VariantSearchModel::getId)
                        .iterator();
            } else {
                VariantSearchSolrIterator nativeIterator = getVariantSearchManager().nativeIterator(dbName, query, queryOptions);
                if (numTotalResults != null) {
                    numTotalResults.set(nativeIterator.getNumFound());
                }
                variantsIterator = Iterators.transform(nativeIterator, VariantSearchModel::getId);
            }
        } catch (VariantSearchException | IOException e) {
            throw new VariantQueryException("Error querying " + VariantSearchManager.SEARCH_ENGINE_ID, e);
        }
        return variantsIterator;
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
    }
}

