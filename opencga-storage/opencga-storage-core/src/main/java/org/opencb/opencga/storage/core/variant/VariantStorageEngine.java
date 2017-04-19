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

import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.ExportMetadata;
import org.opencb.opencga.storage.core.metadata.FileStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.search.VariantSearchManager;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.DEFAULT_TIMEOUT;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.MAX_TIMEOUT;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageEngine extends StorageEngine<VariantDBAdaptor> {

    private final AtomicReference<VariantSearchManager> variantSearchManager = new AtomicReference<>();
    private Logger logger = LoggerFactory.getLogger(VariantStorageEngine.class);
    private CellBaseUtils cellBaseUtils;


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
        @Deprecated
        DB_NAME("database.name", "opencga"),

        TRANSFORM_BATCH_SIZE("transform.batch.size", 200),
        TRANSFORM_THREADS("transform.threads", 4),
        TRANSFORM_FORMAT("transform.format", "avro"),
        LOAD_BATCH_SIZE("load.batch.size", 100),
        LOAD_THREADS("load.threads", 6),

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
     * @param studiesOldNewMap  Map from old to new StudyConfiguration, in case of name remapping
     * @param params       Other options
     * @throws IOException      if there is any I/O error
     * @throws StorageEngineException  if there si any error loading the variants
     * */
    public void importData(URI inputFile, ExportMetadata metadata, Map<StudyConfiguration, StudyConfiguration> studiesOldNewMap,
                           ObjectMap params)
            throws StorageEngineException, IOException {
        VariantImporter variantImporter = newVariantImporter();
        variantImporter.importData(inputFile, metadata, studiesOldNewMap);
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
        VariantExporter exporter = newVariantExporter();
        exporter.export(outputFile, outputFormat, query, queryOptions);
    }

    /**
     * Creates a new {@link VariantExporter} for the current backend.
     * The default implementation iterates locally through the database.
     *
     * @return              new VariantExporter
     * @throws StorageEngineException  if there is an error creating the VariantExporter
     */
    protected VariantExporter newVariantExporter() throws StorageEngineException {
        return new VariantExporter(getDBAdaptor());
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
     * @param query     Query to select variants to annotate
     * @param params    Other params
     * @throws VariantAnnotatorException    If the annotation goes wrong
     * @throws StorageEngineException       If there is any problem related with the StorageEngine
     * @throws IOException                  If there is any IO problem
     */
    public void annotate(Query query, ObjectMap params)
            throws VariantAnnotatorException, StorageEngineException, IOException {
        VariantAnnotator annotator = VariantAnnotatorFactory.buildVariantAnnotator(configuration, getStorageEngineId(), params);
        VariantAnnotationManager annotationManager = newVariantAnnotationManager(annotator);
        annotationManager.annotate(query, params);
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
            try {
                VariantDBAdaptor dbAdaptor = getDBAdaptor();
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
                    annotationQuery.put(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
                }
                annotationQuery.put(VariantQueryParam.STUDIES.key(),
                        Collections.singletonList(studyId));    // annotate just the indexed variants
                // annotate just the indexed variants
                annotationQuery.put(VariantQueryParam.FILES.key(), fileIds);

                QueryOptions annotationOptions = new QueryOptions()
                        .append(DefaultVariantAnnotationManager.OUT_DIR, outdirUri.getPath())
                        .append(DefaultVariantAnnotationManager.FILE_NAME, dbName + "." + TimeUtils.getTime());

                annotate(annotationQuery, annotationOptions);
            } catch (RuntimeException | StorageEngineException | VariantAnnotatorException | IOException e) {
                throw new StoragePipelineException("Error annotating.", e, results);
            }
        }
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
    public void calculateStats(String study, List<String> cohorts, QueryOptions options)
            throws StorageEngineException, IOException {
        VariantStatisticsManager statisticsManager = newVariantStatisticsManager();
        statisticsManager.calculateStatistics(study, cohorts, options);
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
            try {
                VariantDBAdaptor dbAdaptor = getDBAdaptor();
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
                URI statsOutputUri = output.resolve(VariantStoragePipeline.buildFilename(studyConfiguration.getStudyName(), fileIds.get(0))
                        + "." + TimeUtils.getTime());
                statsOptions.put(DefaultVariantStatisticsManager.OUTPUT, statsOutputUri.toString());

                statsOptions.remove(Options.FILE_ID.key());

                List<String> cohorts = Collections.singletonList(StudyEntry.DEFAULT_COHORT);
                calculateStats(studyConfiguration.getStudyName(), cohorts, statsOptions);
            } catch (Exception e) {
                throw new StoragePipelineException("Can't calculate stats.", e, results);
            }
        }
    }

    /**
     * Provide a new VariantAnnotationManager for creating and loading annotations.
     *
     * @return              A new instance of VariantAnnotationManager
     * @throws StorageEngineException  if there is an error creating the VariantStatisticsManager
     */
    public VariantStatisticsManager newVariantStatisticsManager() throws StorageEngineException {
        return new DefaultVariantStatisticsManager(getDBAdaptor());
    }


    public void searchIndex() throws StorageEngineException, IOException, VariantSearchException {
        searchIndex(new Query(), new QueryOptions());
    }

    public void searchIndex(Query query, QueryOptions queryOptions) throws StorageEngineException, IOException,
            VariantSearchException {

        VariantDBAdaptor dbAdaptor = getDBAdaptor();
        StudyConfigurationManager studyConfigurationManager = getStudyConfigurationManager();

        if (configuration.getSearch().getActive() && getVariantSearchManager().isAlive(dbName)) {
            // first, create the collection it it does not exist
            if (!getVariantSearchManager().existCollection(dbName)) {
                // by default: config=OpenCGAConfSet, shards=1, replicas=1
                logger.info("Creating Solr collection " + dbName);
                getVariantSearchManager().createCollection(dbName);
            } else {
                logger.info("Solr collection '" + dbName + "' exists.");
            }

            // then, load variants
            queryOptions = new QueryOptions();
            queryOptions.put(QueryOptions.EXCLUDE, Arrays.asList(VariantField.STUDIES_SAMPLES_DATA, VariantField.STUDIES_FILES));
            VariantDBIterator iterator = dbAdaptor.iterator(query, queryOptions);
            getVariantSearchManager().load(dbName, iterator);
        }
        dbAdaptor.close();
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
    public void testConnection() throws StorageEngineException {}

    public CellBaseUtils getCellBaseUtils() throws StorageEngineException {
        if (cellBaseUtils == null) {
            StudyConfigurationManager studyConfigurationManager = getStudyConfigurationManager();
            List<String> studyNames = studyConfigurationManager.getStudyNames(QueryOptions.empty());
            String species = getOptions().getString(VariantAnnotationManager.SPECIES);
            String assembly = getOptions().getString(VariantAnnotationManager.ASSEMBLY);
            if (!studyNames.isEmpty()) {
                StudyConfiguration sc = studyConfigurationManager
                        .getStudyConfiguration(studyNames.get(0), QueryOptions.empty()).first();
                species = sc.getAttributes().getString(VariantAnnotationManager.SPECIES, species);
                assembly= sc.getAttributes().getString(VariantAnnotationManager.ASSEMBLY, assembly);
            }
            ClientConfiguration clientConfiguration = configuration.getCellbase().toClientConfiguration();
            if (StringUtils.isEmpty(species)) {
                species = clientConfiguration.getDefaultSpecies();
            }
            species = AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName(species);
            cellBaseUtils = new CellBaseUtils(new CellBaseClient(species, assembly, clientConfiguration));
        }
        return cellBaseUtils;
    }

    public ObjectMap getOptions() {
        return configuration.getStorageEngine(storageEngineId).getVariant().getOptions();
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
    public StudyConfigurationManager getStudyConfigurationManager() throws StorageEngineException {
        return new StudyConfigurationManager(new FileStudyConfigurationAdaptor());
    }

    protected VariantSearchManager getVariantSearchManager() throws StorageEngineException {
        if (variantSearchManager.get() == null) {
            synchronized (variantSearchManager) {
                if (variantSearchManager.get() == null) {
                    variantSearchManager.set(new VariantSearchManager(getStudyConfigurationManager(), getCellBaseUtils(), configuration));
                }
            }
        }
        return variantSearchManager.get();
    }

    public VariantQueryResult<Variant> get(Query query, QueryOptions options) throws StorageEngineException {
        setDefaultTimeout(options);
        return getDBAdaptor().get(query, options);
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
        if (timeout > maxTimeout || timeout < 0) {
            timeout = maxTimeout;
        }
        options.put(QueryOptions.TIMEOUT, timeout);
    }

    public VariantDBIterator iterator(Query query, QueryOptions options) throws StorageEngineException {
        VariantDBAdaptor dbAdaptor = getDBAdaptor();
        VariantDBIterator iterator = dbAdaptor.iterator(query, options);
        iterator.addCloseable(dbAdaptor);
        return iterator;
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
        return getDBAdaptor().count(query);
    }

    public QueryResult<Long> aproxCount(Query query, QueryOptions options) throws StorageEngineException {
        return getDBAdaptor().count(query);
    }

    /**
     * Fetch facet (i.e., counts) resulting of executing the query in the database.
     *
     * @param query          Query to be executed in the database to filter variants
     * @param options        Query modifiers, accepted values are: facet fields and facet ranges
     * @return               A FacetedQueryResult with the result of the query
     */
    public FacetedQueryResult facet(Query query, QueryOptions options) {

        if (query == null) {
            query = new Query();
        }
        if (options == null) {
            options = new QueryOptions();
        }

        FacetedQueryResult queryResult;
        try {
            queryResult = getVariantSearchManager().facetedQuery(dbName, query, options);
        } catch (IOException | VariantSearchException | StorageEngineException e) {
            throw Throwables.propagate(e);
        }

        return queryResult;
    }

    protected boolean searchActiveAndAlive() throws StorageEngineException {
        return configuration.getSearch().getActive() && getVariantSearchManager() != null && getVariantSearchManager().isAlive(dbName);
    }

    @Override
    public void close() throws IOException {
        cellBaseUtils = null;
    }
}
