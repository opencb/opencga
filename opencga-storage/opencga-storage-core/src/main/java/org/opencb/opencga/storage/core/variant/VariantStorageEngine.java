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
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateParams;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.storage.core.StoragePipelineResult;
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
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.executors.*;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.opencb.opencga.storage.core.variant.search.SamplesSearchIndexVariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.search.SearchIndexVariantAggregationExecutor;
import org.opencb.opencga.storage.core.variant.search.SearchIndexVariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.search.VariantSecondaryIndexFilter;
import org.opencb.opencga.storage.core.variant.search.solr.SolrInputDocumentDataWriter;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.opencb.opencga.storage.core.variant.stats.SampleVariantStatsAggregationQuery;
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

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.*;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.core.variant.search.VariantSearchUtils.buildSamplesIndexCollectionName;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageEngine extends StorageEngine<VariantDBAdaptor> implements VariantIterable {

    private final AtomicReference<VariantSearchManager> variantSearchManager = new AtomicReference<>();
    private final List<VariantQueryExecutor> lazyVariantQueryExecutorsList = new ArrayList<>();
    private final List<VariantAggregationExecutor> lazyVariantAggregationExecutorsList = new ArrayList<>();
    private CellBaseUtils cellBaseUtils;

    public static final String REMOVE_OPERATION_NAME = TaskMetadata.Type.REMOVE.name().toLowerCase();

    private Logger logger = LoggerFactory.getLogger(VariantStorageEngine.class);
    private ObjectMap options;

    public enum MergeMode {
        BASIC,
        ADVANCED;

        public static MergeMode from(ObjectMap options) {
            String mergeModeStr = options.getString(VariantStorageOptions.MERGE_MODE.key(),
                    VariantStorageOptions.MERGE_MODE.defaultValue().toString());
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

    public enum SplitData {
        CHROMOSOME,
        REGION,
        MULTI;

        public static SplitData from(ObjectMap options) {
            Objects.requireNonNull(options);
            String loadSplitDataStr = options.getString(LOAD_SPLIT_DATA.key());
            boolean multiFile = options.getBoolean(LOAD_MULTI_FILE_DATA.key());
            if (StringUtils.isNotEmpty(loadSplitDataStr) && multiFile) {
                throw new IllegalArgumentException("Unable to mix loadSplitFile and loadMultiFile");
            }
            if (StringUtils.isEmpty(loadSplitDataStr) && !multiFile) {
                return null;
            }
            if (multiFile) {
                return MULTI;
            } else {
                switch (loadSplitDataStr.toLowerCase()) {
                    case "chromosome":
                        return CHROMOSOME;
                    case "region":
                        return REGION;
                    case "multi":
                        return MULTI; // FIXME: This shold not be allowed
                    default:
                        throw new IllegalArgumentException("Unknown split file method by '" + loadSplitDataStr + "'. "
                                + "Available values: " + CHROMOSOME + ", " + REGION);
                }
            }
        }
    }

    @Deprecated
    public VariantStorageEngine() {}

    public VariantStorageEngine(StorageConfiguration configuration) {
        this(configuration.getVariant().getDefaultEngine(), configuration);
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
        if (outputFormat == VariantOutputFormat.VCF || outputFormat == VariantOutputFormat.VCF_GZ) {
            if (!isValidParam(query, VariantQueryParam.UNKNOWN_GENOTYPE)) {
                query.put(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");
            }
        }
        ParsedVariantQuery parsedVariantQuery = parseQuery(query, queryOptions);
        exporter.export(outputFile, outputFormat, variantsFile, parsedVariantQuery);
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
        return new VariantExporter(this, metadataFactory, ioConnectorProvider);
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
     * Load an input file as VariantAnnotation.
     *
     * @param inputFile Annotation file to load
     * @param params    Other params
     * @throws VariantAnnotatorException    If the annotation goes wrong
     * @throws StorageEngineException       If there is any problem related with the StorageEngine
     * @return number of annotated variants
     * @throws IOException                  If there is any IO problem
     */
    public long annotationLoad(URI inputFile, ObjectMap params) throws VariantAnnotatorException, StorageEngineException, IOException {
        // Merge with configuration
        ObjectMap options = getMergedOptions(params);
        options.put(VariantAnnotationManager.LOAD_FILE, inputFile.toString());
        VariantAnnotationManager annotationManager = newVariantAnnotationManager(options);
        return annotationManager.annotate(new Query(), options);
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
        if (files != null && !files.isEmpty() && options.getBoolean(VariantStorageOptions.ANNOTATE.key(),
                VariantStorageOptions.ANNOTATE.defaultValue())) {
            try {

                String studyName = options.getString(VariantStorageOptions.STUDY.key());
                VariantStorageMetadataManager metadataManager = getMetadataManager();
                int studyId = metadataManager.getStudyId(studyName);

                List<String> fileNames = new ArrayList<>(files.size());
                for (URI uri : files) {
                    Integer fileId = metadataManager.getFileId(studyId, VariantReaderUtils.getOriginalFromTransformedFile(uri));
                    fileNames.add(metadataManager.getFileName(studyId, fileId));
                }

                // Annotate only the new indexed variants
                Query annotationQuery = new Query();
                if (!options.getBoolean(ANNOTATION_OVERWEITE.key(), false)) {
                    annotationQuery.put(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
                }
                annotationQuery.put(VariantQueryParam.STUDY.key(), Collections.singletonList(studyName));
                annotationQuery.put(VariantQueryParam.FILE.key(), fileNames);

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

    public DataResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options) throws StorageEngineException {
        options = addDefaultLimit(options, getOptions());
        return getDBAdaptor().getAnnotation(name, query, options);
    }

    public DataResult<ProjectMetadata.VariantAnnotationMetadata> getAnnotationMetadata(String name) throws StorageEngineException {
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
        return new DataResult<>(((int) started.getTime(TimeUnit.MILLISECONDS)), Collections.emptyList(), list.size(), list, list.size());
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
        ProjectMetadata projectMetadata = getMetadataManager().getProjectMetadata(params);
        VariantAnnotator annotator = VariantAnnotatorFactory.buildVariantAnnotator(
                configuration, projectMetadata, getMergedOptions(params));
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
        return new DefaultVariantAnnotationManager(annotator, getDBAdaptor(), ioConnectorProvider);
    }

    /**
     *
     * @param study     Study
     * @param cohorts   Cohorts to calculate stats
     * @param options   Other options
     *                  {@link VariantStorageOptions#STATS_AGGREGATION_MAPPING_FILE}
     *                  {@link VariantStorageOptions#STATS_OVERWRITE}
     *                  {@link VariantStorageOptions#LOAD_THREADS}
     *                  {@link VariantStorageOptions#LOAD_BATCH_SIZE}
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
                && options.getBoolean(VariantStorageOptions.STATS_CALCULATE.key(), VariantStorageOptions.STATS_CALCULATE.defaultValue())) {
            // TODO add filters
            try {
                VariantDBAdaptor dbAdaptor = getDBAdaptor();
                logger.debug("Calculating stats for files: '{}'...", files.toString());

                String studyName = options.getString(VariantStorageOptions.STUDY.key());
                QueryOptions statsOptions = new QueryOptions(options);
                VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
                StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyName);

                List<Integer> fileIds = new ArrayList<>(files.size());
                for (URI uri : files) {
                    String fileName = VariantReaderUtils.getOriginalFromTransformedFile(uri);
                    fileIds.add(metadataManager.getFileId(studyMetadata.getId(), fileName));
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

    public void deleteStats(String study, Collection<String> cohorts, ObjectMap params) throws StorageEngineException {
        throw new UnsupportedOperationException("Unsupported deleteStats");
    }

    /**
     * Build the sample index. For advanced users only.
     * SampleIndex is built while loading data, so this operation should be executed only to rebuild the index,
     * when changing some parameters.
     *
     * @param study   Study
     * @param samples List of samples. Use {@link VariantQueryUtils#ALL} to index all samples.
     * @param options Other options
     * @throws StorageEngineException in an error occurs
     */
    public void sampleIndex(String study, List<String> samples, ObjectMap options) throws StorageEngineException {
        throw new UnsupportedOperationException("Unsupported sampleIndex");
    }

    /**
     * Annotate the sample index. For advanced users only.
     * SampleIndex should be annotated after the annotation process, so this operation should be executed only
     * to reannotate the index,when changing the annotation version.
     *
     * @param study   Study
     * @param samples List of samples. Use {@link VariantQueryUtils#ALL} to index all samples.
     * @param options Other options
     * @throws StorageEngineException in an error occurs
     */
    public void sampleIndexAnnotate(String study, List<String> samples, ObjectMap options) throws StorageEngineException {
        throw new UnsupportedOperationException("Unsupported sampleIndex annotate");
    }

    /**
     * Build the family index given a list of trios.
     * The Family Index is used alongside with the SampleIndex to speed up queries involving children and parents.
     *
     * @param study   Study
     * @param trios List of trios "father, mother, child".
     *              Missing parents in trios are specified with "-",
     *              If a family has two children, two trios should be defined.
     * @param options Other options
     * @throws StorageEngineException in an error occurs
     * @return List of trios used to index. Empty if there was nothing to do.
     */
    public DataResult<List<String>> familyIndex(String study, List<List<String>> trios, ObjectMap options) throws StorageEngineException {
        throw new UnsupportedOperationException("Unsupported familyIndex");
    }

    public DataResult<List<String>> familyIndexUpdate(String study, ObjectMap options) throws StorageEngineException {
        StudyMetadata studyMetadata = getMetadataManager().getStudyMetadata(study);
        int studyId = studyMetadata.getId();
        int version = studyMetadata.getSampleIndexConfigurationLatest().getVersion();
        List<List<String>> trios = new LinkedList<>();
        for (SampleMetadata sampleMetadata : getMetadataManager().sampleMetadataIterable(studyId)) {
            if (sampleMetadata.isFamilyIndexDefined()) {
                if (sampleMetadata.getFamilyIndexStatus(version) != TaskMetadata.Status.READY) {
                    // This sample's family index needs to be updated
                    String father;
                    if (sampleMetadata.getFather() == null) {
                        father = "-";
                    } else {
                        father = getMetadataManager().getSampleName(studyId, sampleMetadata.getFather());
                    }
                    String mother;
                    if (sampleMetadata.getMother() == null) {
                        mother = "-";
                    } else {
                        mother = getMetadataManager().getSampleName(studyId, sampleMetadata.getMother());
                    }
                    trios.add(Arrays.asList(father, mother, sampleMetadata.getName()));
                }
            }
        }
        if (trios.isEmpty()) {
            logger.info("Nothing to do!");
            return new DataResult<List<String>>().setEvents(Collections.singletonList(new Event(Event.Type.INFO, "Nothing to do")));
        } else {
            return familyIndex(study, trios, options);
        }
    }

    /**
     * Provide a new VariantStatisticsManager for creating and loading statistics.
     *
     * @return              A new instance of VariantStatisticsManager
     * @throws StorageEngineException  if there is an error creating the VariantStatisticsManager
     */
    public VariantStatisticsManager newVariantStatisticsManager() throws StorageEngineException {
        return new DefaultVariantStatisticsManager(getDBAdaptor(), ioConnectorProvider);
    }

    /**
     *
     * @param study     Study
     * @param params    Aggregate Family params
     * @param options   Other options
     * @throws StorageEngineException if there is any error
     */
    public void aggregateFamily(String study, VariantAggregateFamilyParams params, ObjectMap options) throws StorageEngineException {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @param study     Study
     * @param params    Aggregate Params
     * @param options   Other options
     * @throws StorageEngineException if there is any error
     */
    public void aggregate(String study, VariantAggregateParams params, ObjectMap options) throws StorageEngineException {
        throw new UnsupportedOperationException();
    }

    public VariantSearchLoadResult secondaryIndex() throws StorageEngineException, IOException, VariantSearchException {
        return secondaryIndex(new Query(), new QueryOptions(), false);
    }

    public VariantSearchLoadResult secondaryIndex(Query inputQuery, QueryOptions inputQueryOptions, boolean overwrite)
            throws StorageEngineException, IOException, VariantSearchException {
        Query query = VariantQueryUtils.copy(inputQuery);
        QueryOptions queryOptions = VariantQueryUtils.copy(inputQueryOptions);

        VariantDBAdaptor dbAdaptor = getDBAdaptor();

        VariantSearchManager variantSearchManager = getVariantSearchManager();
        // first, create the collection it it does not exist
        variantSearchManager.create(dbName);
        if (!configuration.getSearch().isActive() || !variantSearchManager.isAlive(dbName)) {
            throw new StorageEngineException("Solr is not alive!");
        }

        // then, load variants
        queryOptions.put(QueryOptions.EXCLUDE, Arrays.asList(VariantField.STUDIES_SAMPLES, VariantField.STUDIES_FILES));
        try (VariantDBIterator iterator = getVariantsToSecondaryIndex(overwrite, query, queryOptions, dbAdaptor)) {
            VariantSearchLoadResult load = variantSearchManager.load(dbName, iterator, newVariantSearchDataWriter(dbName));

            if (isValidParam(query, VariantQueryParam.REGION)) {
                logger.info("Partial secondary index. Do not update {} timestamp", SEARCH_INDEX_LAST_TIMESTAMP.key());
            } else {
                long value = System.currentTimeMillis();
                getMetadataManager().updateProjectMetadata(projectMetadata -> {
                    projectMetadata.getAttributes().put(SEARCH_INDEX_LAST_TIMESTAMP.key(), value);
                    return projectMetadata;
                });
            }

            return load;
        } catch (StorageEngineException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageEngineException("Exception building secondary index", e);
        }
    }

    protected VariantDBIterator getVariantsToSecondaryIndex(boolean overwrite, Query query, QueryOptions queryOptions,
                                                            VariantDBAdaptor dbAdaptor)
            throws StorageEngineException {
        if (!overwrite) {
            query.put(VariantQueryUtils.VARIANTS_TO_INDEX.key(), true);
        }
        VariantSecondaryIndexFilter filter = new VariantSecondaryIndexFilter(getMetadataManager().getStudies());
        return dbAdaptor.iterator(query, queryOptions).mapBuffered(filter, 10);
    }

    protected void searchIndexLoadedFiles(List<URI> inputFiles, ObjectMap options) throws StorageEngineException {
        try {
            if (options.getBoolean(INDEX_SEARCH.key())) {
                secondaryIndex(new Query(), new QueryOptions(), false);
            }
        } catch (IOException | VariantSearchException e) {
            throw new StorageEngineException("Error indexing in search", e);
        }
    }

    protected SolrInputDocumentDataWriter newVariantSearchDataWriter(String collection) throws StorageEngineException {
        return new SolrInputDocumentDataWriter(collection,
                getVariantSearchManager().getSolrClient(),
                getVariantSearchManager().getInsertBatchSize());
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

                variantSearchManager.load(collectionName, iterator,
                        new SolrInputDocumentDataWriter(collectionName,
                                variantSearchManager.getSolrClient(),
                                variantSearchManager.getInsertBatchSize()));
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
     * @param file   FileName
     * @param outdir Output directory
     * @throws StorageEngineException If the file can not be removed or there was some problem deleting it.
     */
    public void removeFile(String study, String file, URI outdir) throws StorageEngineException {
        removeFiles(study, Collections.singletonList(file), outdir);
    }

    /**
     * Removes files from the Variant Storage.
     *
     * @param study  StudyName or StudyId
     * @param files Files to remove
     * @param outdir Output directory
     * @throws StorageEngineException If the files can not be removed or there was some problem deleting it.
     */
    public abstract void removeFiles(String study, List<String> files, URI outdir) throws StorageEngineException;

    /**
     * Removes samples from the Variant Storage.
     *
     * @param study  StudyName or StudyId
     * @param samples Samples to remove
     * @param outdir Output directory
     * @throws StorageEngineException If the samples can not be removed or there was some problem deleting it.
     */
    public void removeSamples(String study, List<String> samples, URI outdir) throws StorageEngineException {
        throw new UnsupportedOperationException("Unsupported remove sample operation at storage engine " + getStorageEngineId());
    }

    /**
     * Atomically updates the storage metadata before removing samples.
     *
     * @param study    Study
     * @param files     Files to fully delete, including all their samples.
     * @param samples   Samples to remove, leaving partial files.
     * @return FileIds to remove
     * @throws StorageEngineException StorageEngineException
     */
    protected TaskMetadata preRemove(String study, List<String> files, List<String> samples) throws StorageEngineException {
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
     *    Removes partially removed samples from their files
     * If error:
     *    Updates remove status with ERROR
     *
     * @param study      Study
     * @param fileIds    Files to fully delete, including all their samples.
     * @param sampleIds  Samples to remove, leaving partial files.
     * @param taskId     Remove task id
     * @param error      If the remove operation succeeded
     * @throws StorageEngineException StorageEngineException
     */
    protected void postRemoveFiles(String study, List<Integer> fileIds, List<Integer> sampleIds, int taskId, boolean error)
            throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        metadataManager.updateStudyMetadata(study, studyMetadata -> {
            if (error) {
                metadataManager.setStatus(studyMetadata.getId(), taskId, TaskMetadata.Status.ERROR);
            } else {
                metadataManager.setStatus(studyMetadata.getId(), taskId, TaskMetadata.Status.READY);
                metadataManager.removeIndexedFiles(studyMetadata.getId(), fileIds);

                for (Integer fileId : metadataManager.getFileIdsFromSampleIds(studyMetadata.getId(), sampleIds)) {
                    metadataManager.updateFileMetadata(studyMetadata.getId(), fileId, f -> {
                        f.getSamples().removeAll(sampleIds);
                        return f;
                    });
                }
                for (Integer sampleId : sampleIds) {
                    metadataManager.updateSampleMetadata(studyMetadata.getId(), sampleId, s -> {
                        s.setIndexStatus(TaskMetadata.Status.NONE);
                        for (Integer v : s.getSampleIndexVersions()) {
                            s.setSampleIndexStatus(TaskMetadata.Status.NONE, v);
                        }
                        for (Integer v : s.getSampleIndexAnnotationVersions()) {
                            s.setSampleIndexAnnotationStatus(TaskMetadata.Status.NONE, v);
                        }
                        for (Integer v : s.getFamilyIndexVersions()) {
                            s.setFamilyIndexStatus(TaskMetadata.Status.NONE, v);
                        }
                        s.setAnnotationStatus(TaskMetadata.Status.NONE);
                        s.setMendelianErrorStatus(TaskMetadata.Status.NONE);
                        s.setFiles(Collections.emptyList());
                        s.setCohorts(Collections.emptySet());
                    });
                }

                Set<Integer> removedSamples = new HashSet<>(sampleIds);
                Set<Integer> removedSamplesFromFiles = new HashSet<>();
                for (Integer fileId : fileIds) {
                    removedSamplesFromFiles.addAll(metadataManager.getFileMetadata(studyMetadata.getId(), fileId).getSamples());
                }
                removedSamples.addAll(removedSamplesFromFiles);

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
                            cohort -> {
                                cohort.getFiles().removeAll(fileIds);
                                return cohort.setStatsStatus(TaskMetadata.Status.ERROR);
                            });
                }

                // Restore default cohort with indexed samples
                metadataManager.setSamplesToCohort(studyMetadata.getId(), StudyEntry.DEFAULT_COHORT,
                        metadataManager.getIndexedSamples(studyMetadata.getId()));


                for (Integer fileId : fileIds) {
                    metadataManager.removeVariantFileMetadata(studyMetadata.getId(), fileId);
                }
            }
            return studyMetadata;
        });
    }

    /**
     * Remove a whole study from the Variant Storage.
     *
     * @param study  StudyName or StudyId
     * @param outdir Output directory
     * @throws StorageEngineException If the file can not be removed or there was some problem deleting it.
     */
    public abstract void removeStudy(String study, URI outdir) throws StorageEngineException;

    public abstract void loadVariantScore(URI scoreFile, String study, String scoreName, String cohort1, String cohort2,
                                          VariantScoreFormatDescriptor descriptor, ObjectMap options)
    throws StorageEngineException;

    public abstract void deleteVariantScore(String study, String scoreName, ObjectMap options) throws StorageEngineException;

    @Override
    public abstract void testConnection() throws StorageEngineException;

    public void reloadCellbaseConfiguration() {
        cellBaseUtils = null;
    }

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

    @Override
    public void setConfiguration(StorageConfiguration configuration, String storageEngineId, String dbName) {
        options = new ObjectMap(configuration.getVariantEngine(storageEngineId).getOptions());
        // Merge general options
        configuration.getVariant().getOptions().forEach(options::putIfNotNull);
        super.setConfiguration(configuration, storageEngineId, dbName);
    }

    public ObjectMap getOptions() {
        return options;
    }

    public final ObjectMap getMergedOptions(Map<? extends String, ?> params) {
        ObjectMap options = new ObjectMap(getOptions());
        if (params != null) {
            params.forEach(options::putIfNotNull);
        }
        return options;
    }

    public VariantReaderUtils getVariantReaderUtils() {
        return new VariantReaderUtils(ioConnectorProvider);
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
                    variantSearchManager.set(new VariantSearchManager(getMetadataManager(), configuration, getOptions()));
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

    public DataResult<Variant> getSampleData(String variant, String study, QueryOptions options) throws StorageEngineException {
        return new VariantSampleDataManager(getDBAdaptor()).getSampleData(variant, study, options);
    }

    public VariantQueryResult<Variant> get(Query query, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }
        if (query == null) {
            query = new Query();
        }
        addDefaultLimit(options, getOptions());
        addDefaultSampleLimit(query, getOptions());
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
        query = VariantQueryUtils.copy(query);
        options = VariantQueryUtils.copy(options);
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
        executors.add(new BreakendVariantQueryExecutor(
                getMetadataManager(), getStorageEngineId(), getOptions(), new DBAdaptorVariantQueryExecutor(
                getDBAdaptor(), getStorageEngineId(), getOptions()), getDBAdaptor()));
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
        throw new VariantQueryException("No VariantQueryExecutor found to run the query!");
    }

    public Query preProcessQuery(Query originalQuery, QueryOptions options) {
        try {
            return getVariantQueryParser().preProcessQuery(originalQuery, options);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e).setQuery(originalQuery);
        }
    }

    public ParsedVariantQuery parseQuery(Query originalQuery, QueryOptions options) {
        try {
            return getVariantQueryParser().parseQuery(originalQuery, options);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e).setQuery(originalQuery);
        }
    }

    protected VariantQueryParser getVariantQueryParser() throws StorageEngineException {
        return new VariantQueryParser(getCellBaseUtils(), getMetadataManager());
    }

    public DataResult distinct(Query query, String field) throws StorageEngineException {
        return getDBAdaptor().distinct(query, field);
    }

    public DataResult rank(Query query, String field, int numResults, boolean asc) throws StorageEngineException {
        return getDBAdaptor().rank(query, field, numResults, asc);
    }

    public DataResult getFrequency(Query query, Region region, int regionIntervalSize) throws StorageEngineException {
        return getDBAdaptor().getFrequency(getVariantQueryParser().parseQuery(query, new QueryOptions(VariantField.SUMMARY, true)),
                region, regionIntervalSize);
    }

    public DataResult groupBy(Query query, String field, QueryOptions options) throws StorageEngineException {
        return getDBAdaptor().groupBy(query, field, options);
    }

    public DataResult groupBy(Query query, List<String> fields, QueryOptions options) throws StorageEngineException {
        return getDBAdaptor().groupBy(query, fields, options);
    }

    public DataResult<Long> count(Query query) throws StorageEngineException {
        query = preProcessQuery(query, QueryOptions.empty());
        VariantQueryExecutor variantQueryExecutor = getVariantQueryExecutor(query, new QueryOptions(QueryOptions.COUNT, true));
        return variantQueryExecutor.count(query);
    }



    public DataResult<SampleVariantStats> sampleStatsQuery(String studyStr, String sample, Query query, QueryOptions options)
            throws StorageEngineException {
        return new SampleVariantStatsAggregationQuery(this).sampleStatsQuery(studyStr, sample, query, options);
    }

    /**
     * Fetch facet (i.e., counts) resulting of executing the query in the database.
     *
     * @param query          Query to be executed in the database to filter variants
     * @param options        Query modifiers, accepted values are: facet fields and facet ranges
     * @return               A FacetedQueryResult with the result of the query
     */
    public DataResult<FacetField> facet(Query query, QueryOptions options) {
        query = copy(query);
        options = copy(options);
        // Hardcode INCLUDE to simplify preProcess operation, as the query does not return any study data.
        options.put(QueryOptions.INCLUDE, VariantField.ID.fieldName());
        addDefaultLimit(options, getOptions());
        query = preProcessQuery(query, options);
//        logger.info("Filter transcript = {} (raw: '{}')",
//                options.getBoolean("filterTranscript", false), options.get("filterTranscript"));
        return getVariantAggregationExecutor(query, options).aggregation(query, options);
    }

    protected final List<VariantAggregationExecutor> getVariantAggregationExecutors() {
        if (lazyVariantAggregationExecutorsList.isEmpty()) {
            synchronized (lazyVariantAggregationExecutorsList) {
                if (lazyVariantAggregationExecutorsList.isEmpty()) {
                    lazyVariantAggregationExecutorsList.addAll(initVariantAggregationExecutors());
                }
            }
        }
        return lazyVariantAggregationExecutorsList;
    }

    protected List<VariantAggregationExecutor> initVariantAggregationExecutors() {
        List<VariantAggregationExecutor> executors = new ArrayList<>(3);

        try {
            executors.add(new SearchIndexVariantAggregationExecutor(getVariantSearchManager(), getDBName()));
            executors.add(new ChromDensityVariantAggregationExecutor(this, getMetadataManager()));
        } catch (Exception e) {
            throw VariantQueryException.internalException(e);
        }
        return executors;
    }

    /**
     * Determine which {@link VariantQueryExecutor} should be used to execute the given query.
     *
     * @param query   Query to execute
     * @param options Options for the query
     * @return VariantQueryExecutor to use
     */
    public VariantAggregationExecutor getVariantAggregationExecutor(Query query, QueryOptions options) {
        List<String> messages = new LinkedList<>();
        for (VariantAggregationExecutor executor : getVariantAggregationExecutors()) {
            if (executor.canUseThisExecutor(query, options, messages)) {
                return executor;
            }
        }
        String facet = options == null ? null : options.getString(QueryOptions.FACET);
        // This should rarely happen
        logger.warn("Unable to run aggregation facet '" + facet + "' with query " + VariantQueryUtils.printQuery(query));
        for (String message : messages) {
            logger.warn(message);
        }
        throw new VariantQueryException("No VariantAggregationExecutor found to run the query. " + messages).setQuery(query);
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

