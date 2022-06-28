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

package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StopWatch;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.DatabaseCredentials;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.config.storage.StorageEngineConfiguration;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateParams;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.query.executors.*;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.opencb.opencga.storage.core.variant.search.SamplesSearchIndexVariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.search.SearchIndexVariantAggregationExecutor;
import org.opencb.opencga.storage.core.variant.search.SearchIndexVariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.io.HDFSIOConnector;
import org.opencb.opencga.storage.hadoop.utils.DeleteHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HBaseColumnIntersectVariantQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.sample.HBaseVariantSampleDataManager;
import org.opencb.opencga.storage.hadoop.variant.annotation.HadoopDefaultVariantAnnotationManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutorFactory;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsDriver;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsFromArchiveMapper;
import org.opencb.opencga.storage.hadoop.variant.gaps.PrepareFillMissingDriver;
import org.opencb.opencga.storage.hadoop.variant.gaps.write.FillMissingHBaseWriterDriver;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexCompoundHeterozygousQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexMendelianErrorQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexVariantAggregationExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexVariantQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.family.FamilyIndexLoader;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexAnnotationLoader;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDeleteHBaseColumnTask;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexBuilder;
import org.opencb.opencga.storage.hadoop.variant.io.HadoopVariantExporter;
import org.opencb.opencga.storage.hadoop.variant.prune.VariantPruneManager;
import org.opencb.opencga.storage.hadoop.variant.score.HadoopVariantScoreLoader;
import org.opencb.opencga.storage.hadoop.variant.score.HadoopVariantScoreRemover;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchDataWriter;
import org.opencb.opencga.storage.hadoop.variant.search.SecondaryIndexPendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.stats.HadoopDefaultVariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.variant.stats.HadoopMRVariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.*;
import static org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsDriver.*;

public class HadoopVariantStorageEngine extends VariantStorageEngine implements Configurable {
    public static final String STORAGE_ENGINE_ID = "hadoop";

    public static final EnumSet<VariantType> TARGET_VARIANT_TYPE_SET = EnumSet.of(
            VariantType.SNV, VariantType.SNP,
            VariantType.INDEL,
            VariantType.MNV, VariantType.MNP,
            VariantType.INSERTION, VariantType.DELETION,
            VariantType.CNV,
            VariantType.COPY_NUMBER, VariantType.COPY_NUMBER_LOSS, VariantType.COPY_NUMBER_GAIN,
            VariantType.DUPLICATION, VariantType.TANDEM_DUPLICATION, VariantType.TRANSLOCATION,
            VariantType.BREAKEND,
            VariantType.SV, VariantType.SYMBOLIC
    );

    public static final String FILE_ID = "fileId";
    public static final String STUDY_ID = "studyId";

    // Project attributes
    // Last time (in millis from epoch) that a file was loaded
    public static final String LAST_LOADED_FILE_TS = "lastLoadedFileTs";
    // Last time (in millis from epoch) that the list of "pendingVariantsToAnnotate" was updated
    public static final String LAST_VARIANTS_TO_ANNOTATE_UPDATE_TS = "lastVariantsToAnnotateUpdateTs";

    // Study attributes
    // Specify if all missing genotypes from the study are updated. Set to true after fill_missings / aggregation
    public static final String MISSING_GENOTYPES_UPDATED = "missing_genotypes_updated";

    public static final int FILL_GAPS_MAX_SAMPLES = 100;

    protected Configuration conf = null;
    protected MRExecutor mrExecutor;
    private HBaseManager hBaseManager;
    private final AtomicReference<VariantHadoopDBAdaptor> dbAdaptor = new AtomicReference<>();
    private Logger logger = LoggerFactory.getLogger(HadoopVariantStorageEngine.class);
    private HBaseVariantTableNameGenerator tableNameGenerator;
    private final AtomicReference<SampleIndexDBAdaptor> sampleIndexDBAdaptor = new AtomicReference<>();

    public HadoopVariantStorageEngine() {
//        variantReaderUtils = new HdfsVariantReaderUtils(conf);
    }

    @Override
    protected IOConnectorProvider createIOConnectorProvider(StorageConfiguration configuration) {
        IOConnectorProvider ioConnectorProvider = super.createIOConnectorProvider(configuration);
        ioConnectorProvider.add(new HDFSIOConnector(getHadoopConfiguration()));
        return ioConnectorProvider;
    }

    @Override
    public List<StoragePipelineResult> index(List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean doTransform, boolean doLoad)
            throws StorageEngineException {

        if (inputFiles.size() == 1 || !doLoad) {
            return super.index(inputFiles, outdirUri, doExtract, doTransform, doLoad);
        }

        final int nThreadArchive = getOptions().getInt(HADOOP_LOAD_FILES_IN_PARALLEL.key(), HADOOP_LOAD_FILES_IN_PARALLEL.defaultValue());
        ObjectMap extraOptions = new ObjectMap();

        final List<StoragePipelineResult> concurrResult = new CopyOnWriteArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(
                nThreadArchive,
                r -> {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    return t;
                }); // Set Daemon for quick shutdown !!!
        LinkedList<Future<StoragePipelineResult>> futures = new LinkedList<>();
        List<Integer> indexedFiles = new CopyOnWriteArrayList<>();
        AtomicBoolean continueLoading = new AtomicBoolean(true);
        for (URI inputFile : inputFiles) {
            //Provide a connected storageETL if load is required.

            VariantStoragePipeline storageETL = newStoragePipeline(doLoad, new ObjectMap(extraOptions));
            futures.add(executorService.submit(() -> {
                if (!continueLoading.get()) {
                    return null;
                }
                try {
                    Thread.currentThread().setName(UriUtils.fileName(inputFile));
                    StoragePipelineResult storagePipelineResult = new StoragePipelineResult(inputFile);
                    URI nextUri = inputFile;
                    boolean error = false;
                    if (doTransform) {
                        try {
                            nextUri = transformFile(storageETL, storagePipelineResult, concurrResult, nextUri, outdirUri);

                        } catch (StoragePipelineException ignore) {
                            //Ignore here. Errors are stored in the ETLResult
                            error = true;
                        }
                    }

                    if (doLoad && !error) {
                        try {
                            loadFile(storageETL, storagePipelineResult, concurrResult, nextUri, outdirUri);
                        } catch (StoragePipelineException ignore) {
                            //Ignore here. Errors are stored in the ETLResult
                            error = true;
                        }
                    }
                    if (doLoad && !error) {
                        // Read the VariantSource to get the original fileName (it may be different from the
                        // nextUri.getFileName if this is the transformed file)
                        String filePath = storageETL.readVariantFileMetadata(nextUri).getPath();
                        String fileName = Paths.get(filePath).getFileName().toString();
                        // Get latest study metadata from DB, might have been changed since
                        StudyMetadata studyMetadata = storageETL.getStudyMetadata();
                        // Get file ID for the provided file name
                        Integer fileId = storageETL.getMetadataManager().getFileId(studyMetadata.getId(), fileName);
                        indexedFiles.add(fileId);
                    }
                    return storagePipelineResult;
                } finally {
                    try {
                        storageETL.close();
                    } catch (StorageEngineException e) {
                        logger.error("Issue closing DB connection ", e);
                    }
                }
            }));
        }

        executorService.shutdown();

        int errors = 0;
        try {
            while (!futures.isEmpty()) {
                // Check values
                if (futures.peek().isDone() || futures.peek().isCancelled()) {
                    Future<StoragePipelineResult> first = futures.pop();
                    StoragePipelineResult result = first.get(1, TimeUnit.MINUTES);
                    if (result == null) {
                        continue;
                    }
                    boolean error = false;
                    if (result.getTransformError() != null) {
                        logger.error("Error transforming file " + result.getInput(), result.getTransformError());
                        error = true;
                    } else if (result.getLoadError() != null) {
                        error = true;
                        logger.error("Error loading file " + result.getInput(), result.getLoadError());
                    }
                    if (error) {
                        //TODO: Handle errors. Retry?
                        errors++;
                        if (getOptions().getBoolean("abortOnError", false)) {
                            continueLoading.set(false);
                        }
                    }
                    concurrResult.add(result);
                } else {
                    // Sleep only if the task is not done
                    executorService.awaitTermination(1, TimeUnit.MINUTES);
                }
            }
            if (errors > 0) {
                throw new StoragePipelineException("Errors found", concurrResult);
            }

            if (doLoad) {
                annotateLoadedFiles(outdirUri, inputFiles, concurrResult, getOptions());
                calculateStatsForLoadedFiles(outdirUri, inputFiles, concurrResult, getOptions());
                searchIndexLoadedFiles(inputFiles, getOptions());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StoragePipelineException("Interrupted!", e, concurrResult);
        } catch (ExecutionException e) {
            throw new StoragePipelineException("Execution exception!", e, concurrResult);
        } catch (TimeoutException e) {
            throw new StoragePipelineException("Timeout Exception", e, concurrResult);
        } finally {
            if (!executorService.isShutdown()) {
                try {
                    executorService.shutdownNow();
                } catch (Exception e) {
                    logger.error("Problems shutting executer service down", e);
                }
            }
        }
        return concurrResult;
    }

    @Override
    public HadoopVariantStoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException {
        return newStoragePipeline(connected, null);
    }

    @Override
    protected VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator) throws StorageEngineException {
        return new HadoopDefaultVariantAnnotationManager(annotator, getDBAdaptor(), getMRExecutor(), getOptions(), ioConnectorProvider);
    }

    @Override
    protected VariantExporter newVariantExporter(VariantMetadataFactory metadataFactory) throws StorageEngineException {
        return new HadoopVariantExporter(this, metadataFactory, getMRExecutor(), ioConnectorProvider);
    }

    @Override
    public void deleteStats(String study, Collection<String> cohorts, ObjectMap params) throws StorageEngineException {
        ObjectMap options = getMergedOptions(params);
        // Remove unneeded values that might leak into options.
        options.remove("sample");
        options.remove("file");
        boolean force = options.getBoolean(FORCE.key());

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        final int studyId = metadataManager.getStudyId(study);
        List<Integer> cohortIds = new ArrayList<>(cohorts.size());
        List<String> cohortsInWrongStatus = new ArrayList<>(cohorts.size());
        for (String cohort : cohorts) {
            int cohortId = metadataManager.getCohortIdOrFail(studyId, cohort);
            cohortIds.add(cohortId);
            TaskMetadata.Status status = metadataManager.getCohortMetadata(studyId, cohortId).getStatsStatus();
            if (status != TaskMetadata.Status.READY) {
                if (force) {
                    logger.warn("Delete variant stats from cohort '{}' in status '{}'", cohort, status);
                } else {
                    cohortsInWrongStatus.add(cohort);
                }
            }
        }
        if (!cohortsInWrongStatus.isEmpty()) {
            throw new StorageEngineException("Unable to delete variant stats from cohorts " + cohortsInWrongStatus);
        }

        options.put(DeleteHBaseColumnDriver.TWO_PHASES_PARAM, true);
        options.put(DeleteHBaseColumnDriver.DELETE_HBASE_COLUMN_TASK_CLASS, VariantsTableDeleteColumnTask.class.getName());

        try {
            logger.info("------------------------------------------------------");
            logger.info("Deleting variant stats from {} cohorts", cohortIds.size());
            logger.info("------------------------------------------------------");

            StopWatch stopWatch = new StopWatch().start();
            List<String> columns = new ArrayList<>(VariantPhoenixSchema.COHORT_STATS_COLUMNS_PER_COHORT * cohortIds.size());
            for (Integer cohortId : cohortIds) {
                VariantPhoenixSchema.getStatsColumns(studyId, cohortId)
                        .stream()
                        .map(PhoenixHelper.Column::fullColumn)
                        .forEach(columns::add);
            }

            String[] deleteFromVariantsArgs = DeleteHBaseColumnDriver.buildArgs(getVariantTableName(), columns, options);
            getMRExecutor().run(DeleteHBaseColumnDriver.class, deleteFromVariantsArgs, "Delete variant stats from variants table");

            logger.info("Total time: {}", TimeUtils.durationToString(stopWatch.now(TimeUnit.MILLISECONDS)));

            for (Integer cohortId : cohortIds) {
                metadataManager.updateCohortMetadata(studyId, cohortId, c -> {
                    c.setStatsStatus(TaskMetadata.Status.NONE);
                    return c;
                });
            }
        } catch (Exception e) {
            throw new StorageEngineException("Error removing variant stats", e);
        }
    }

    @Override
    public void sampleIndex(String study, List<String> samples, ObjectMap options) throws StorageEngineException {
        options = getMergedOptions(options);
        System.out.println("options.toJson() = " + options.toJson());
        new SampleIndexBuilder(getSampleIndexDBAdaptor(), study, getMRExecutor())
                .buildSampleIndex(samples, options);
    }


    @Override
    public void sampleIndexAnnotate(String study, List<String> samples, ObjectMap options) throws StorageEngineException {
        options = getMergedOptions(options);
        new SampleIndexAnnotationLoader(getSampleIndexDBAdaptor(), getMRExecutor())
                .updateSampleAnnotation(study, samples, options);
    }


    @Override
    public DataResult<List<String>> familyIndex(String study, List<List<String>> trios, ObjectMap options) throws StorageEngineException {
        options = getMergedOptions(options);
        return new FamilyIndexLoader(getSampleIndexDBAdaptor(), getDBAdaptor(), getMRExecutor())
                .load(study, trios, options);
    }


    @Override
    public VariantStatisticsManager newVariantStatisticsManager() throws StorageEngineException {
        // By default, execute a MR to calculate statistics
        if (getOptions().getBoolean(STATS_LOCAL.key(), STATS_LOCAL.defaultValue())) {
            return new HadoopDefaultVariantStatisticsManager(getDBAdaptor(), ioConnectorProvider);
        } else {
            return new HadoopMRVariantStatisticsManager(getDBAdaptor(), getMRExecutor(), getOptions());
        }
    }

    public void aggregate(String study, VariantAggregateParams params, ObjectMap options) throws StorageEngineException {
        logger.info("Aggregate: Study " + study);

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);

        options = params.toObjectMap(options);
        fillGapsOrMissing(study, studyMetadata, metadataManager.getIndexedFiles(studyMetadata.getId()), Collections.emptyList(),
                false, params.isOverwrite(), options);
    }

    @Override
    public VariantSearchLoadResult secondaryIndex(Query query, QueryOptions queryOptions, boolean overwrite)
            throws StorageEngineException, IOException, VariantSearchException {
        queryOptions = queryOptions == null ? new QueryOptions() : new QueryOptions(queryOptions);

        if (getOptions().getBoolean("skipDiscoverPendingVariantsToSecondaryIndex", false)) {
            logger.info("Skip discover pending variants to secondary index");
        } else {
            new SecondaryIndexPendingVariantsManager(getDBAdaptor())
                    .discoverPending(getMRExecutor(), overwrite, getMergedOptions(queryOptions));
        }

        return super.secondaryIndex(query, queryOptions, overwrite);
    }

    @Override
    protected VariantDBIterator getVariantsToSecondaryIndex(boolean overwrite, Query query, QueryOptions queryOptions,
                                                            VariantDBAdaptor dbAdaptor) throws StorageEngineException {

        logger.info("Get variants to index from pending variants table");
        logger.info("Query: " + query.toJson());
        return new SecondaryIndexPendingVariantsManager(getDBAdaptor()).iterator(query);
    }

    @Override
    protected HadoopVariantSearchDataWriter newVariantSearchDataWriter(String collection) throws StorageEngineException {
        return new HadoopVariantSearchDataWriter(
                collection, getVariantSearchManager().getSolrClient(), getVariantSearchManager().getInsertBatchSize(),
                getDBAdaptor());
    }

    @Override
    public void aggregateFamily(String study, VariantAggregateFamilyParams params, ObjectMap options) throws StorageEngineException {
        List<String> samples = params.getSamples();
        if (samples == null || samples.size() < 2) {
            throw new IllegalArgumentException("Aggregate family operation requires at least two samples.");
        } else if (samples.size() > FILL_GAPS_MAX_SAMPLES) {
            throw new IllegalArgumentException("Unable to execute fill gaps operation with more than "
                    + FILL_GAPS_MAX_SAMPLES + " samples.");
        }

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        List<Integer> sampleIds = new ArrayList<>(samples.size());
        for (String sample : samples) {
            Integer sampleId = metadataManager.getSampleId(studyMetadata.getId(), sample);
            if (sampleId != null) {
                sampleIds.add(sampleId);
            } else {
                throw VariantQueryException.sampleNotFound(sample, studyMetadata.getName());
            }
        }

        // Get files
        Set<Integer> fileIds = metadataManager.getFileIdsFromSampleIds(studyMetadata.getId(), sampleIds);

        options = params.toObjectMap(options);
        if (StringUtils.isNotEmpty(params.getGapsGenotype())) {
            options.put(FILL_GAPS_GAP_GENOTYPE.key(), params.getGapsGenotype());
        }
        logger.info("FillGaps: Study " + study + ", samples " + samples);
        fillGapsOrMissing(study, studyMetadata, fileIds, sampleIds, true, false, params.toObjectMap(options));
    }

    private void fillGapsOrMissing(String study, StudyMetadata studyMetadata, Set<Integer> fileIds, List<Integer> sampleIds,
                                   boolean fillGaps, boolean overwrite, ObjectMap inputOptions) throws StorageEngineException {
        ObjectMap options = new ObjectMap(getOptions());
        if (inputOptions != null) {
            options.putAll(inputOptions);
        }

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        int studyId = studyMetadata.getId();

        String jobOperationName = fillGaps ? FILL_GAPS_OPERATION_NAME : FILL_MISSING_OPERATION_NAME;
        List<Integer> fileIdsList = new ArrayList<>(fileIds);
        fileIdsList.sort(Integer::compareTo);

        boolean resume = options.getBoolean(RESUME.key(), RESUME.defaultValue());
        TaskMetadata task = metadataManager.addRunningTask(
                studyId,
                jobOperationName,
                fileIdsList,
                resume,
                TaskMetadata.Type.OTHER,
                // Allow concurrent operations if fillGaps.
                (v) -> fillGaps || v.getName().equals(FILL_GAPS_OPERATION_NAME));
        options.put(AbstractVariantsTableDriver.TIMESTAMP, task.getTimestamp());

        if (!fillGaps) {
            URI directory = URI.create(options.getString(INTERMEDIATE_HDFS_DIRECTORY.key()));
            if (directory.getScheme() != null && !directory.getScheme().equals("hdfs")) {
                throw new StorageEngineException("Output must be in HDFS");
            }
            String regionStr = options.getString(VariantQueryParam.REGION.key());
            String outputPath = directory.resolve(dbName + "_fill_missing_study_" + studyId
                    + (StringUtils.isNotEmpty(regionStr) ? '_' + regionStr.replace(':', '_').replace('-', '_') : "") + ".bin").toString();
            logger.info("Using intermediate file = " + outputPath);
            options.put(FILL_MISSING_INTERMEDIATE_FILE, outputPath);
        }

        Thread hook = metadataManager.buildShutdownHook(jobOperationName, studyId, task.getId());
        Exception exception = null;
        try {
            Runtime.getRuntime().addShutdownHook(hook);

            options.put(FillGapsFromArchiveMapper.SAMPLES, sampleIds);
            options.put(FillGapsFromArchiveMapper.FILL_GAPS, fillGaps);
            options.put(FillGapsFromArchiveMapper.OVERWRITE, overwrite);

            String[] args = FillGapsDriver.buildArgs(
                    getArchiveTableName(studyId),
                    getVariantTableName(),
                    studyId, fileIds, options);

            // TODO: Save progress in Metadata

            // Prepare fill missing
            if (!fillGaps) {
                if (options.getBoolean("skipPrepareFillMissing", false)) {
                    logger.info("=================================================");
                    logger.info("SKIP prepare archive table for " + FILL_MISSING_OPERATION_NAME);
                    logger.info("=================================================");
                } else {
                    String taskDescription = "Prepare archive table for " + FILL_MISSING_OPERATION_NAME;
                    getMRExecutor().run(PrepareFillMissingDriver.class, args, taskDescription);
                }
            }

            // Execute main operation
            String taskDescription = jobOperationName + " of samples " + (fillGaps ? sampleIds.toString() : "\"ALL\"")
                    + " into variants table '" + getVariantTableName() + '\'';
            getMRExecutor().run(FillGapsDriver.class, args, taskDescription);

            // Write results
            if (!fillGaps) {
                taskDescription = "Write results in variants table for " + FILL_MISSING_OPERATION_NAME;
                getMRExecutor().run(FillMissingHBaseWriterDriver.class, args, taskDescription);
            }

        } catch (RuntimeException e) {
            exception = e;
            throw new StorageEngineException("Error " + jobOperationName + " for samples " + sampleIds, e);
        } catch (StorageEngineException e) {
            exception = e;
            throw e;
        } finally {
            boolean fail = exception != null;
            metadataManager.setStatus(studyId, task.getId(), fail ? TaskMetadata.Status.ERROR : TaskMetadata.Status.READY);
            metadataManager.updateStudyMetadata(study, sm -> {
                if (!fillGaps && StringUtils.isEmpty(options.getString(REGION.key()))) {
                    sm.getAttributes().put(MISSING_GENOTYPES_UPDATED, !fail);
                }
                return sm;
            });
            Runtime.getRuntime().removeShutdownHook(hook);
        }

    }

    public HadoopVariantStoragePipeline newStoragePipeline(boolean connected, Map<? extends String, ?> extraOptions)
            throws StorageEngineException {
        ObjectMap options = getMergedOptions(extraOptions);
//        if (connected) {
//            // Ensure ProjectMetadata exists. Don't really care about the value.
//            getStudyConfigurationManager().getProjectMetadata(getMergedOptions(options)).first();
//        }
        VariantHadoopDBAdaptor dbAdaptor = connected ? getDBAdaptor() : null;
        Configuration hadoopConfiguration = dbAdaptor == null ? null : dbAdaptor.getConfiguration();
        hadoopConfiguration = hadoopConfiguration == null ? getHadoopConfiguration(options) : hadoopConfiguration;
        hadoopConfiguration.setIfUnset(ARCHIVE_TABLE_COMPRESSION.key(), ARCHIVE_TABLE_COMPRESSION.key());
        for (String key : options.keySet()) {
            hadoopConfiguration.set(key, options.getString(key));
        }

        MergeMode mergeMode;
        if (connected) {
            String study = options.getString(VariantStorageOptions.STUDY.key());
//            archiveCredentials = buildCredentials(getArchiveTableName(studyId));
            StudyMetadata sm = getMetadataManager().getStudyMetadata(study);
            if (sm == null || !sm.getAttributes().containsKey(MERGE_MODE.key())) {
                mergeMode = MergeMode.from(options);
            } else {
                mergeMode = MergeMode.from(sm.getAttributes());
            }
        } else {
            mergeMode = MergeMode.BASIC;
        }

        if (mergeMode.equals(MergeMode.ADVANCED)) {
//            throw new IllegalStateException("Unable to load with MergeMode " + MergeMode.ADVANCED);
            // Force to use MergeMode=BASIC for Hadoop
            options.put(MERGE_MODE.key(), MergeMode.BASIC.name());
            mergeMode = MergeMode.BASIC;
        }
        HadoopVariantStoragePipeline storageETL = new HadoopLocalLoadVariantStoragePipeline(configuration, dbAdaptor,
                ioConnectorProvider, hadoopConfiguration, options);
//        if (mergeMode.equals(MergeMode.BASIC)) {
//            storageETL = new HadoopMergeBasicVariantStoragePipeline(configuration, dbAdaptor,
//                    hadoopConfiguration, archiveCredentials, getVariantReaderUtils(hadoopConfiguration), options);
//        } else {
//            storageETL = new HadoopVariantStoragePipelineMRLoad(configuration, dbAdaptor, getMRExecutor(options),
//                    hadoopConfiguration, archiveCredentials, getVariantReaderUtils(hadoopConfiguration), options);
//        }
        return storageETL;
    }

    @Override
    public void removeSamples(String study, List<String> samples, URI outdir) throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        final int studyId = metadataManager.getStudyId(study);
        samples = new ArrayList<>(samples);
        Set<Integer> sampleIds = new HashSet<>(samples.size());
        List<String> samplesAlreadyDeletedOrMissing = new ArrayList<>();
        for (String sample : samples) {
            Integer sampleId = metadataManager.getSampleId(studyId, sample);
            sampleIds.add(sampleId);
            if (sampleId == null) {
                samplesAlreadyDeletedOrMissing.add(sample);
            } else if (!metadataManager.getSampleMetadata(studyId, sampleId).isIndexed()) {
                samplesAlreadyDeletedOrMissing.add(sample);
            }
        }
        if (!samplesAlreadyDeletedOrMissing.isEmpty()) {
            if (getOptions().getBoolean(FORCE.key(), false)) {
                logger.info("Force sample delete of {} samples already deleted or missing", samplesAlreadyDeletedOrMissing.size());
            } else {
                throw new StorageEngineException("Unable to delete samples from variant storage. Already deleted or never loaded."
                        + " Samples = " + samplesAlreadyDeletedOrMissing);
            }
        }

        // Check if any file is being completely deleted
        Set<Integer> partiallyDeletedFiles = metadataManager.getFileIdsFromSampleIds(studyId, sampleIds, true);
        List<String> fullyDeletedFiles = new ArrayList<>();
        List<Integer> fullyDeletedFileIds = new ArrayList<>();
        for (Integer partiallyDeletedFile : partiallyDeletedFiles) {
            LinkedHashSet<Integer> samplesFromFile = metadataManager.getSampleIdsFromFileId(studyId, partiallyDeletedFile);
            if (sampleIds.containsAll(samplesFromFile)) {
                fullyDeletedFileIds.add(partiallyDeletedFile);
                fullyDeletedFiles.add(metadataManager.getFileName(studyId, partiallyDeletedFile));
            }
        }

        for (Integer sampleId : sampleIds) {
            SampleMetadata sm = metadataManager.getSampleMetadata(studyId, sampleId);
            if (fullyDeletedFileIds.containsAll(sm.getFiles())) {
                samples.remove(sm.getName());
            }
        }

        remove(study, fullyDeletedFiles, samples, outdir);
    }

    @Override
    public void removeFiles(String study, List<String> files, URI outdir) throws StorageEngineException {
        remove(study, files, Collections.emptyList(), outdir);
    }

    /**
     * Remove files and samples from the database.
     *
     * @param study     Study
     * @param files     Files to fully delete, including all their samples.
     * @param samples   Samples to remove, leaving partial files.
     * @param outdir    outdir
     * @throws StorageEngineException if something goes wrong
     */
    private void remove(String study, List<String> files, List<String> samples, URI outdir) throws StorageEngineException {
        ObjectMap options = new ObjectMap(getOptions());
        // Remove unneeded values that might leak into options.
        options.remove("sample");
        options.remove("file");

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        final int studyId = metadataManager.getStudyId(study);
        TaskMetadata task = preRemove(study, files, samples);
        List<Integer> fileIds = task.getFileIds();
        List<Integer> sampleIds = new ArrayList<>(samples.size());
        Set<Integer> allSampleIds = new HashSet<>();
        Set<Integer> sampleIdsFromFiles = new HashSet<>();

        for (String sample : samples) {
            sampleIds.add(metadataManager.getSampleId(studyId, sample));
        }
        allSampleIds.addAll(sampleIds);
        for (Integer fileId : fileIds) {
            LinkedHashSet<Integer> sampleIdsFromFile = metadataManager.getSampleIdsFromFileId(studyId, fileId);
            sampleIdsFromFiles.addAll(sampleIdsFromFile);
            allSampleIds.addAll(sampleIdsFromFile);
        }

//        // Pre delete
//        scm.lockAndUpdate(studyId, sc -> {
//            if (!sc.getIndexedFiles().contains(fileId)) {
//                throw StorageEngineException.unableToExecute("File not indexed.", fileId, sc);
//            }
//            boolean resume = options.getBoolean(Options.RESUME.key(), Options.RESUME.defaultValue())
//                    || options.getBoolean(HadoopVariantStorageEngine.HADOOP_LOAD_VARIANT_RESUME, false);
//            BatchFileOperation operation =
//                    etl.addBatchOperation(sc, VariantTableRemoveFileDriver.JOB_OPERATION_NAME, fileList, resume,
//                            BatchFileOperation.Type.REMOVE);
//            options.put(AbstractAnalysisTableDriver.TIMESTAMP, operation.getTimestamp());
//            return sc;
//        });

        StudyMetadata sm = metadataManager.getStudyMetadata(studyId);
        LinkedHashSet<Integer> indexedFiles = metadataManager.getIndexedFiles(sm.getId());
        boolean removeWholeStudy = indexedFiles.size() == fileIds.size() && indexedFiles.containsAll(fileIds);
        options.put(AbstractVariantsTableDriver.TIMESTAMP, task.getTimestamp());
        options.put(DeleteHBaseColumnDriver.TWO_PHASES_PARAM, true);

        // Delete
        Thread hook = getMetadataManager().buildShutdownHook(REMOVE_OPERATION_NAME, studyId, task.getId());
        try {
            Runtime.getRuntime().addShutdownHook(hook);

            String archiveTable = getArchiveTableName(studyId);
            String variantsTable = getVariantTableName();
            String sampleIndexTable = getSampleIndexDBAdaptor().getSampleIndexTableNameLatest(studyId);


            long startTime = System.currentTimeMillis();
            logger.info("------------------------------------------------------");
            if (!files.isEmpty()) {
                logger.info("Deleting {} files and their {} samples", files.size(), sampleIdsFromFiles.size());
            }
            if (!samples.isEmpty()) {
                logger.info("Deleting {} samples leaving partial input files", samples.size());
            }
            logger.info("------------------------------------------------------");
            boolean parallelDelete = options.getBoolean(
                    VariantStorageOptions.DELETE_PARALLEL.key(),
                    VariantStorageOptions.DELETE_PARALLEL.defaultValue());
            ExecutorService service = parallelDelete
                    ? Executors.newCachedThreadPool()
                    : Executors.newSingleThreadExecutor();
            Future<Long> deleteFromVariants = service.submit(() -> {
                StopWatch stopWatch = new StopWatch().start();
                Map<String, List<String>> columns = new HashMap<>();
                for (Integer fileId : fileIds) {
                    String fileColumn = VariantPhoenixSchema.getFileColumn(studyId, fileId).fullColumn();
                    List<String> sampleColumns = new ArrayList<>();
                    FileMetadata fileMetadata = metadataManager.getFileMetadata(sm.getId(), fileId);
                    for (Integer sampleId : fileMetadata.getSamples()) {
                        SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
                        for (PhoenixHelper.Column sampleColumn : VariantPhoenixSchema
                                .getSampleColumns(sampleMetadata, Collections.singleton(fileId))) {
                            sampleColumns.add(sampleColumn.fullColumn());
                        }
                    }
                    columns.put(fileColumn, sampleColumns);
                }
                for (Integer sampleId : sampleIds) {
                    SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(sm.getId(), sampleId);
                    for (PhoenixHelper.Column column : VariantPhoenixSchema.getSampleColumns(sampleMetadata)) {
                        columns.put(column.fullColumn(), null);
                    }
                }
                if (removeWholeStudy) {
                    columns.put(VariantPhoenixSchema.getStudyColumn(studyId).fullColumn(), Collections.emptyList());
                }
                ObjectMap thisOptions = new ObjectMap(options);
                thisOptions.put(DeleteHBaseColumnDriver.DELETE_HBASE_COLUMN_TASK_CLASS, VariantsTableDeleteColumnTask.class.getName());

                String[] deleteFromVariantsArgs = DeleteHBaseColumnDriver.buildArgs(variantsTable, columns, thisOptions);
                getMRExecutor().run(DeleteHBaseColumnDriver.class, deleteFromVariantsArgs, "Delete from variants table");
                return stopWatch.now(TimeUnit.MILLISECONDS);
            });
            // TODO: Remove whole table if removeWholeStudy
            final Future<Long> deleteFromArchive;
            if (CollectionUtils.isEmpty(files)) {
                deleteFromArchive = null;
            } else if (!getDBAdaptor().getHBaseManager().tableExists(archiveTable)) {
                // Might not exist if the initial index was executed without archive (--load-archive no)
                logger.info("Archive table '{}' does not exist. Skip delete!", archiveTable);
                deleteFromArchive = null;
            } else {
                deleteFromArchive = service.submit(() -> {
                    StopWatch stopWatch = new StopWatch().start();
                    List<String> archiveColumns = new ArrayList<>();
                    String family = Bytes.toString(GenomeHelper.COLUMN_FAMILY_BYTES);
                    for (Integer fileId : fileIds) {
                        archiveColumns.add(family + ':' + ArchiveTableHelper.getRefColumnName(fileId));
                        archiveColumns.add(family + ':' + ArchiveTableHelper.getNonRefColumnName(fileId));
                    }
                    String[] deleteFromArchiveArgs = DeleteHBaseColumnDriver.buildArgs(archiveTable, archiveColumns, options);
                    getMRExecutor().run(DeleteHBaseColumnDriver.class, deleteFromArchiveArgs, "Delete from archive table");
                    return stopWatch.now(TimeUnit.MILLISECONDS);
                });
            }
            // TODO: Remove whole table if removeWholeStudy
            List<String> samplesToRebuildIndex = new ArrayList<>();
            final Future<Long> deleteFromSampleIndex;
            if (allSampleIds.isEmpty()) {
                deleteFromSampleIndex = null;
            } else if (getDBAdaptor().getHBaseManager().tableExists(sampleIndexTable)) {
                // Might not exist if the initial index was executed without sample-index (--load-sample-index no)
                logger.info("Sample index table '{}' does not exist. Skip delete!", sampleIndexTable);
                deleteFromSampleIndex = null;
            } else {
                deleteFromSampleIndex = service.submit(() -> {
                    StopWatch stopWatch = new StopWatch().start();
                    for (Integer sampleId : allSampleIds) {
                        // Check if sampleIndex needs to be rebuild if the sample was not being fully deleted
                        //  a) Not in samples list
                        //  b) Part of a non-deleted file.
                        if (!sampleIds.contains(sampleId)) {
                            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
                            Set<Integer> filesFromSample = new HashSet<>(sampleMetadata.getFiles());
                            filesFromSample.removeAll(fileIds);
                            if (!filesFromSample.isEmpty()) {
                                boolean otherFilesFromSampleIndexed = false;
                                for (Integer fileFromSample : filesFromSample) {
                                    if (metadataManager.isFileIndexed(studyId, fileFromSample)) {
                                        otherFilesFromSampleIndexed = true;
                                        break;
                                    }
                                }
                                if (otherFilesFromSampleIndexed) {
                                    // The sample has other files that are not deleted, need to rebuild the sample index
                                    samplesToRebuildIndex.add(sampleMetadata.getName());
                                }
                            }
                        }
                    }
                    ObjectMap thisOptions = new ObjectMap(options);
                    thisOptions.put(DeleteHBaseColumnDriver.DELETE_HBASE_COLUMN_TASK_CLASS,
                            SampleIndexDeleteHBaseColumnTask.class.getName());
                    thisOptions.put(SampleIndexDeleteHBaseColumnTask.SAMPLE_IDS_TO_DELETE_FROM_SAMPLE_INDEX, allSampleIds);

                    String[] deleteFromSampleIndexArgs = DeleteHBaseColumnDriver.buildArgs(sampleIndexTable, null, true, null, thisOptions);
                    getMRExecutor().run(DeleteHBaseColumnDriver.class, deleteFromSampleIndexArgs,
                            "Delete from SamplesIndex table");
                    return stopWatch.now(TimeUnit.MILLISECONDS);
                });
            }
            service.shutdown();
            service.awaitTermination(10, TimeUnit.DAYS);
            if (!samplesToRebuildIndex.isEmpty()) {
                logger.info("Rebuild sample index for samples " + samplesToRebuildIndex);
                for (String sample : samplesToRebuildIndex) {
                    int sampleId = getMetadataManager().getSampleIdOrFail(studyId, sample);
                    getMetadataManager().updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                        for (int v : sampleMetadata.getSampleIndexVersions()) {
                            sampleMetadata.setSampleIndexStatus(TaskMetadata.Status.ERROR, v);
                        }
                        for (int v : sampleMetadata.getSampleIndexAnnotationVersions()) {
                            sampleMetadata.setSampleIndexAnnotationStatus(TaskMetadata.Status.ERROR, v);
                        }
                        for (int v : sampleMetadata.getFamilyIndexVersions()) {
                            sampleMetadata.setFamilyIndexStatus(TaskMetadata.Status.ERROR, v);
                        }
                    });
                }
                sampleIndex(study, samplesToRebuildIndex, options);
                sampleIndexAnnotate(study, samplesToRebuildIndex, options);
            }

            logger.info("------------------------------------------------------");
            if (deleteFromArchive != null) {
                logger.info("Delete from archive: {}", TimeUtils.durationToString(deleteFromArchive.get()));
            }
            logger.info("Delete from variants: {}", TimeUtils.durationToString(deleteFromVariants.get()));
            if (deleteFromSampleIndex != null) {
                logger.info("Delete from sample index: {}", TimeUtils.durationToString(deleteFromSampleIndex.get()));
            }
            logger.info("Total time: {}", TimeUtils.durationToString(System.currentTimeMillis() - startTime));

//            // Post Delete
//            // If everything went fine, remove file column from Archive table and from studyconfig
//            scm.lockAndUpdate(studyId, sc -> {
//                scm.setStatus(sc, BatchFileOperation.Status.READY,
//                        VariantTableRemoveFileDriver.JOB_OPERATION_NAME, fileIds);
//                sc.getIndexedFiles().remove(fileId);
//                return sc;
//            });

            postRemoveFiles(study, fileIds, sampleIds, task.getId(), false);
        } catch (StorageEngineException e) {
            postRemoveFiles(study, fileIds, sampleIds, task.getId(), true);
            throw e;
        } catch (Exception e) {
            postRemoveFiles(study, fileIds, sampleIds, task.getId(), true);
            throw new StorageEngineException("Error removing files " + fileIds + " from tables ", e);
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    @Override
    protected void postRemoveFiles(String study, List<Integer> fileIds, List<Integer> sampleIds, int taskId, boolean error)
            throws StorageEngineException {
        // First, if the operation finished without errors, remove the phoenix columns.
        if (!error) {
            VariantHadoopDBAdaptor dbAdaptor = getDBAdaptor();
            VariantPhoenixSchemaManager schemaManager = new VariantPhoenixSchemaManager(dbAdaptor);

            StudyMetadata sm = getMetadataManager().getStudyMetadata(study);

            try {
                schemaManager.dropFiles(sm.getId(), fileIds);
                schemaManager.dropSamples(sm.getId(), sampleIds);
            } catch (SQLException e) {
                throw new StorageEngineException("Error removing columns from Phoenix", e);
            }
        }
        // Then, run the default postRemoveFiles
        super.postRemoveFiles(study, fileIds, sampleIds, taskId, error);
    }

    @Override
    public void removeStudy(String studyName, URI outdir) throws StorageEngineException {
        int studyId = getMetadataManager().getStudyId(studyName);
        removeFiles(studyName, getMetadataManager().getIndexedFiles(studyId).stream().map(Object::toString).collect(Collectors.toList()),
                outdir);
    }

    @Override
    public void variantsPrune(boolean dryMode, boolean resume, URI outdir) throws StorageEngineException {
        new VariantPruneManager(this).prune(dryMode, resume, outdir);
    }

    @Override
    public void loadVariantScore(URI scoreFile, String study, String scoreName, String cohort1, String cohort2,
                                 VariantScoreFormatDescriptor descriptor, ObjectMap options)
            throws StorageEngineException {
        new HadoopVariantScoreLoader(getDBAdaptor(), ioConnectorProvider)
                .loadVariantScore(scoreFile, study, scoreName, cohort1, cohort2, descriptor, options);
    }

    @Override
    public void deleteVariantScore(String study, String scoreName, ObjectMap options)
            throws StorageEngineException {
        new HadoopVariantScoreRemover(getDBAdaptor(), getMRExecutor())
                .remove(study, scoreName, options);
    }

    private HBaseCredentials getDbCredentials() throws StorageEngineException {
        String table = getVariantTableName();
        return buildCredentials(table);
    }

    @Override
    public VariantHadoopDBAdaptor getDBAdaptor() throws StorageEngineException {
        if (dbAdaptor.get() == null) {
            synchronized (dbAdaptor) {
                if (dbAdaptor.get() == null) {
                    HBaseCredentials credentials = getDbCredentials();
                    try {
                        Configuration configuration = getHadoopConfiguration();
                        configuration = VariantHadoopDBAdaptor.getHbaseConfiguration(configuration, credentials);
                        dbAdaptor.set(new VariantHadoopDBAdaptor(getHBaseManager(configuration),
                                this.configuration, configuration, getTableNameGenerator()));
                    } catch (IOException e) {
                        throw new StorageEngineException("Error creating DB Adapter", e);
                    }
                }
            }
        }
        return dbAdaptor.get();
    }

    public SampleIndexDBAdaptor getSampleIndexDBAdaptor() throws StorageEngineException {
        VariantHadoopDBAdaptor dbAdaptor = getDBAdaptor();
        SampleIndexDBAdaptor sampleIndexDBAdaptor = this.sampleIndexDBAdaptor.get();
        if (sampleIndexDBAdaptor == null) {
            synchronized (this.sampleIndexDBAdaptor) {
                sampleIndexDBAdaptor = this.sampleIndexDBAdaptor.get();
                if (sampleIndexDBAdaptor == null) {
                    sampleIndexDBAdaptor = new SampleIndexDBAdaptor(dbAdaptor.getHBaseManager(),
                            dbAdaptor.getTableNameGenerator(), dbAdaptor.getMetadataManager());
                    this.sampleIndexDBAdaptor.set(sampleIndexDBAdaptor);
                }
            }
        }
        return sampleIndexDBAdaptor;
    }

    private synchronized HBaseManager getHBaseManager(Configuration configuration) {
        if (hBaseManager == null) {
            hBaseManager = new HBaseManager(configuration);
        }
        return hBaseManager;
    }

    @Override
    public Query preProcessQuery(Query originalQuery, QueryOptions options) {
        Query query = super.preProcessQuery(originalQuery, options);

        VariantStorageMetadataManager metadataManager;
        CellBaseUtils cellBaseUtils;
        try {
            metadataManager = getMetadataManager();
            cellBaseUtils = getCellBaseUtils();
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }
        List<String> studyNames = metadataManager.getStudyNames();

        if (isValidParam(query, STUDY) && studyNames.size() == 1) {
            String study = query.getString(STUDY.key());
            if (!isNegated(study)) {
                try {
                    // Check that study exists
                    getMetadataManager().getStudyId(study);
                } catch (StorageEngineException e) {
                    throw VariantQueryException.internalException(e);
                }
                query.remove(STUDY.key());
            }
        }

        convertGenesToRegionsQuery(query, cellBaseUtils);
        return query;
    }

    @Override
    protected List<VariantAggregationExecutor> initVariantAggregationExecutors() {
        List<VariantAggregationExecutor> executors = new ArrayList<>(3);
        try {
            executors.add(new SearchIndexVariantAggregationExecutor(getVariantSearchManager(), getDBName()));
            executors.add(new SampleIndexVariantAggregationExecutor(getMetadataManager(), getSampleIndexDBAdaptor()));
            executors.add(new ChromDensityVariantAggregationExecutor(this, getMetadataManager()));
        } catch (Exception e) {
            throw VariantQueryException.internalException(e);
        }
        return executors;
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (hBaseManager != null) {
            hBaseManager.close();
            hBaseManager = null;
        }
        if (dbAdaptor.get() != null) {
            dbAdaptor.get().close();
            dbAdaptor.set(null);
        }
        if (sampleIndexDBAdaptor.get() != null) {
//            sampleIndexDBAdaptor.get().close();
            sampleIndexDBAdaptor.set(null);
        }
        if (tableNameGenerator != null) {
            tableNameGenerator = null;
        }
    }

    private HBaseCredentials buildCredentials(String table) throws StorageEngineException {
        StorageEngineConfiguration vStore = configuration.getVariantEngine(STORAGE_ENGINE_ID);

        DatabaseCredentials db = vStore.getDatabase();
        String user = db.getUser();
        String pass = db.getPassword();
        List<String> hostList = db.getHosts();
        if (hostList != null && hostList.size() > 1) {
            throw new IllegalStateException("Expect only one server name");
        }
        String target = hostList != null && !hostList.isEmpty() ? hostList.get(0) : null;
        try {
            String server;
            Integer port;
            String zookeeperPath;
            if (target == null || target.isEmpty()) {
                Configuration conf = getHadoopConfiguration();
                server = conf.get(HConstants.ZOOKEEPER_QUORUM);
                port = 60000;
                zookeeperPath = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            } else {
                URI uri;
                try {
                    uri = new URI(target);
                } catch (URISyntaxException e) {
                    try {
                        uri = new URI("hbase://" + target);
                    } catch (URISyntaxException e1) {
                        throw e;
                    }
                }
                server = uri.getHost();
                port = uri.getPort() > 0 ? uri.getPort() : 60000;
                // If just an IP or host name is provided, the URI parser will return empty host, and the content as "path". Avoid that
                if (server == null) {
                    server = uri.getPath();
                    zookeeperPath = null;
                } else {
                    zookeeperPath = uri.getPath();
                }
            }
            HBaseCredentials credentials;
            if (!StringUtils.isBlank(zookeeperPath)) {
                credentials = new HBaseCredentials(server, table, user, pass, port, zookeeperPath);
            } else {
                credentials = new HBaseCredentials(server, table, user, pass, port);
            }
            return credentials;
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }


    @Override
    public VariantStorageMetadataManager getMetadataManager() throws StorageEngineException {
        return getDBAdaptor().getMetadataManager();
    }

    @Override
    public DataResult<Variant> getSampleData(String variant, String study, QueryOptions options) throws StorageEngineException {
        return new HBaseVariantSampleDataManager(getDBAdaptor(), getCellBaseUtils()).getSampleData(variant, study, options);
    }

    @Override
    protected List<VariantQueryExecutor> initVariantQueryExecutors() throws StorageEngineException {
        List<VariantQueryExecutor> executors = new ArrayList<>(8);

        executors.add(new SampleIndexCompoundHeterozygousQueryExecutor(
                getMetadataManager(), getStorageEngineId(), getOptions(), this, getSampleIndexDBAdaptor(), getDBAdaptor()));
        executors.add(new BreakendVariantQueryExecutor(
                getMetadataManager(), getStorageEngineId(), getOptions(), new SampleIndexVariantQueryExecutor(
                getDBAdaptor(), getSampleIndexDBAdaptor(), getStorageEngineId(), getOptions()), getDBAdaptor()));
        executors.add(new SamplesSearchIndexVariantQueryExecutor(
                getDBAdaptor(), getVariantSearchManager(), getStorageEngineId(), dbName, getConfiguration(), getOptions()));
        executors.add(new SampleIndexMendelianErrorQueryExecutor(
                getDBAdaptor(), getSampleIndexDBAdaptor(), getStorageEngineId(), getOptions()));
        executors.add(new SampleIndexVariantQueryExecutor(
                getDBAdaptor(), getSampleIndexDBAdaptor(), getStorageEngineId(), getOptions()));
        executors.add(new SearchIndexVariantQueryExecutor(
                getDBAdaptor(), getVariantSearchManager(), getStorageEngineId(), dbName, getConfiguration(), getOptions())
                .setIntersectParamsThreshold(1));
        executors.add(new HBaseColumnIntersectVariantQueryExecutor(
                getDBAdaptor(), getStorageEngineId(), getOptions()));
        executors.add(new DBAdaptorVariantQueryExecutor(
                getDBAdaptor(), getStorageEngineId(), getOptions()));

        return executors;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return getHadoopConfiguration();
    }

    private Configuration getHadoopConfiguration() {
        return getHadoopConfiguration(getOptions());
    }

    private Configuration getHadoopConfiguration(ObjectMap options) {
        Configuration conf = this.conf == null ? HBaseConfiguration.create() : this.conf;
        // This is the only key needed to connect to HDFS:
        //   CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY = fs.defaultFS
        //

        if (conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) == null) {
            throw new IllegalArgumentException("Missing configuration parameter \""
                    + CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY + "\"");
        }

        options.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> conf.set(entry.getKey(), options.getString(entry.getKey())));
        return conf;
    }

    public MRExecutor getMRExecutor() throws StorageEngineException {
        if (mrExecutor == null) {
            mrExecutor = MRExecutorFactory.getMRExecutor(getOptions());
        }
        return mrExecutor;
    }

    /**
     * Get the archive table name given a StudyId.
     *
     * @param studyId Numerical study identifier
     * @return Table name
     */
    public String getArchiveTableName(int studyId) {
        return getTableNameGenerator().getArchiveTableName(studyId);
    }

    public String getVariantTableName() {
        return getTableNameGenerator().getVariantTableName();
    }

    private HBaseVariantTableNameGenerator getTableNameGenerator() {
        if (tableNameGenerator == null) {
            tableNameGenerator = new HBaseVariantTableNameGenerator(dbName, getOptions());
        }
        return tableNameGenerator;
    }

    @Override
    public void testConnection() throws StorageEngineException {
        try {
            Configuration conf = getHadoopConfiguration();
            try {
                // HBase 2.x
//                HBaseAdmin.available(conf);
                HBaseAdmin.class.getMethod("available", Configuration.class).invoke(null, conf);
            } catch (NoSuchMethodException e) {
                // HBase 1.x
//                HBaseAdmin.checkHBaseAvailable(conf);
                HBaseAdmin.class.getMethod("checkHBaseAvailable", Configuration.class).invoke(null, conf);
            }
        } catch (Exception e) {
            logger.error("Connection to database '" + dbName + "' failed", e);
            throw new StorageEngineException("HBase Database connection test failed", e);
        }
    }

}
