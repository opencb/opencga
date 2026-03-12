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

package org.opencb.opencga.storage.mongodb.variant;

import com.google.common.base.Throwables;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.executors.VariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.mongodb.annotation.MongoDBVariantAnnotationManager;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.variant.gaps.MongoDBFillGapsFromFile;
import org.opencb.opencga.storage.mongodb.variant.gaps.MongoDBFillGapsTask;
import org.opencb.opencga.storage.mongodb.metadata.MongoDBVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.io.MongoDBVariantExporter;
import org.opencb.opencga.storage.mongodb.variant.load.MongoVariantImporter;
import org.opencb.opencga.storage.mongodb.variant.query.RegionVariantQueryExecutor;
import org.opencb.opencga.storage.mongodb.variant.stats.MongoDBVariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.RESUME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions.*;

/**
 * Created by imedina on 13/08/14.
 */
public class MongoDBVariantStorageEngine extends VariantStorageEngine {

    /*
     * This field defaultValue must be the same that the one at storage-configuration.yml
     */
    public static final String STORAGE_ENGINE_ID = "mongodb";

    // Connection to MongoDB.
    private MongoDataStoreManager mongoDataStoreManager = null;
    private final AtomicReference<VariantMongoDBAdaptor> dbAdaptor = new AtomicReference<>();
    private Logger logger = LoggerFactory.getLogger(MongoDBVariantStorageEngine.class);
    private VariantStorageMetadataManager metadataManager;

    public MongoDBVariantStorageEngine() {
    }

    @Override
    public void testConnection() throws StorageEngineException {
        MongoCredentials credentials = getMongoCredentials();

        if (!credentials.check()) {
            logger.error("Connection to database '{}' failed", dbName);
            throw new StorageEngineException("Database connection test failed");
        }
    }

    @Override
    protected VariantImporter newVariantImporter() throws StorageEngineException {
        return new MongoVariantImporter(getDBAdaptor());
    }

    @Override
    protected VariantExporter newVariantExporter(VariantMetadataFactory metadataFactory) throws StorageEngineException {
        return new MongoDBVariantExporter(this, metadataFactory, ioConnectorProvider);
    }

    @Override
    public boolean supportsNativeSparseFilter() {
        return true;
    }

    @Override
    public MongoDBVariantStoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException {
        VariantMongoDBAdaptor dbAdaptor = connected ? getDBAdaptor() : null;
        ObjectMap options = new ObjectMap(getOptions());
        return new MongoDBVariantStoragePipeline(configuration, STORAGE_ENGINE_ID, dbAdaptor, ioConnectorProvider, options,
                getSampleIndexDBAdaptor());
    }

    @Override
    public VariantStatisticsManager newVariantStatisticsManager() throws StorageEngineException {
        return new MongoDBVariantStatisticsManager(getDBAdaptor(), ioConnectorProvider);
    }

    @Override
    protected VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator) throws StorageEngineException {
        VariantMongoDBAdaptor mongoDbAdaptor = getDBAdaptor();
        return new MongoDBVariantAnnotationManager(annotator, mongoDbAdaptor, ioConnectorProvider, getSampleIndexDBAdaptor()
                .newSampleAnnotationIndexer(this));
    }

    @Override
    protected VariantSearchLoadResult secondaryIndex(Query inputQuery, QueryOptions inputQueryOptions, boolean overwrite,
                                                     SearchIndexMetadata indexMetadata, long updateStartTimestamp)
            throws StorageEngineException, IOException, VariantSearchException {
        VariantSearchManager variantSearchManager = getVariantSearchManager();

        int deletedVariants;
        VariantSearchLoadResult searchIndex;

        if (configuration.getSearch().isActive() && variantSearchManager.isAlive(indexMetadata)) {
            // First remove trashed variants.
            ProgressLogger progressLogger = new ProgressLogger("Variants removed from Solr");
            try (VariantDBIterator removedVariants = getDBAdaptor().trashedVariants(updateStartTimestamp)) {
                deletedVariants = variantSearchManager.delete(indexMetadata, removedVariants, progressLogger);
                getDBAdaptor().cleanTrash(updateStartTimestamp);
            } catch (StorageEngineException | IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new StorageEngineException("Exception closing VariantDBIterator", e);
            }

            // Then, load new variants.
            searchIndex = super.secondaryIndex(inputQuery, inputQueryOptions, overwrite, indexMetadata, updateStartTimestamp);
        } else {
            //The current dbName from the SearchEngine is not alive or does not exist. There is nothing to remove
            deletedVariants = 0;
            logger.debug("Skip removed variants!");

            // Try to index the rest of variants. This method will fail if the search engine is not alive
            searchIndex = super.secondaryIndex(inputQuery, inputQueryOptions, overwrite, indexMetadata, updateStartTimestamp);

            // If the variants were loaded correctly, the trash can be clean up.
            getDBAdaptor().cleanTrash(updateStartTimestamp);
        }

        return new VariantSearchLoadResult(
                searchIndex.getNumProcessedVariants(),
                searchIndex.getNumLoadedVariants(),
                deletedVariants,
                searchIndex.getNumInsertedVariants(),
                searchIndex.getNumLoadedVariantsPartialStatsUpdate());
    }

    @Override
    public void removeFiles(String study, List<String> files, URI outdir) throws StorageEngineException {

        TaskMetadata task = preRemove(study, files, Collections.emptyList());
        List<Integer> fileIds = task.getFileIds();

        ObjectMap options = new ObjectMap(getOptions());

        VariantStorageMetadataManager scm = getMetadataManager();
        int studyId = scm.getStudyId(study);

        // Collect samples in the removed files and which ones still have other indexed files.
        Set<Integer> otherIndexedFiles = new HashSet<>(scm.getIndexedFiles(studyId));
        otherIndexedFiles.removeAll(fileIds);
        Set<Integer> allRemovedSampleIds = new HashSet<>();
        List<String> samplesToRebuildIndex = new ArrayList<>();
        for (Integer fileId : fileIds) {
            for (Integer sampleId : scm.getFileMetadata(studyId, fileId).getSamples()) {
                allRemovedSampleIds.add(sampleId);
                // Sample is still present in the study via other files — its sample-index needs rebuilding.
                if (scm.getSampleMetadata(studyId, sampleId).getFiles().stream().anyMatch(otherIndexedFiles::contains)) {
                    samplesToRebuildIndex.add(scm.getSampleName(studyId, sampleId));
                }
            }
        }

        MongoDBSampleIndexDBAdaptor mongoSampleIndexDBAdaptor = (MongoDBSampleIndexDBAdaptor) getSampleIndexDBAdaptor();
        int schemaVersion = mongoSampleIndexDBAdaptor.getSchemaLatest(study).getVersion();

        Thread hook = scm.buildShutdownHook(REMOVE_OPERATION_NAME, studyId, task.getId());
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            getDBAdaptor().removeFiles(study, files, task.getTimestamp(), new QueryOptions(options));
            postRemoveFiles(study, fileIds, Collections.emptyList(), task.getId(), false);
            // Clear sample index for ALL samples in removed files to remove stale data.
            if (!allRemovedSampleIds.isEmpty()) {
                mongoSampleIndexDBAdaptor.clearSampleIndex(studyId, schemaVersion, allRemovedSampleIds);
            }
            // Rebuild sample index for samples that still have data after the removal.
            if (!samplesToRebuildIndex.isEmpty()) {
                sampleIndex(study, samplesToRebuildIndex, new ObjectMap(options).append("overwrite", true));
            }
        } catch (Exception e) {
            postRemoveFiles(study, fileIds, Collections.emptyList(), task.getId(), true);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    @Override
    public void removeSamples(String study, List<String> samples, URI outdir) throws StorageEngineException {
        VariantStorageMetadataManager mm = getMetadataManager();
        int studyId = mm.getStudyId(study);

        // Resolve sample IDs
        List<Integer> sampleIds = new ArrayList<>(samples.size());
        for (String sample : samples) {
            sampleIds.add(mm.getSampleId(studyId, sample));
        }
        Set<Integer> sampleIdSet = new HashSet<>(sampleIds);

        // Classify files: fully deleted (all samples removed) vs partially deleted
        Set<Integer> affectedFileIds = mm.getFileIdsFromSampleIds(studyId, sampleIdSet, true);
        List<String> fullyDeletedFiles = new ArrayList<>();
        List<Integer> fullyDeletedFileIds = new ArrayList<>();
        Set<Integer> partiallyDeletedFileIds = new LinkedHashSet<>();

        for (Integer fileId : affectedFileIds) {
            LinkedHashSet<Integer> samplesFromFile = mm.getSampleIdsFromFileId(studyId, fileId);
            if (sampleIdSet.containsAll(samplesFromFile)) {
                fullyDeletedFileIds.add(fileId);
                fullyDeletedFiles.add(mm.getFileName(studyId, fileId));
            } else {
                partiallyDeletedFileIds.add(fileId);
            }
        }

        TaskMetadata task = preRemove(study, fullyDeletedFiles, samples);
        ObjectMap options = new ObjectMap(getOptions());
        MongoDBSampleIndexDBAdaptor mongoSampleIndexDBAdaptor = (MongoDBSampleIndexDBAdaptor) getSampleIndexDBAdaptor();
        int schemaVersion = mongoSampleIndexDBAdaptor.getSchemaLatest(study).getVersion();

        Thread hook = mm.buildShutdownHook(REMOVE_OPERATION_NAME, studyId, task.getId());
        try {
            Runtime.getRuntime().addShutdownHook(hook);

            // 1. Remove fully deleted files (reuse existing logic)
            if (!fullyDeletedFileIds.isEmpty()) {
                getDBAdaptor().removeFiles(study, fullyDeletedFiles, task.getTimestamp(), new QueryOptions(options));
            }

            // 2. Remove samples from partially deleted files
            if (!partiallyDeletedFileIds.isEmpty()) {
                getDBAdaptor().removeSamples(studyId, sampleIdSet, partiallyDeletedFileIds, task.getTimestamp());
            }

            // 3. Metadata cleanup
            postRemoveFiles(study, fullyDeletedFileIds, sampleIds, task.getId(), false);

            // 4. Clear sample index for all removed samples
            mongoSampleIndexDBAdaptor.clearSampleIndex(studyId, schemaVersion, sampleIdSet);

            // 5. Rebuild sample index for remaining samples in partially deleted files
            Set<String> samplesToRebuildIndex = new LinkedHashSet<>();
            for (Integer fileId : partiallyDeletedFileIds) {
                for (Integer sid : mm.getSampleIdsFromFileId(studyId, fileId)) {
                    if (!sampleIdSet.contains(sid)) {
                        samplesToRebuildIndex.add(mm.getSampleName(studyId, sid));
                    }
                }
            }
            if (!samplesToRebuildIndex.isEmpty()) {
                List<Integer> rebuildIds = new ArrayList<>(samplesToRebuildIndex.size());
                for (String s : samplesToRebuildIndex) {
                    rebuildIds.add(mm.getSampleId(studyId, s));
                }
                mongoSampleIndexDBAdaptor.clearSampleIndex(studyId, schemaVersion, rebuildIds);
                sampleIndex(study, new ArrayList<>(samplesToRebuildIndex), new ObjectMap(options).append("overwrite", true));
            }
        } catch (Exception e) {
            postRemoveFiles(study, fullyDeletedFileIds, sampleIds, task.getId(), true);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    @Override
    public void aggregateFamily(String study, VariantAggregateFamilyParams params, ObjectMap options, URI outdir)
            throws StorageEngineException {
        List<String> samples = params.getSamples();
        if (samples == null || samples.size() < 2) {
            throw new IllegalArgumentException("Aggregate family operation requires at least two samples.");
        } else if (new HashSet<>(samples).size() != samples.size()) {
            throw new IllegalArgumentException("Unable to execute aggregate-family operation with duplicated samples.");
        }

        VariantStorageMetadataManager mm = getMetadataManager();
        StudyMetadata studyMetadata = mm.getStudyMetadata(study);
        int studyId = studyMetadata.getId();
        List<Integer> sampleIds = new ArrayList<>(samples.size());
        Set<Integer> fileIds = new LinkedHashSet<>();
        for (String sample : samples) {
            Integer sampleId = mm.getSampleId(studyId, sample);
            if (sampleId == null) {
                throw VariantQueryException.sampleNotFound(sample, studyMetadata.getName());
            }
            sampleIds.add(sampleId);
            List<Integer> sampleFiles = mm.getFileIdsFromSampleId(studyId, sampleId, true);
            if (sampleFiles.size() > 1) {
                throw new IllegalArgumentException("Unable to execute operation with more than one file per sample. Found "
                        + sampleFiles.size() + " files for sample " + sample);
            }
            fileIds.addAll(sampleFiles);
        }

        // Resolve VCF file URIs — all files must exist on filesystem
        List<URI> uris = new ArrayList<>();
        for (Integer fileId : fileIds) {
            FileMetadata fileMetadata = mm.getFileMetadata(studyId, fileId);
            Path filePath = Paths.get(fileMetadata.getPath());
            if (!filePath.toFile().exists()) {
                throw new StorageEngineException("File not found: " + filePath
                        + ". MongoDB aggregateFamily requires the original VCF files to be accessible.");
            }
            uris.add(filePath.toUri());
        }

        String gapsGenotype = params.getGapsGenotype();
        if (gapsGenotype == null || gapsGenotype.isEmpty()) {
            gapsGenotype = "0/0";
        }
        logger.info("FillGaps: Study " + study + ", samples " + samples);

        // Register cohort
        int internalCohortId = mm.registerAggregateFamilySamplesCohort(
                studyId, samples, params.isResume(), params.isResume());

        // Reset family index status to NONE
        for (Integer sampleId : sampleIds) {
            mm.updateSampleMetadata(studyId, sampleId, sm -> {
                Integer version = sm.getFamilyIndexVersion();
                if (version != null) {
                    logger.info("Updating family index status for sample '{}' to {}", sm.getName(), TaskMetadata.Status.NONE);
                    sm.setFamilyIndexStatus(TaskMetadata.Status.NONE, version);
                }
            });
        }

        try {
            MongoDBFillGapsFromFile fillGapsFromFile = new MongoDBFillGapsFromFile(
                    getDBAdaptor(), mm, options != null ? options : new ObjectMap());
            fillGapsFromFile.fillGaps(studyMetadata.getName(), uris, gapsGenotype);

            // Update loaded genotypes
            MongoDBFillGapsTask tempTask = new MongoDBFillGapsTask(mm, studyMetadata, false, gapsGenotype);
            tempTask.updateLoadedGenotypes();

            mm.updateCohortMetadata(studyId, internalCohortId, cohort -> {
                cohort.setStatusByType(TaskMetadata.Status.READY);
            });
            mm.removeExtraInternalCohorts(studyId, internalCohortId);

            // Rebuild sample index for affected samples
            MongoDBSampleIndexDBAdaptor sampleIndexAdaptor = (MongoDBSampleIndexDBAdaptor) getSampleIndexDBAdaptor();
            int schemaVersion = sampleIndexAdaptor.getSchemaLatest(study).getVersion();
            sampleIndexAdaptor.clearSampleIndex(studyId, schemaVersion, sampleIds);

            List<String> sampleNames = new ArrayList<>(sampleIds.size());
            for (Integer sampleId : sampleIds) {
                sampleNames.add(mm.getSampleName(studyId, sampleId));
            }
            sampleIndex(study, sampleNames, new ObjectMap(options != null ? options : new ObjectMap())
                    .append("overwrite", true));
        } catch (Exception e) {
            try {
                mm.updateCohortMetadata(studyId, internalCohortId, cohort -> {
                    cohort.setStatusByType(TaskMetadata.Status.ERROR);
                });
            } catch (Exception e1) {
                e.addSuppressed(e1);
            }
            throw e instanceof StorageEngineException ? (StorageEngineException) e : new StorageEngineException(e.getMessage(), e);
        }
    }

    @Override
    public void removeStudy(String studyName, URI outdir) throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        AtomicReference<TaskMetadata> batchFileOperation = new AtomicReference<>();
        AtomicReference<TaskMetadata> taskMetadata = new AtomicReference<>();
        StudyMetadata studyMetadata = metadataManager.updateStudyMetadata(studyName, sm -> {
            boolean resume = getOptions().getBoolean(RESUME.key(), RESUME.defaultValue());
            taskMetadata.set(metadataManager.addRunningTask(sm.getId(),
                    REMOVE_OPERATION_NAME,
                    Collections.emptyList(),
                    resume,
                    TaskMetadata.Type.REMOVE));
            return sm;
        });
        int studyId = studyMetadata.getId();

        int taskId = taskMetadata.get().getId();
        Thread hook = metadataManager.buildShutdownHook(REMOVE_OPERATION_NAME, studyId, taskId);
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            ObjectMap options = new ObjectMap(getOptions());
            getDBAdaptor().removeStudy(studyName, batchFileOperation.get().getTimestamp(), new QueryOptions(options));

            LinkedHashSet<Integer> indexedFiles = metadataManager.getIndexedFiles(studyId);
            for (Integer fileId : indexedFiles) {
                getDBAdaptor().getMetadataManager().removeVariantFileMetadata(studyId, fileId);
            }

            metadataManager.removeIndexedFiles(studyId, indexedFiles);

            metadataManager.setStatus(studyId, taskId, TaskMetadata.Status.READY);
        } catch (Exception e) {
            metadataManager.setStatus(studyId, taskId, TaskMetadata.Status.ERROR);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    @Override
    public void loadVariantScore(URI scoreFile, String study, String scoreName, String cohort1, String cohort2,
                                 VariantScoreFormatDescriptor descriptor, ObjectMap options) {
        throw new UnsupportedOperationException("Unable to load VariantScore in " + getStorageEngineId());
    }

    @Override
    public void deleteVariantScore(String study, String scoreName, ObjectMap options) throws StorageEngineException {
        throw new UnsupportedOperationException("Unable to remove VariantScore in " + getStorageEngineId());
    }

    @Override
    public List<StoragePipelineResult> index(List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean doTransform, boolean doLoad)
            throws StorageEngineException {
        if (doLoad) {
            createStudyIfNeeded();
        }
        Map<URI, MongoDBVariantStoragePipeline> storageResultMap = new LinkedHashMap<>();
        Map<URI, StoragePipelineResult> resultsMap = new LinkedHashMap<>();
        LinkedList<StoragePipelineResult> results = new LinkedList<>();

//        MemoryUsageMonitor monitor = new MemoryUsageMonitor();
//        monitor.setDelay(5, TimeUnit.SECONDS);
//        monitor.start();
        try {
            for (URI inputFile : inputFiles) {
                StoragePipelineResult storagePipelineResult = new StoragePipelineResult(inputFile);
                MongoDBVariantStoragePipeline storagePipeline = newStoragePipeline(doLoad);
                storagePipeline.getOptions().append(VariantStorageOptions.TRANSFORM_ISOLATE.key(), true);
                storageResultMap.put(inputFile, storagePipeline);
                resultsMap.put(inputFile, storagePipelineResult);
                results.add(storagePipelineResult);
            }


            if (doExtract) {
                for (Map.Entry<URI, MongoDBVariantStoragePipeline> entry : storageResultMap.entrySet()) {
                    URI uri = entry.getValue().extract(entry.getKey(), outdirUri);
                    resultsMap.get(entry.getKey()).setExtractResult(uri);
                }
            }

            if (doTransform) {
                for (Map.Entry<URI, MongoDBVariantStoragePipeline> entry : storageResultMap.entrySet()) {
                    StoragePipelineResult result = resultsMap.get(entry.getKey());
                    URI input = result.getExtractResult() == null ? entry.getKey() : result.getExtractResult();
                    transformFile(entry.getValue(), result, results, input, outdirUri);
                }
            }

            boolean doStage = doLoad && getOptions().getBoolean(STAGE.key());
            boolean doMerge = doLoad && getOptions().getBoolean(MERGE.key());
            if (!doStage && !doMerge) {
                doStage = doLoad;
                doMerge = doLoad;
            }

            if (doLoad) {
                int batchLoad = getOptions().getInt(MERGE_BATCH_SIZE.key(), MERGE_BATCH_SIZE.defaultValue());
                // Files to merge
                List<Integer> filesToMerge = new ArrayList<>(batchLoad);
                List<StoragePipelineResult> resultsToMerge = new ArrayList<>(batchLoad);
                List<Integer> mergedFiles = new ArrayList<>();

                Iterator<Map.Entry<URI, MongoDBVariantStoragePipeline>> iterator = storageResultMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<URI, MongoDBVariantStoragePipeline> entry = iterator.next();
                    StoragePipelineResult result = resultsMap.get(entry.getKey());
                    URI input = result.getPostTransformResult() == null ? entry.getKey() : result.getPostTransformResult();
                    MongoDBVariantStoragePipeline storagePipeline = entry.getValue();

                    StopWatch loadWatch = StopWatch.createStarted();
                    try {
                        boolean doDirectLoad;
                        // Decide if use direct load or not.
                        if (doStage && doMerge) {
                            doDirectLoad = storagePipeline.checkCanLoadDirectly(inputFiles);
                        } else {
                            doDirectLoad = false;
                        }

                        storagePipeline.getOptions().put(STAGE.key(), doStage);
                        storagePipeline.getOptions().put(MERGE.key(), doMerge);
                        storagePipeline.getOptions().put(DIRECT_LOAD.key(), doDirectLoad);

                        logger.info("PreLoad '{}'", input);
                        input = storagePipeline.preLoad(input, outdirUri);
                        result.setPreLoadResult(input);

                        if (doDirectLoad) {
                            storagePipeline.getOptions().put(STAGE.key(), false);
                            storagePipeline.getOptions().put(MERGE.key(), false);
                            storagePipeline.directLoad(input, outdirUri);
                            result.setLoadExecuted(true);
                            result.setLoadStats(storagePipeline.getLoadStats());
                            result.setLoadTimeMillis(loadWatch.getTime(TimeUnit.MILLISECONDS));
                        } else {
                            if (doStage) {
                                logger.info("Load - Stage '{}'", input);
                                storagePipeline.stage(input, outdirUri);
                                result.setLoadResult(input);
                                result.setLoadStats(storagePipeline.getLoadStats());
                                result.getLoadStats().put(STAGE.key(), true);
                                result.setLoadTimeMillis(loadWatch.getTime(TimeUnit.MILLISECONDS));
                            }

                            if (doMerge) {
                                logger.info("Load - Merge '{}'", input);
                                filesToMerge.add(storagePipeline.getFileId());
                                resultsToMerge.add(result);

                                if (filesToMerge.size() == batchLoad || !iterator.hasNext()) {
                                    StopWatch mergeWatch = StopWatch.createStarted();
                                    try {
                                        storagePipeline.merge(new ArrayList<>(filesToMerge));
                                    } catch (Exception e) {
                                        for (StoragePipelineResult storagePipelineResult : resultsToMerge) {
                                            storagePipelineResult.setLoadError(e);
                                        }
                                        throw new StoragePipelineException("Exception executing merge.", e, results);
                                    } finally {
                                        long mergeTime = mergeWatch.getTime(TimeUnit.MILLISECONDS);
                                        for (StoragePipelineResult storagePipelineResult : resultsToMerge) {
                                            storagePipelineResult.setLoadTimeMillis(storagePipelineResult.getLoadTimeMillis() + mergeTime);
                                            for (Map.Entry<String, Object> statsEntry : storagePipeline.getLoadStats().entrySet()) {
                                                storagePipelineResult.getLoadStats()
                                                        .putIfAbsent(statsEntry.getKey(), statsEntry.getValue());
                                            }
                                            storagePipelineResult.setLoadExecuted(true);
                                        }
                                        mergedFiles.addAll(filesToMerge);
                                        filesToMerge.clear();
                                        resultsToMerge.clear();
                                    }
                                } else {
                                    // We don't execute merge for this file
                                    storagePipeline.getOptions().put(MERGE.key(), false);
                                }
                            }
                        }


                        logger.info("PostLoad '{}'", input);
                        input = storagePipeline.postLoad(input, outdirUri);
                        result.setPostLoadResult(input);
                    } catch (Exception e) {
                        if (result.getLoadError() == null) {
                            result.setLoadError(e);
                        }
                        if (!(e instanceof StoragePipelineException)) {
                            throw new StoragePipelineException("Exception executing load: " + e.getMessage(), e, results);
                        } else {
                            throw e;
                        }
                    } finally {
                        if (result.getLoadTimeMillis() == 0) {
                            result.setLoadTimeMillis(loadWatch.getTime(TimeUnit.MILLISECONDS));
                        }
                        if (result.getLoadStats() == null) {
                            result.setLoadStats(storagePipeline.getLoadStats());
                        }
                    }

                }
                if (doMerge) {
                    StudyMetadata metadata = storageResultMap.get(inputFiles.get(0)).getStudyMetadata();
                    ObjectMap options = getOptions();
                    options.put(VariantStorageOptions.STUDY.key(), metadata.getName());

                    annotateLoadedFiles(outdirUri, inputFiles, results, options);
                    calculateStatsForLoadedFiles(outdirUri, inputFiles, results, options);
                    searchIndexLoadedFiles(inputFiles, options);
                }
            }

        } finally {
//            monitor.interrupt();
            for (StoragePipeline storagePipeline : storageResultMap.values()) {
                storagePipeline.close();
            }
        }

        return results;
    }

    @Override
    public VariantMongoDBAdaptor getDBAdaptor() {
        // Lazy initialization of dbAdaptor
        if (dbAdaptor.get() == null) {
            synchronized (dbAdaptor) {
                if (dbAdaptor.get() == null) {
                    VariantMongoDBAdaptor variantMongoDBAdaptor = newDBAdaptor();
                    this.dbAdaptor.set(variantMongoDBAdaptor);
                }
            }
        }
        return dbAdaptor.get();
    }

    @Override
    public MongoDBSampleIndexDBAdaptor getSampleIndexDBAdaptor() throws StorageEngineException {
        MongoDataStoreManager mongoManager = getMongoDataStoreManager();
        MongoCredentials credentials = getMongoCredentials();
        MongoDataStore db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        return new MongoDBSampleIndexDBAdaptor(db, getMetadataManager());
    }

    @Override
    protected VariantQueryParser getVariantQueryParser() throws StorageEngineException {
        return new MongoDBVariantQueryParser(getCellBaseUtils(), getMetadataManager());
    }

    @Override
    protected List<VariantQueryExecutor> initVariantQueryExecutors() throws StorageEngineException {
        List<VariantQueryExecutor> executors = new ArrayList<>();

        // First, detect if it's a region only query.
        executors.add(new RegionVariantQueryExecutor(getDBAdaptor(), getStorageEngineId(), getOptions()));
        // Then, add the default executors
        executors.addAll(super.initVariantQueryExecutors());

        return executors;
    }

    private VariantMongoDBAdaptor newDBAdaptor() {
        MongoCredentials credentials = getMongoCredentials();
        VariantMongoDBAdaptor variantMongoDBAdaptor;
        ObjectMap options = getOptions();

        String variantsCollection = options.getString(COLLECTION_VARIANTS.key(), COLLECTION_VARIANTS.defaultValue());
        String filesCollection = options.getString(COLLECTION_FILES.key(), COLLECTION_FILES.defaultValue());
        MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager();
        try {
            VariantStorageMetadataManager variantStorageMetadataManager = getMetadataManager();
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(mongoDataStoreManager, credentials, variantsCollection,
                    variantStorageMetadataManager, configuration);

        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
        logger.debug("getting DBAdaptor to db: {}", credentials.getMongoDbName());
        return variantMongoDBAdaptor;
    }

    MongoCredentials getMongoCredentials() {

        DatabaseCredentials database = configuration.getVariantEngine(STORAGE_ENGINE_ID).getDatabase();

        try {
            return new MongoCredentials(database, dbName);
        } catch (IllegalOpenCGACredentialsException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public VariantStorageMetadataManager getMetadataManager() {
        ObjectMap options = getOptions();
        if (metadataManager != null) {
            return metadataManager;
        } else {
            MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager();
            MongoDataStore db = mongoDataStoreManager.get(
                    getMongoCredentials().getMongoDbName(),
                    getMongoCredentials().getMongoDBConfiguration());
            metadataManager = new VariantStorageMetadataManager(new MongoDBVariantStorageMetadataDBAdaptorFactory(db, options));
            return metadataManager;
        }
    }

    @Override
    public Query preProcessQuery(Query originalQuery, QueryOptions options) {
        Query query = super.preProcessQuery(originalQuery, options);
        List<String> studyNames = metadataManager.getStudyNames();

        if (isValidParam(query, VariantQueryParam.STUDY)
                && studyNames.size() == 1
                && !isNegated(query.getString(VariantQueryParam.STUDY.key()))
                && !isValidParam(query, FILE)
                && !isValidParam(query, FILTER)
                && !isValidParam(query, QUAL)
                && !isValidParam(query, FILE_DATA)
                && !isValidParam(query, SAMPLE)
                && !isValidParam(query, SAMPLE_DATA)
                && !isValidParam(query, GENOTYPE)) {
            query.remove(VariantQueryParam.STUDY.key());
        }

        return query;
    }

    private synchronized MongoDataStoreManager getMongoDataStoreManager() {
        if (mongoDataStoreManager == null) {
            mongoDataStoreManager = new MongoDataStoreManager(getMongoCredentials().getDataStoreServerAddresses());
        }
        return mongoDataStoreManager;
    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
        if (dbAdaptor.get() != null) {
            dbAdaptor.get().close();
            dbAdaptor.set(null);
        }
        if (metadataManager != null) {
            metadataManager.close();
            metadataManager = null;
        }
        if (mongoDataStoreManager != null) {
            mongoDataStoreManager.close();
            mongoDataStoreManager = null;
        }
    }

}
