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
import com.google.common.collect.BiMap;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.VariantDeduplicationTask;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOManagerProvider;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.MergeMode;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.transform.DiscardDuplicatedVariantsResolver;
import org.opencb.opencga.storage.core.variant.transform.RemapVariantIdsTask;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.exceptions.MongoVariantStorageEngineException;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantWriteResult;
import org.opencb.opencga.storage.mongodb.variant.load.direct.MongoDBVariantDirectLoader;
import org.opencb.opencga.storage.mongodb.variant.load.direct.MongoDBVariantStageAndFileReader;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageConverterTask;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageReader;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBOperations;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBVariantMergeLoader;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBVariantMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.*;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.*;
import static org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBQueryParser.OVERLAPPED_FILES_ONLY;

/**
 * Created on 30/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStoragePipeline extends VariantStoragePipeline {

    // Type of variants that won't be loaded.
    public static final Set<VariantType> SKIPPED_VARIANTS = Collections.unmodifiableSet(EnumSet.of(
            VariantType.NO_VARIATION,
            VariantType.SYMBOLIC,
//            VariantType.CNV,
//            VariantType.DUPLICATION,
//            VariantType.INVERSION,
            VariantType.TRANSLOCATION
//            VariantType.BREAKEND
    ));

    private final VariantMongoDBAdaptor dbAdaptor;
    private final ObjectMap loadStats = new ObjectMap();
    private final Logger logger = LoggerFactory.getLogger(MongoDBVariantStoragePipeline.class);
    private MongoDBVariantWriteResult writeResult;
    private List<Integer> fileIds;
    // current running task
    private TaskMetadata currentTask;

    public MongoDBVariantStoragePipeline(StorageConfiguration configuration, String storageEngineId,
                                         VariantMongoDBAdaptor dbAdaptor, IOManagerProvider ioManagerProvider) {
        super(configuration, storageEngineId, dbAdaptor, ioManagerProvider);
        this.dbAdaptor = dbAdaptor;
    }

    public URI preLoad(URI input, URI output) throws StorageEngineException {
        URI uri = super.preLoad(input, output);
        if (isResumeStage(options)) {
            logger.info("Resume stage load.");
            // Clean stage collection?
        }
        return uri;
    }

    @Override
    protected void securePreLoad(StudyMetadata studyMetadata, VariantFileMetadata source) throws StorageEngineException {
        super.securePreLoad(studyMetadata, source);
        int fileId = getFileId();

        // 1) Determine merge mode
        if (studyMetadata.getAttributes().containsKey(Options.MERGE_MODE.key())
                || studyMetadata.getAttributes().containsKey(MERGE_IGNORE_OVERLAPPING_VARIANTS.key())) {
            if (studyMetadata.getAttributes().getBoolean(MERGE_IGNORE_OVERLAPPING_VARIANTS.key())) {
                studyMetadata.getAttributes().put(Options.MERGE_MODE.key(), MergeMode.BASIC);
                logger.debug("Do not merge overlapping variants, as said in the StudyMetadata");
            } else {
                studyMetadata.getAttributes().put(Options.MERGE_MODE.key(), MergeMode.ADVANCED);
                logger.debug("Merge overlapping variants, as said in the StudyMetadata");
            }
            options.put(Options.MERGE_MODE.key(), studyMetadata.getAttributes().get(Options.MERGE_MODE.key()));
        } else {
            MergeMode mergeMode = MergeMode.from(options);
            studyMetadata.getAttributes().put(Options.MERGE_MODE.key(), mergeMode);
            switch (mergeMode) {
                case BASIC:
                    studyMetadata.getAttributes().put(MERGE_IGNORE_OVERLAPPING_VARIANTS.key(), true);
                    break;
                case ADVANCED:
                    studyMetadata.getAttributes().put(MERGE_IGNORE_OVERLAPPING_VARIANTS.key(), false);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown merge mode: " + mergeMode);
            }
        }

        // 2) Determine DEFAULT_GENOTYPE
        if (studyMetadata.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key()).contains(GenotypeClass.UNKNOWN_GENOTYPE)) {
            // Remove if UNKNOWN_GENOTYPE
            studyMetadata.getAttributes().remove(DEFAULT_GENOTYPE.key());
        }
        if (studyMetadata.getAttributes().containsKey(DEFAULT_GENOTYPE.key())) {
            Set<String> defaultGenotype = new HashSet<>(studyMetadata.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key()));
            logger.debug("Using default genotype from study configuration: {}", defaultGenotype);
        } else {
            // Read from configuration file, or get the default value
            Set<String> defaultGenotype;
            if (options.containsKey(DEFAULT_GENOTYPE.key())) {
                defaultGenotype = new HashSet<>(options.getAsStringList(DEFAULT_GENOTYPE.key()));
                if (defaultGenotype.contains(GenotypeClass.UNKNOWN_GENOTYPE)) {
                    throw new StorageEngineException("Unable to use genotype '" + GenotypeClass.UNKNOWN_GENOTYPE + "' as "
                            + DEFAULT_GENOTYPE.key());
                }
            } else {
                defaultGenotype = new HashSet<>(DEFAULT_GENOTYPE.defaultValue());
            }
            studyMetadata.getAttributes().put(DEFAULT_GENOTYPE.key(), defaultGenotype);
        }

        boolean loadSplitData = options.getBoolean(Options.LOAD_SPLIT_DATA.key(), Options.LOAD_SPLIT_DATA.defaultValue());
        boolean newSampleBatch = checkCanLoadSampleBatch(getMetadataManager(), studyMetadata, fileId, loadSplitData);

        if (newSampleBatch) {
            logger.info("New sample batch!!!");
            //TODO: Check if there are regions with gaps
//            ArrayList<Integer> indexedFiles = new ArrayList<>(studyConfiguration.getIndexedFiles());
//            if (!indexedFiles.isEmpty()) {
//                LinkedHashSet<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(indexedFiles.get(indexedFiles.size() - 1));
//                if (!sampleIds.isEmpty()) {
//                    Integer sampleId = sampleIds.iterator().next();
//                    String files = "";
//                    for (Integer indexedFileId : indexedFiles) {
//                        if (studyConfiguration.getSamplesInFiles().get(indexedFileId).contains(sampleId)) {
//                            files += "!" + indexedFileId + ";";
//                        }
//                    }
////                    String genotypes = sampleIds.stream().map(i -> studyConfiguration.getSampleIds().inverse().get(i) + ":" +
// DBObjectToSamplesConverter.UNKNOWN_GENOTYPE).collect(Collectors.joining(","));
//                    String genotypes = sampleId + ":" + DBObjectToSamplesConverter.UNKNOWN_GENOTYPE;
//                    Long v = getDBAdaptor(null).count(new Query()
//                            .append(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId())
//                            .append(VariantDBAdaptor.VariantQueryParams.FILES.key(), files)
//                            .append(VariantDBAdaptor.VariantQueryParams.GENOTYPE.key(), genotypes)).first();
//                }
//            }
        }

        boolean doMerge = options.getBoolean(MERGE.key(), false);
        boolean doStage = options.getBoolean(STAGE.key(), false);

        if (!doMerge && !doStage) {
            doMerge = true;
            doStage = true;
        }
        options.put(MERGE.key(), doMerge);
        options.put(STAGE.key(), doStage);

        if (options.getBoolean(DIRECT_LOAD.key(), DIRECT_LOAD.defaultValue())) {
            // TODO: Check if can execute direct load
            currentTask = getMetadataManager().addRunningTask(
                    studyMetadata.getId(),
                    DIRECT_LOAD.key(),
                    Collections.singletonList(fileId),
                    isResume(options),
                    TaskMetadata.Type.LOAD);
            if (currentTask.getStatus().size() > 1) {
                options.put(Options.RESUME.key(), true);
                options.put(STAGE_RESUME.key(), true);
                options.put(MERGE_RESUME.key(), true);
            }
        } else {
            securePreStage(fileId, studyMetadata);
        }
//        QueryResult<Long> countResult = dbAdaptor.count(new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration
//                .getStudyId())
//                .append(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileId));
//        Long count = countResult.first();
//        if (count != 0) {
//            logger.warn("Resume mode. There are already loaded variants from the file "
//                    + studyConfiguration.getFileIds().inverse().get(fileId) + " : " + fileId + " ");
//            options.put(ALREADY_LOADED_VARIANTS.key(), count);
//        }
    }

    @Override
    public URI load(URI inputUri) throws IOException, StorageEngineException {


//        boolean includeSamples = options.getBoolean(Options.INCLUDE_GENOTYPES.key(), Options.INCLUDE_GENOTYPES.defaultValue());
//        boolean includeStats = options.getBoolean(Options.INCLUDE_STATS.key(), Options.INCLUDE_STATS.defaultValue());
//        boolean includeSrc = options.getBoolean(Options.INCLUDE_SRC.key(), Options.INCLUDE_SRC.defaultValue());
//        boolean compressGenotypes = options.getBoolean(Options.COMPRESS_GENOTYPES.key(), false);
//        boolean compressGenotypes = defaultGenotype != null && !defaultGenotype.isEmpty();

        final int fileId = getFileId();

        logger.info("Loading variants...");
        long start = System.currentTimeMillis();

        boolean directLoad = options.getBoolean(DIRECT_LOAD.key(), DIRECT_LOAD.defaultValue());
        if (directLoad) {
            directLoad(inputUri);
        } else {
            boolean doMerge = options.getBoolean(MERGE.key(), false);
            boolean doStage = options.getBoolean(STAGE.key(), false);

            if (doStage) {
                stage(inputUri);
            }

            long skippedVariants = options.getLong("skippedVariants");
            if (doMerge) {
                merge(Collections.singletonList(fileId), skippedVariants);
            }
        }
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants loaded!");


        return inputUri; //TODO: Return something like this: mongo://<host>/<dbName>/<collectionName>
    }

    public void directLoad(URI inputUri) throws StorageEngineException {
        int fileId = getFileId();
        int studyId = getStudyId();
        List<Integer> fileIds = Collections.singletonList(fileId);

        VariantFileMetadata fileMetadata = readVariantFileMetadata(inputUri);
        VariantStudyMetadata metadata = fileMetadata.toVariantStudyMetadata(String.valueOf(studyId));
        int numRecords = fileMetadata.getStats().getNumVariants();
        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int loadThreads = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        final int numReaders = 1;
        boolean resume = isResume(options);
        StudyMetadata studyMetadata = getStudyMetadata();
        boolean stdin = options.getBoolean(STDIN.key(), STDIN.defaultValue());

        try {
            //Dedup task
            VariantDeduplicationTask duplicatedVariantsDetector =
                    new VariantDeduplicationTask(new DiscardDuplicatedVariantsResolver(fileId));

            //Remapping ids task
            org.opencb.commons.run.Task remapIdsTask = new RemapVariantIdsTask(studyMetadata.getId(), fileId);

            // File reader
            DataReader<Variant> variantReader = variantReaderUtils.getVariantReader(inputUri, metadata, stdin)
                    .then(duplicatedVariantsDetector)
                    .then(remapIdsTask);

            MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyId);
            MergeMode mergeMode = MergeMode.from(studyMetadata.getAttributes());
            boolean addAllStageDocuments = mergeMode.equals(MergeMode.ADVANCED);

            // Reader -- MongoDBVariantStageAndFileReader
            MongoDBVariantStageAndFileReader stageReader = new MongoDBVariantStageAndFileReader(
                    variantReader, stageCollection, studyId, fileId, addAllStageDocuments);


            //TaskMetadata -- MongoDBVariantMerger
            ProgressLogger progressLogger = new ProgressLogger("Write variants in VARIANTS collection:", numRecords, 200);

            int release = options.getInt(Options.RELEASE.key(), Options.RELEASE.defaultValue());
            boolean ignoreOverlapping = studyMetadata.getAttributes().getBoolean(MERGE_IGNORE_OVERLAPPING_VARIANTS.key(),
                    MERGE_IGNORE_OVERLAPPING_VARIANTS.defaultValue());

//            Map<String, Set<Integer>> chromosomeInLoadedFiles = getChromosomeInLoadedFiles();
            MongoDBVariantMerger variantMerger = new MongoDBVariantMerger(dbAdaptor, studyMetadata, fileIds,
                    resume, ignoreOverlapping, release);

            // Writer -- MongoDBVariantDirectLoader
            MongoDBVariantDirectLoader loader = new MongoDBVariantDirectLoader(dbAdaptor, studyMetadata, fileId, resume,
                    progressLogger);

            // Runner
            ParallelTaskRunner<Document, ?> ptr;
            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                    .setReadQueuePutTimeout(20 * 60)
                    .setNumTasks(loadThreads)
                    .setBatchSize(batchSize)
                    .setAbortOnFail(true).build();
            if (isDirectLoadParallelWrite(options)) {
                logger.info("Multi thread direct load... [{} readerThreads, {} writerThreads]", numReaders, loadThreads);
                ptr = new ParallelTaskRunner<>(stageReader, variantMerger.then(loader), null, config);
            } else {
                logger.info("Multi thread direct load... [{} readerThreads, {} tasks, {} writerThreads]", numReaders, loadThreads, 1);
                ptr = new ParallelTaskRunner<>(stageReader, variantMerger, loader, config);
            }

            // Run
            Thread hook = getMetadataManager().buildShutdownHook(DIRECT_LOAD.key(), studyId, fileId);
            try {
                Runtime.getRuntime().addShutdownHook(hook);
                ptr.run();
                getMetadataManager().atomicSetStatus(studyId, TaskMetadata.Status.DONE, DIRECT_LOAD.key(), fileIds);
            } finally {
                Runtime.getRuntime().removeShutdownHook(hook);
            }

            writeResult = loader.getResult();
            writeResult.setSkippedVariants(stageReader.getSkippedVariants());
            writeResult.setNonInsertedVariants(duplicatedVariantsDetector.getDiscardedVariants());
            loadStats.append("directLoad", true);
            loadStats.append("writeResult", writeResult);

            fileMetadata.setId(String.valueOf(fileId));
            dbAdaptor.getMetadataManager().updateVariantFileMetadata(String.valueOf(studyId), fileMetadata);
        } catch (ExecutionException e) {
            try {
                getMetadataManager().atomicSetStatus(studyId, TaskMetadata.Status.ERROR, DIRECT_LOAD.key(),
                        fileIds);
            } catch (Exception e2) {
                // Do not propagate this exception!
                logger.error("Error reporting direct load error!", e2);
            }

            throw new StorageEngineException("Error executing direct load", e);
        }
    }

    public void stage(URI input) throws StorageEngineException {
        final int fileId = getFileId();

        if (!options.getBoolean(STAGE.key(), false)) {
            // Do not stage!
            return;
        }


        VariantFileMetadata fileMetadata = readVariantFileMetadata(input);
        VariantStudyMetadata metadata = fileMetadata.toVariantStudyMetadata(String.valueOf(getStudyId()));
        int numRecords = fileMetadata.getStats().getNumVariants();
        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int bulkSize = options.getInt(BULK_SIZE.key(), batchSize);
        int loadThreads = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        final int numReaders = 1;
//        final int numTasks = loadThreads == 1 ? 1 : loadThreads - numReaders; //Subtract the reader thread
        boolean stdin = options.getBoolean(STDIN.key(), STDIN.defaultValue());


        try {
            StudyMetadata studyMetadata = getStudyMetadata();
            MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyMetadata.getId());

            //Reader
            VariantReader variantReader = variantReaderUtils.getVariantReader(input, metadata, stdin);

            //Remapping ids task
            org.opencb.commons.run.Task remapIdsTask = new RemapVariantIdsTask(studyMetadata.getId(), fileId);

            //Runner
            ProgressLogger progressLogger = new ProgressLogger("Write variants in STAGE collection:", numRecords, 200);
            MongoDBVariantStageConverterTask converterTask = new MongoDBVariantStageConverterTask(progressLogger);
            MongoDBVariantStageLoader stageLoader =
                    new MongoDBVariantStageLoader(stageCollection, studyMetadata.getId(), fileId,
                            isResumeStage(options));

            ParallelTaskRunner<Variant, ?> ptr;
            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                    .setReadQueuePutTimeout(20 * 60)
                    .setNumTasks(loadThreads)
                    .setBatchSize(batchSize)
                    .setAbortOnFail(true).build();
            if (isStageParallelWrite(options)) {
                logger.info("Multi thread stage load... [{} readerThreads, {} writerThreads]", numReaders, loadThreads);
                ptr = new ParallelTaskRunner<>(variantReader, remapIdsTask.then(converterTask).then(stageLoader), null, config);
            } else {
                logger.info("Multi thread stage load... [{} readerThreads, {} tasks, {} writerThreads]", numReaders, loadThreads, 1);
                ptr = new ParallelTaskRunner<>(variantReader, remapIdsTask.then(converterTask), stageLoader, config);
            }

            Thread hook = new Thread(() -> {
                try {
                    logger.error("Stage shutdown hook!");
                    stageError();
                } catch (StorageEngineException e) {
                    logger.error("Error at shutdown", e);
                    throw Throwables.propagate(e);
                }
            });
            try {
                Runtime.getRuntime().addShutdownHook(hook);
                ptr.run();
                stageSuccess(fileMetadata);
            } finally {
                Runtime.getRuntime().removeShutdownHook(hook);
            }

            long skippedVariants = converterTask.getSkippedVariants();
            stageLoader.getWriteResult().setSkippedVariants(skippedVariants);
            loadStats.append(MERGE.key(), false);
            loadStats.append("stageWriteResult", stageLoader.getWriteResult());
            options.put("skippedVariants", skippedVariants);
            logger.info("Stage Write result: {}", skippedVariants);
        } catch (ExecutionException | RuntimeException e) {
            try {
                stageError();
            } catch (Exception e2) {
                // Do not propagate this exception!
                logger.error("Error reporting stage error!", e2);
            }
            throw new StorageEngineException("Error while executing STAGE variants", e);
        }
    }

    /**
     * Check can stage this file.
     *
     * - The file is not staged
     * - The file is not being staged
     *
     */
    private TaskMetadata preStage(int fileId) throws StorageEngineException {
        VariantStorageMetadataManager scm = dbAdaptor.getMetadataManager();
        AtomicReference<TaskMetadata> operation = new AtomicReference<>();
        scm.updateStudyMetadata(getStudyId(), studyMetadata -> {
            operation.set(securePreStage(fileId, studyMetadata));
            return studyMetadata;
        });

        return operation.get();
    }

    private TaskMetadata securePreStage(int fileId, StudyMetadata studyMetadata) throws StorageEngineException {
        String fileName = getMetadataManager().getFileName(studyMetadata.getId(), fileId);

        TaskMetadata operation;
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        if (metadataManager.getFileMetadata(studyMetadata.getId(), fileId).isReady(STAGE.key())) {
            // Already staged!
            logger.info("File \"{}\" ({}) already staged!", fileName, fileId);

            operation = getMetadataManager().getTask(studyMetadata.getId(), STAGE.key(), Collections.singletonList(fileId));

            if (operation != null && !operation.currentStatus().equals(TaskMetadata.Status.READY)) {
                // There was an error writing the operation status. Restore to "READY"
                operation.addStatus(TaskMetadata.Status.READY);
            }
            options.put(STAGE.key(), false);
        } else {
            boolean resume = isResumeStage(options);
            operation = metadataManager.addRunningTask(
                    getStudyId(), STAGE.key(),
                    Collections.singletonList(fileId),
                    resume,
                    TaskMetadata.Type.OTHER,
                    batchFileOperation -> batchFileOperation.getName().equals(STAGE.key()));

            // If there is more than one status is because we are resuming the operation.
            if (operation.getStatus().size() != 1) {
                options.put(STAGE_RESUME.key(), true);
            }
            options.put(STAGE.key(), true);
        }
        currentTask = operation;
        return operation;
    }

    public void stageError() throws StorageEngineException {
        int fileId = getFileId();
        getMetadataManager()
                .atomicSetStatus(getStudyId(), TaskMetadata.Status.ERROR, STAGE.key(), Collections.singletonList(fileId));
    }

    public void stageSuccess(VariantFileMetadata metadata) throws StorageEngineException {
        // Stage loading finished. Save VariantSource and update BatchOperation
        int fileId = getFileId();
        metadata.setId(String.valueOf(fileId));

        getMetadataManager().updateFileMetadata(getStudyId(), fileId, file -> file.setStatus(STAGE.key(), TaskMetadata.Status.READY));
        getMetadataManager()
                .setStatus(getStudyId(), currentTask.getId(), TaskMetadata.Status.READY);
        metadata.setId(String.valueOf(fileId));
        dbAdaptor.getMetadataManager().updateVariantFileMetadata(String.valueOf(getStudyId()), metadata);

    }

    /**
     * Merge staged files into Variant collection.
     *
     * @param fileIds           FileIDs of the files to be merged
     * @return                  Write Result with times and count
     * @throws StorageEngineException  If there is a problem executing the {@link ParallelTaskRunner}
     */
    public MongoDBVariantWriteResult merge(List<Integer> fileIds) throws StorageEngineException {
        return merge(fileIds, options.getInt("skippedVariants", 0));
    }

    /**
     * Merge staged files into Variant collection.
     *
     * 1- Find if the files are in different chromosomes.
     * 2- If splitted, call once per chromosome. Else, call only once.
     *
     * @see MongoDBVariantMerger
     *
     * @param fileIds           FileIDs of the files to be merged
     * @param skippedVariants   Number of skipped variants into the Stage
     * @return                  Write Result with times and count
     * @throws StorageEngineException  If there is a problem executing the {@link ParallelTaskRunner}
     */
    protected MongoDBVariantWriteResult merge(List<Integer> fileIds, long skippedVariants)
            throws StorageEngineException {

        long start = System.currentTimeMillis();
        this.fileIds = fileIds;

        StudyMetadata studyMetadata = preMerge(fileIds);

        //Stage collection where files are loaded.
        MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyMetadata.getId());

        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int loadThreads = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        int capacity = options.getInt("blockingQueueCapacity", loadThreads * 2);

        if (options.getBoolean(MERGE_SKIP.key())) {
            // It was already merged, but still some work is needed. Exit to do postLoad step
            writeResult = new MongoDBVariantWriteResult();
        } else {
            Thread hook = new Thread(() -> {
                try {
                    logger.error("Merge shutdown hook!");
                    getMetadataManager().atomicSetStatus(getStudyId(), TaskMetadata.Status.ERROR, MERGE.key(), fileIds);
                } catch (Exception e) {
                    logger.error("Failed setting status '" + MERGE.key() + "' operation over files " + fileIds
                            + " to '" + TaskMetadata.Status.ERROR + '\'', e);
                    throw Throwables.propagate(e);
                }
            });
            Runtime.getRuntime().addShutdownHook(hook);
            try {
                writeResult = mergeByChromosome(fileIds, batchSize, loadThreads, studyMetadata);
            } catch (Exception e) {
                getMetadataManager().atomicSetStatus(getStudyId(), TaskMetadata.Status.ERROR, MERGE.key(), fileIds);
                throw e;
            } finally {
                Runtime.getRuntime().removeShutdownHook(hook);
            }
            getMetadataManager().atomicSetStatus(getStudyId(), TaskMetadata.Status.DONE, MERGE.key(), fileIds);
        }

        if (!options.getBoolean(STAGE_CLEAN_WHILE_LOAD.key(), STAGE_CLEAN_WHILE_LOAD.defaultValue())) {
            StopWatch time = StopWatch.createStarted();
            logger.info("Deleting variant records from Stage collection");
            long modifiedCount = MongoDBVariantStageLoader.cleanStageCollection(stageCollection, studyMetadata.getId(), fileIds,
                    null, writeResult);
            logger.info("Delete variants time: " + time.getTime(TimeUnit.MILLISECONDS) / 1000.0 + "s , CleanDocuments: " + modifiedCount);
        }

        writeResult.setSkippedVariants(skippedVariants);

        logger.info("Write result: {}", writeResult.toString());
//        logger.info("Write result: {}", writeResult.toTSV());
        logger.info("Write result: {}", writeResult.toJson());
        loadStats.append(MERGE.key(), true);
        loadStats.append("mergeWriteResult", writeResult);

        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants merged!");
        return writeResult;
    }

    private StudyMetadata preMerge(List<Integer> fileIds) throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        return metadataManager.updateStudyMetadata(getStudyId(), studyMetadata -> {
            studyMetadata = ensureStudyMetadataExists(studyMetadata);
            LinkedHashSet<Integer> indexedFiles = getMetadataManager().getIndexedFiles(studyMetadata.getId());
            for (Integer fileId : fileIds) {
                if (indexedFiles.contains(fileId)) {
                    throw StorageEngineException.alreadyLoaded(fileId, metadataManager.getFileName(getStudyId(), fileId));
                }
            }
            boolean resume = isResumeMerge(options);
            currentTask = getMetadataManager()
                    .addRunningTask(studyMetadata.getId(), MERGE.key(), fileIds, resume, TaskMetadata.Type.LOAD);

            if (currentTask.currentStatus().equals(TaskMetadata.Status.DONE)) {
                options.put(MERGE_SKIP.key(), true);
            }

            return studyMetadata;
        });
    }

    private MongoDBVariantWriteResult mergeByChromosome(List<Integer> fileIds, int batchSize, int loadThreads,
                                                        StudyMetadata studyMetadata)
            throws StorageEngineException {
        MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyMetadata.getId());
        MongoDBVariantStageReader reader = new MongoDBVariantStageReader(stageCollection, studyMetadata.getId());
        MergeMode mergeMode = MergeMode.from(studyMetadata.getAttributes());
        if (mergeMode.equals(MergeMode.BASIC)) {
            // Read only files to load when MergeMode is BASIC
            reader.setFileIds(fileIds);
        }
        boolean resume = isResumeMerge(options);
        boolean cleanWhileLoading = options.getBoolean(STAGE_CLEAN_WHILE_LOAD.key(), STAGE_CLEAN_WHILE_LOAD.defaultValue());
        ProgressLogger progressLogger = new ProgressLogger("Write variants in VARIANTS collection:", reader::countNumVariants, 200);
        progressLogger.setApproximateTotalCount(reader.countAproxNumVariants());

        boolean ignoreOverlapping = studyMetadata.getAttributes().getBoolean(MERGE_IGNORE_OVERLAPPING_VARIANTS.key(),
                MERGE_IGNORE_OVERLAPPING_VARIANTS.defaultValue());
        int release = options.getInt(Options.RELEASE.key(), Options.RELEASE.defaultValue());
        MongoDBVariantMerger variantMerger = new MongoDBVariantMerger(dbAdaptor, studyMetadata, fileIds, resume,
                ignoreOverlapping, release);
        MongoDBVariantMergeLoader variantLoader = new MongoDBVariantMergeLoader(
                dbAdaptor.getVariantsCollection(), stageCollection, dbAdaptor.getStudiesCollection(),
                studyMetadata, fileIds, resume, cleanWhileLoading, progressLogger);

        ParallelTaskRunner<Document, MongoDBOperations> ptrMerge;
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setReadQueuePutTimeout(20 * 60)
                .setNumTasks(loadThreads)
                .setBatchSize(batchSize)
                .setAbortOnFail(true).build();
        try {
            if (isMergeParallelWrite(options)) {
                ptrMerge = new ParallelTaskRunner<>(reader, variantMerger.then(variantLoader), null, config);
            } else {
                ptrMerge = new ParallelTaskRunner<>(reader, variantMerger, variantLoader, config);
            }
        } catch (RuntimeException e) {
            throw new StorageEngineException("Error while creating ParallelTaskRunner", e);
        }

        try {
            logger.info("Merging files " + fileIds);
            ptrMerge.run();
        } catch (ExecutionException e) {
            logger.info("Write result: {}", variantLoader.getResult());
            throw new StorageEngineException("Error while executing LoadVariants in ParallelTaskRunner", e);
        }
        return variantLoader.getResult();
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageEngineException {

        if (options.getBoolean(MERGE.key()) || options.getBoolean(DIRECT_LOAD.key(), DIRECT_LOAD.defaultValue())) {
            return postLoad(input, output, fileIds);
        } else {
            return input;
        }
    }

    @Override
    protected void securePostLoad(List<Integer> fileIds, StudyMetadata studyMetadata) throws StorageEngineException {
        super.securePostLoad(fileIds, studyMetadata);
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        TaskMetadata.Status status = metadataManager.setStatus(studyMetadata.getId(), currentTask.getId(), TaskMetadata.Status.READY);
        if (status != TaskMetadata.Status.DONE) {
            logger.warn("Unexpected status " + status);
        }
        Set<String> genotypes = new HashSet<>(studyMetadata.getAttributes().getAsStringList(LOADED_GENOTYPES.key()));
        genotypes.addAll(writeResult.getGenotypes());
        studyMetadata.getAttributes().put(LOADED_GENOTYPES.key(), genotypes);
    }

    @Override
    public ObjectMap getLoadStats() {
        return loadStats;
    }

    @Override
    protected void checkLoadedVariants(List<Integer> fileIds, StudyMetadata studyMetadata)
            throws StorageEngineException {
        if (fileIds.size() == 1) {
            checkLoadedVariants(fileIds.get(0), studyMetadata);
        } else {
            // FIXME: Check variants in this situation!
            logger.warn("Skip check loaded variants");
        }
    }

    @Override
    protected void checkLoadedVariants(int fileId, StudyMetadata studyMetadata) throws
            StorageEngineException {

        if (getOptions().getBoolean(POST_LOAD_CHECK_SKIP.key(), POST_LOAD_CHECK_SKIP.defaultValue())) {
            logger.warn("Skip check loaded variants");
            return;
        }

        VariantFileMetadata fileMetadata = getMetadataManager().getVariantFileMetadata(getStudyId(), fileId, null).first();

        Long count = dbAdaptor.count(new Query()
                .append(VariantQueryParam.FILE.key(), fileId)
                .append(VariantQueryParam.STUDY.key(), studyMetadata.getId())).first();
        Long overlappedCount = dbAdaptor.count(new Query()
                .append(VariantQueryParam.FILE.key(), fileId)
                .append(OVERLAPPED_FILES_ONLY, true)
                .append(VariantQueryParam.STUDY.key(), studyMetadata.getId())).first();
        long variantsToLoad = 0;

        long expectedSkippedVariants = 0;
        long alreadyLoadedVariants = options.getLong(ALREADY_LOADED_VARIANTS.key(), 0L);

        for (Map.Entry<String, Integer> entry : fileMetadata.getStats().getVariantTypeCounts().entrySet()) {
            if (SKIPPED_VARIANTS.contains(VariantType.valueOf(entry.getKey()))) {
                expectedSkippedVariants += entry.getValue();
            } else {
                variantsToLoad += entry.getValue();
            }
        }
        long expectedCount = variantsToLoad;
        if (alreadyLoadedVariants != 0) {
            writeResult.setNonInsertedVariants(writeResult.getNonInsertedVariants() - alreadyLoadedVariants);
        }
        if (writeResult.getNonInsertedVariants() != 0) {
            expectedCount -= writeResult.getNonInsertedVariants();
        }
        if (writeResult.getOverlappedVariants() != 0) {
            // Expect to find this file in all the overlapped variants
            expectedCount += writeResult.getOverlappedVariants();
        }

        String fileName = getMetadataManager().getFileMetadata(studyMetadata.getId(), fileId).getName();
        logger.info("============================================================");
        logger.info("Check loaded file '" + fileName + "' (" + fileId + ')');
        if (expectedSkippedVariants != writeResult.getSkippedVariants()) {
            logger.error("Wrong number of skipped variants. Expected " + expectedSkippedVariants + " and got " + writeResult
                    .getSkippedVariants());
        } else if (writeResult.getSkippedVariants() > 0) {
            logger.warn("There were " + writeResult.getSkippedVariants() + " skipped variants.");
            for (VariantType type : SKIPPED_VARIANTS) {
                Integer countByType = fileMetadata.getStats().getVariantTypeCounts().get(type.toString());
                if (countByType != null && countByType > 0) {
                    logger.info("  * Of which " + countByType + " are " + type.toString() + " variants.");
                }
            }
        }

        if (writeResult.getNonInsertedVariants() != 0) {
            logger.error("There were " + writeResult.getNonInsertedVariants() + " duplicated variants not inserted. ");
        }

        if (alreadyLoadedVariants != 0) {
            logger.info("Resume mode. Previously loaded variants: " + alreadyLoadedVariants);
        }

        StorageEngineException exception = null;
        if (expectedCount != (count + overlappedCount)) {
            String message = "Wrong number of loaded variants. Expected: " + expectedCount + " and got: " + (count + overlappedCount)
                    + " (" + count + " from file, " + overlappedCount + " overlapped)";
            logger.error(message);
            logger.error("  * Variants to load : " + variantsToLoad);
            logger.error("  * Non Inserted (due to duplications) : " + writeResult.getNonInsertedVariants());
            logger.error("  * Overlapped variants (extra insertions) : " + writeResult.getOverlappedVariants());
//            exception = new StorageEngineException(message);
        } else {
            logger.info("Final number of loaded variants: " + count
                    + (overlappedCount > 0 ? " + " + overlappedCount + " overlapped variants" : ""));
        }
        logger.info("============================================================");
        if (exception != null) {
            throw exception;
        }
    }


    /* --------------------------------------- */
    /*  StudyMetadata utils methods       */
    /* --------------------------------------- */


    /**
     * Check if the samples from the selected file can be loaded.
     * <p>
     * MongoDB storage plugin is not able to load batches of samples in a unordered way.
     * A batch of samples is a group of samples of any size. It may be composed of one or several VCF files, depending
     * on whether it is split by region (horizontally) or not.
     * All the files from the same batch must be loaded, before loading the next batch. If a new batch of
     * samples begins to be loaded, it won't be possible to load other files from previous batches
     * <p>
     * The StudyMetadata must be complete, with all the indexed files, and samples in files.
     * Provided StudyMetadata won't be modified
     * Requirements:
     * - All samples in file must be or loaded or not loaded
     * - If all samples loaded, must match (same order and samples) with the last loaded file.
     *
     *
     * @param metadataManager    MetadataManager
     * @param studyMetadata      StudyMetadata from the selected study
     * @param fileId             File to load
     * @param loadSplitData      Allow load split data
     * @return Returns if this file represents a new batch of samples
     * @throws StorageEngineException If there is any unaccomplished requirement
     */
    public static boolean checkCanLoadSampleBatch(
            VariantStorageMetadataManager metadataManager, final StudyMetadata studyMetadata, int fileId, boolean loadSplitData)
            throws StorageEngineException {
        FileMetadata fileMetadata = metadataManager.getFileMetadata(studyMetadata.getId(), fileId);
        LinkedHashSet<Integer> sampleIds = fileMetadata.getSamples();
        if (!sampleIds.isEmpty()) {
            boolean allSamplesRepeated = true;
            boolean someSamplesRepeated = false;

            BiMap<String, Integer> indexedSamples = metadataManager.getIndexedSamplesMap(studyMetadata.getId());
            for (Integer sampleId : sampleIds) {
                if (!indexedSamples.containsValue(sampleId)) {
                    allSamplesRepeated = false;
                } else {
                    someSamplesRepeated = true;
                }
            }

            if (allSamplesRepeated) {
                if (loadSplitData) {
                    Logger logger = LoggerFactory.getLogger(MongoDBVariantStoragePipeline.class);
                    if (sampleIds.size() > 100) {
                        logger.info("About to load split data for samples in file " + fileMetadata.getName());
                    } else {
                        String samples = sampleIds
                                .stream()
                                .map(s -> metadataManager.getSampleName(studyMetadata.getId(), s))
                                .collect(Collectors.joining(",", "[", "]"));
                        logger.info("About to load split data for samples " + samples);
                    }
                } else {
                    List<String> sampleNames = sampleIds
                            .stream()
                            .map(s -> metadataManager.getSampleName(studyMetadata.getId(), s))
                            .collect(Collectors.toList());
                    throw MongoVariantStorageEngineException.alreadyLoadedSamples(fileMetadata.getName(), sampleNames);
                }
                return false;
            } else if (someSamplesRepeated) {
                throw MongoVariantStorageEngineException.alreadyLoadedSomeSamples(fileMetadata.getName());
            }
        }
        return true; // This is a new batch of samples
    }

    /**
     * Check if the file can be loaded using direct load.
     *
     * First loaded file in study:
     *   There is no other indexed file
     *   There is no staged file
     * First loaded file in region:
     *   --load-split-data is provided
     *   There are some loaded file
     *   All loaded files, and the file to load, have the same samples
     *
     * @param input File to load
     * @return  If the file can be loaded using direct load
     * @throws StorageEngineException is there is a problem reading metadata
     */
    public boolean checkCanLoadDirectly(List<URI> input) throws StorageEngineException {
        boolean doDirectLoad;

        if (input.size() > 1) {
            // Direct load can be done, but for more than one file we choose by default not to do it.
            // Let's read whatever is in the configuration file
            return getOptions().getBoolean(DIRECT_LOAD.key(), false);
        }

        // Direct load can be avoided from outside, but can not be forced.
        if (!getOptions().getBoolean(DIRECT_LOAD.key(), true)) {
            doDirectLoad = false;
        } else {

            if (!getOptions().getBoolean(STAGE.key(), false) && !getOptions().getBoolean(MERGE.key(), false)) {
                doDirectLoad = true;
            } else {
                doDirectLoad = false;
            }

//            StudyMetadata studyConfiguration = getStudyMetadata();
//
//            // Direct load if loading one file, and there were no other indexed file in the study.
//            if ((studyConfiguration == null || studyConfiguration.getIndexedFiles().isEmpty())) {
//                doDirectLoad = true;
//            } else if (getOptions().getBoolean(LOAD_SPLIT_DATA.key(), LOAD_SPLIT_DATA.defaultValue())) {
//                LinkedHashSet<Integer> sampleIds = readVariantFileMetadata(input).getSampleIds().stream()
//                        .map(studyConfiguration.getSampleIds()::get).collect(Collectors.toCollection(LinkedHashSet::new));
//                doDirectLoad = true;
//                for (Integer fileId : studyConfiguration.getIndexedFiles()) {
//                    if (!sampleIds.equals(studyConfiguration.getSamplesInFiles().get(fileId))) {
//                        doDirectLoad = false;
//                        break;
//                    }
//                }
//            } else {
//                doDirectLoad = false;
//            }
        }
        return doDirectLoad;
    }

}
