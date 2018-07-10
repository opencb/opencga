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
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.metadata.SampleSetType;
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
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.metadata.adaptors.VariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.MergeMode;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.transform.RemapVariantIdsTask;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.exceptions.MongoVariantStorageEngineException;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantWriteResult;
import org.opencb.opencga.storage.mongodb.variant.load.direct.MongoDBVariantDirectConverter;
import org.opencb.opencga.storage.mongodb.variant.load.direct.MongoDBVariantDirectLoader;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.metadata.StudyConfigurationManager.addBatchOperation;
import static org.opencb.opencga.storage.core.metadata.StudyConfigurationManager.setStatus;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.LOAD_SPLIT_DATA;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.POST_LOAD_CHECK_SKIP;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.*;

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

    public MongoDBVariantStoragePipeline(StorageConfiguration configuration, String storageEngineId,
                                         VariantMongoDBAdaptor dbAdaptor) {
        super(configuration, storageEngineId, dbAdaptor,
                new VariantReaderUtils());
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
    protected void securePreLoad(StudyConfiguration studyConfiguration, VariantFileMetadata source) throws StorageEngineException {
        super.securePreLoad(studyConfiguration, source);
        int fileId = getFileId();

        if (studyConfiguration.getAttributes().containsKey(Options.MERGE_MODE.key())
                || studyConfiguration.getAttributes().containsKey(MERGE_IGNORE_OVERLAPPING_VARIANTS.key())) {
            if (studyConfiguration.getAttributes().getBoolean(MERGE_IGNORE_OVERLAPPING_VARIANTS.key())) {
                studyConfiguration.getAttributes().put(Options.MERGE_MODE.key(), MergeMode.BASIC);
                logger.debug("Do not merge overlapping variants, as said in the StudyConfiguration");
            } else {
                studyConfiguration.getAttributes().put(Options.MERGE_MODE.key(), MergeMode.ADVANCED);
                logger.debug("Merge overlapping variants, as said in the StudyConfiguration");
            }
            options.put(Options.MERGE_MODE.key(), studyConfiguration.getAttributes().get(Options.MERGE_MODE.key()));
        } else {
            MergeMode mergeMode = MergeMode.from(options);
            studyConfiguration.getAttributes().put(Options.MERGE_MODE.key(), mergeMode);
            switch (mergeMode) {
                case BASIC:
                    studyConfiguration.getAttributes().put(MERGE_IGNORE_OVERLAPPING_VARIANTS.key(), true);
                    // When ignoring overlapping variants, the default genotype MUST be the UNKNOWN_GENOTYPE (?/?).
                    // Otherwise, a "fillGaps" step will be needed afterwards
                    studyConfiguration.getAttributes().put(DEFAULT_GENOTYPE.key(), GenotypeClass.UNKNOWN_GENOTYPE);
                    break;
                case ADVANCED:
                    studyConfiguration.getAttributes().put(MERGE_IGNORE_OVERLAPPING_VARIANTS.key(), false);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown merge mode: " + mergeMode);
            }
        }
        if (studyConfiguration.getAttributes().containsKey(DEFAULT_GENOTYPE.key())) {
            Set<String> defaultGenotype = new HashSet<>(studyConfiguration.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key()));
            logger.debug("Using default genotype from study configuration: {}", defaultGenotype);
        } else {
            Set<String> defaultGenotype;
            if (options.containsKey(DEFAULT_GENOTYPE.key())) {
                defaultGenotype = new HashSet<>(options.getAsStringList(DEFAULT_GENOTYPE.key()));
            } else {
                SampleSetType studyType = options.get(Options.STUDY_TYPE.key(), SampleSetType.class, Options.STUDY_TYPE
                        .defaultValue());
                switch (studyType) {
                    case FAMILY:
                    case TRIO:
                    case PAIRED:
                        defaultGenotype = Collections.singleton(GenotypeClass.UNKNOWN_GENOTYPE);
                        logger.debug("Do not compress genotypes. Default genotype : {}", defaultGenotype);
                        break;
                    default:
                        defaultGenotype = new HashSet<>(DEFAULT_GENOTYPE.defaultValue());
                        logger.debug("No default genotype found. Using default genotype: {}", defaultGenotype);
                        break;
                }
            }
            studyConfiguration.getAttributes().put(DEFAULT_GENOTYPE.key(), defaultGenotype);
        }

        boolean loadSplitData = options.getBoolean(Options.LOAD_SPLIT_DATA.key(), Options.LOAD_SPLIT_DATA.defaultValue());
        boolean newSampleBatch = checkCanLoadSampleBatch(studyConfiguration, fileId, loadSplitData);

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
            BatchFileOperation operation = addBatchOperation(studyConfiguration, DIRECT_LOAD.key(), Collections.singletonList(fileId),
                    isResume(options), BatchFileOperation.Type.LOAD);
            if (operation.getStatus().size() > 1) {
                options.put(Options.RESUME.key(), true);
                options.put(STAGE_RESUME.key(), true);
                options.put(MERGE_RESUME.key(), true);
            }
        } else {
            securePreStage(fileId, studyConfiguration);
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
        StudyConfiguration studyConfiguration = getStudyConfiguration();

        try {
            //Reader
            DataReader<Variant> variantReader;
            VariantDeduplicationTask duplicatedVariantsDetector = new VariantDeduplicationTask(list -> {
                if (list.size() > 1) {
                    logger.warn("Found {} duplicated variants for file {} in variant {}.", list.size(), fileId, list.get(0));
                    return Collections.emptyList();
                } else {
                    throw new IllegalStateException("Unexpected list of " + list.size() + " duplicated variants : " + list);
                }
            });
            variantReader = VariantReaderUtils.getVariantReader(Paths.get(inputUri), metadata).then(duplicatedVariantsDetector);

            //Remapping ids task
            Task<Variant, Variant> remapIdsTask = new RemapVariantIdsTask(studyConfiguration, fileId);

            //Runner
            ProgressLogger progressLogger = new ProgressLogger("Write variants in VARIANTS collection:", numRecords, 200);
            int release = options.getInt(Options.RELEASE.key(), Options.RELEASE.defaultValue());
            MongoDBVariantDirectConverter converter = new MongoDBVariantDirectConverter(dbAdaptor, getStudyConfiguration(), fileId,
                    isResume(options), release, progressLogger);
            MongoDBVariantDirectLoader loader = new MongoDBVariantDirectLoader(dbAdaptor, getStudyConfiguration(), fileId,
                    isResume(options));

            ParallelTaskRunner<Variant, ?> ptr;
            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                    .setReadQueuePutTimeout(20 * 60)
                    .setNumTasks(loadThreads)
                    .setBatchSize(batchSize)
                    .setAbortOnFail(true).build();
            if (isDirectLoadParallelWrite(options)) {
                logger.info("Multi thread direct load... [{} readerThreads, {} writerThreads]", numReaders, loadThreads);
                ptr = new ParallelTaskRunner<>(variantReader, remapIdsTask.then(converter).then(loader), null, config);
            } else {
                logger.info("Multi thread direct load... [{} readerThreads, {} tasks, {} writerThreads]", numReaders, loadThreads, 1);
                ptr = new ParallelTaskRunner<>(variantReader, remapIdsTask.then(converter), loader, config);
            }

            Thread hook = getStudyConfigurationManager().buildShutdownHook(DIRECT_LOAD.key(), studyId, fileId);
            try {
                Runtime.getRuntime().addShutdownHook(hook);
                ptr.run();
                getStudyConfigurationManager().atomicSetStatus(studyId, BatchFileOperation.Status.DONE, DIRECT_LOAD.key(), fileIds);
            } finally {
                Runtime.getRuntime().removeShutdownHook(hook);
            }

            writeResult = loader.getResult();
            writeResult.setSkippedVariants(converter.getSkippedVariants());
            writeResult.setNonInsertedVariants(duplicatedVariantsDetector.getDiscardedVariants());
            loadStats.append("directLoad", true);
            loadStats.append("writeResult", writeResult);

            fileMetadata.setId(String.valueOf(fileId));
            dbAdaptor.getStudyConfigurationManager().updateVariantFileMetadata(String.valueOf(studyId), fileMetadata);
        } catch (ExecutionException e) {
            try {
                getStudyConfigurationManager().atomicSetStatus(studyId, BatchFileOperation.Status.ERROR, DIRECT_LOAD.key(),
                        fileIds);
            } catch (Exception e2) {
                // Do not propagate this exception!
                logger.error("Error reporting direct load error!", e2);
            }

            throw new StorageEngineException("Error executing direct load", e);
        }
    }

    public void stage(URI inputUri) throws StorageEngineException {
        final int fileId = getFileId();

        if (!options.getBoolean(STAGE.key(), false)) {
            // Do not stage!
            return;
        }

        Path input = Paths.get(inputUri.getPath());

        VariantFileMetadata fileMetadata = readVariantFileMetadata(inputUri);
        VariantStudyMetadata metadata = fileMetadata.toVariantStudyMetadata(String.valueOf(getStudyId()));
        int numRecords = fileMetadata.getStats().getNumVariants();
        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int bulkSize = options.getInt(BULK_SIZE.key(), batchSize);
        int loadThreads = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        final int numReaders = 1;
//        final int numTasks = loadThreads == 1 ? 1 : loadThreads - numReaders; //Subtract the reader thread


        try {
            StudyConfiguration studyConfiguration = getStudyConfiguration();
            MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyConfiguration.getStudyId());

            //Reader
            VariantReader variantReader;
            variantReader = VariantReaderUtils.getVariantReader(input, metadata);

            //Remapping ids task
            Task<Variant, Variant> remapIdsTask = new RemapVariantIdsTask(studyConfiguration, fileId);

            //Runner
            ProgressLogger progressLogger = new ProgressLogger("Write variants in STAGE collection:", numRecords, 200);
            MongoDBVariantStageConverterTask converterTask = new MongoDBVariantStageConverterTask(progressLogger);
            MongoDBVariantStageLoader stageLoader =
                    new MongoDBVariantStageLoader(stageCollection, studyConfiguration.getStudyId(), fileId,
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
    private BatchFileOperation preStage(int fileId) throws StorageEngineException {

        StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();
        AtomicReference<BatchFileOperation> operation = new AtomicReference<>();
        scm.lockAndUpdate(getStudyId(), studyConfiguration -> {
            operation.set(securePreStage(fileId, studyConfiguration));
            return studyConfiguration;
        });

        return operation.get();
    }

    private BatchFileOperation securePreStage(int fileId, StudyConfiguration studyConfiguration) throws StorageEngineException {
        String fileName = studyConfiguration.getFileIds().inverse().get(fileId);

        Query query = new Query()
                .append(VariantFileMetadataDBAdaptor.VariantFileMetadataQueryParam.STUDY_ID.key(), studyConfiguration.getStudyId())
                .append(VariantFileMetadataDBAdaptor.VariantFileMetadataQueryParam.FILE_ID.key(), fileId);

        BatchFileOperation operation;
        if (dbAdaptor.getStudyConfigurationManager().countVariantFileMetadata(query).first() == 1) {
            // Already staged!
            logger.info("File \"{}\" ({}) already staged!", fileName, fileId);

            operation = StudyConfigurationManager.getOperation(studyConfiguration, STAGE.key(), Collections.singletonList(fileId));

            if (operation != null && !operation.currentStatus().equals(BatchFileOperation.Status.READY)) {
                // There was an error writing the operation status. Restore to "READY"
                operation.addStatus(BatchFileOperation.Status.READY);
            }
            options.put(STAGE.key(), false);
        } else {
            boolean resume = isResumeStage(options);
            operation = StudyConfigurationManager.addBatchOperation(
                    studyConfiguration, STAGE.key(),
                    Collections.singletonList(fileId),
                    resume,
                    BatchFileOperation.Type.OTHER,
                    batchFileOperation -> batchFileOperation.getOperationName().equals(STAGE.key()));

            // If there is more than one status is because we are resuming the operation.
            if (operation.getStatus().size() != 1) {
                options.put(STAGE_RESUME.key(), true);
            }
            options.put(STAGE.key(), true);
        }

        return operation;
    }

    public void stageError() throws StorageEngineException {
        int fileId = getFileId();
        getStudyConfigurationManager()
                .atomicSetStatus(getStudyId(), BatchFileOperation.Status.ERROR, STAGE.key(), Collections.singletonList(fileId));
    }

    public void stageSuccess(VariantFileMetadata metadata) throws StorageEngineException {
        // Stage loading finished. Save VariantSource and update BatchOperation
        int fileId = getFileId();
        metadata.setId(String.valueOf(fileId));

        getStudyConfigurationManager()
                .atomicSetStatus(getStudyId(), BatchFileOperation.Status.READY, STAGE.key(), Collections.singletonList(fileId));
        metadata.setId(String.valueOf(fileId));
        dbAdaptor.getStudyConfigurationManager().updateVariantFileMetadata(String.valueOf(getStudyId()), metadata);

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

        StudyConfiguration studyConfiguration = preMerge(fileIds);

        //Stage collection where files are loaded.
        MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyConfiguration.getStudyId());

        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int loadThreads = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        int capacity = options.getInt("blockingQueueCapacity", loadThreads * 2);

        //Iterate over all the files
        Query query = new Query(VariantFileMetadataDBAdaptor.VariantFileMetadataQueryParam.STUDY_ID.key(), studyConfiguration.getStudyId());
        Iterator<VariantFileMetadata> iterator = dbAdaptor.getStudyConfigurationManager().variantFileMetadataIterator(query, null);

        // List of chromosomes to be loaded
        TreeSet<String> chromosomesToLoad = new TreeSet<>((s1, s2) -> {
            try {
                return Integer.valueOf(s1).compareTo(Integer.valueOf(s2));
            } catch (NumberFormatException e) {
                return s1.compareTo(s2);
            }
        });
        // List of all the indexed files that cover each chromosome
        ListMultimap<String, Integer> chromosomeInLoadedFiles = LinkedListMultimap.create();
        // List of all the indexed files that cover each chromosome
        ListMultimap<String, Integer> chromosomeInFilesToLoad = LinkedListMultimap.create();

        Set<String> wholeGenomeFiles = new HashSet<>();
        Set<String> byChromosomeFiles = new HashSet<>();

        boolean loadUnknownGenotypes = MongoDBVariantMerger.loadUnknownGenotypes(studyConfiguration);
        // Loading split files is only a problem when loading unknown genotypes
        // If so, load files per chromosome.
        if (loadUnknownGenotypes) {
            while (iterator.hasNext()) {
                VariantFileMetadata fileMetadata = iterator.next();
                int fileId = Integer.parseInt(fileMetadata.getId());

                // If the file is going to be loaded, check if covers just one chromosome
                if (fileIds.contains(fileId)) {
                    if (fileMetadata.getStats().getChromosomeCounts().size() == 1) {
                        chromosomesToLoad.addAll(fileMetadata.getStats().getChromosomeCounts().keySet());
                        byChromosomeFiles.add(fileMetadata.getPath());
                    } else {
                        wholeGenomeFiles.add(fileMetadata.getPath());
                    }
                }
                // If the file is indexed, add to the map of chromosome->fileId
                for (String chromosome : fileMetadata.getStats().getChromosomeCounts().keySet()) {
                    if (studyConfiguration.getIndexedFiles().contains(fileId)) {
                        chromosomeInLoadedFiles.put(chromosome, fileId);
                    } else if (fileIds.contains(fileId)) {
                        chromosomeInFilesToLoad.put(chromosome, fileId);
                    } // else { ignore files that are not loaded, and are not going to be loaded }
                }
            }
        }

        if (options.getBoolean(MERGE_SKIP.key())) {
            // It was already merged, but still some work is needed. Exit to do postLoad step
            writeResult = new MongoDBVariantWriteResult();
        } else {
            Thread hook = new Thread(() -> {
                try {
                    logger.error("Merge shutdown hook!");
                    getStudyConfigurationManager().atomicSetStatus(getStudyId(), BatchFileOperation.Status.ERROR, MERGE.key(), fileIds);
                } catch (Exception e) {
                    logger.error("Failed setting status '" + MERGE.key() + "' operation over files " + fileIds
                            + " to '" + BatchFileOperation.Status.ERROR + '\'', e);
                    throw Throwables.propagate(e);
                }
            });
            Runtime.getRuntime().addShutdownHook(hook);
            try {
                // This scenario only matters when adding unknownGenotypes
                if (loadUnknownGenotypes && !wholeGenomeFiles.isEmpty() && !byChromosomeFiles.isEmpty()) {
                    String message = "Impossible to merge files split and not split by chromosome at the same time! "
                            + "Files covering only one chromosome: " + byChromosomeFiles + ". "
                            + "Files covering more than one chromosome: " + wholeGenomeFiles;
                    logger.error(message);
                    throw new StorageEngineException(message);
                }

                if (chromosomesToLoad.isEmpty()) {
                    writeResult = mergeByChromosome(fileIds, batchSize, loadThreads,
                            studyConfiguration, null, studyConfiguration.getIndexedFiles());
                } else {
                    writeResult = new MongoDBVariantWriteResult();
                    for (String chromosome : chromosomesToLoad) {
                        List<Integer> filesToLoad = chromosomeInFilesToLoad.get(chromosome);
                        Set<Integer> indexedFiles = new HashSet<>(chromosomeInLoadedFiles.get(chromosome));
                        MongoDBVariantWriteResult aux = mergeByChromosome(filesToLoad, batchSize, loadThreads,
                                studyConfiguration, chromosome, indexedFiles);
                        writeResult.merge(aux);
                    }
                }
            } catch (Exception e) {
                getStudyConfigurationManager().atomicSetStatus(getStudyId(), BatchFileOperation.Status.ERROR, MERGE.key(), fileIds);
                throw e;
            } finally {
                Runtime.getRuntime().removeShutdownHook(hook);
            }
            getStudyConfigurationManager().atomicSetStatus(getStudyId(), BatchFileOperation.Status.DONE, MERGE.key(), fileIds);
        }

        if (!options.getBoolean(STAGE_CLEAN_WHILE_LOAD.key(), STAGE_CLEAN_WHILE_LOAD.defaultValue())) {
            StopWatch time = StopWatch.createStarted();
            logger.info("Deleting variant records from Stage collection");
            long modifiedCount = MongoDBVariantStageLoader.cleanStageCollection(stageCollection, studyConfiguration.getStudyId(), fileIds,
                    chromosomesToLoad, writeResult);
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

    private StudyConfiguration preMerge(List<Integer> fileIds) throws StorageEngineException {
        return dbAdaptor.getStudyConfigurationManager().lockAndUpdate(getStudyId(), studyConfiguration -> {
            studyConfiguration = checkExistsStudyConfiguration(studyConfiguration);
            for (Integer fileId : fileIds) {
                if (studyConfiguration.getIndexedFiles().contains(fileId)) {
                    throw StorageEngineException.alreadyLoaded(fileId, studyConfiguration);
                }
            }
            boolean resume = isResumeMerge(options);
            BatchFileOperation operation = StudyConfigurationManager
                    .addBatchOperation(studyConfiguration, MERGE.key(), fileIds, resume, BatchFileOperation.Type.LOAD);

            if (operation.currentStatus().equals(BatchFileOperation.Status.DONE)) {
                options.put(MERGE_SKIP.key(), true);
            }

            return studyConfiguration;
        });
    }

    private MongoDBVariantWriteResult mergeByChromosome(List<Integer> fileIds, int batchSize, int loadThreads,
            StudyConfiguration studyConfiguration, String chromosomeToLoad, Set<Integer> indexedFiles)
            throws StorageEngineException {
        MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyConfiguration.getStudyId());
        MongoDBVariantStageReader reader = new MongoDBVariantStageReader(stageCollection, studyConfiguration.getStudyId(),
                chromosomeToLoad == null ? Collections.emptyList() : Collections.singletonList(chromosomeToLoad));
        MergeMode mergeMode = MergeMode.from(studyConfiguration.getAttributes());
        if (mergeMode.equals(MergeMode.BASIC)) {
            // Read only files to load when MergeMode is BASIC
            reader.setFileIds(fileIds);
        }
        boolean resume = isResumeMerge(options);
        boolean cleanWhileLoading = options.getBoolean(STAGE_CLEAN_WHILE_LOAD.key(), STAGE_CLEAN_WHILE_LOAD.defaultValue());
        ProgressLogger progressLogger = new ProgressLogger("Write variants in VARIANTS collection:", reader::countNumVariants, 200);
        progressLogger.setApproximateTotalCount(reader.countAproxNumVariants());

        boolean ignoreOverlapping = studyConfiguration.getAttributes().getBoolean(MERGE_IGNORE_OVERLAPPING_VARIANTS.key(),
                MERGE_IGNORE_OVERLAPPING_VARIANTS.defaultValue());
        int release = options.getInt(Options.RELEASE.key(), Options.RELEASE.defaultValue());
        MongoDBVariantMerger variantMerger = new MongoDBVariantMerger(dbAdaptor, studyConfiguration, fileIds, indexedFiles, resume,
                ignoreOverlapping, release);
        MongoDBVariantMergeLoader variantLoader = new MongoDBVariantMergeLoader(
                dbAdaptor.getVariantsCollection(), stageCollection, dbAdaptor.getStudiesCollection(),
                studyConfiguration, fileIds, resume, cleanWhileLoading, progressLogger);

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
            if (chromosomeToLoad != null) {
                logger.info("Merging files {} in chromosome: {}. Other indexed files in chromosome {}: {}",
                        fileIds, chromosomeToLoad, chromosomeToLoad, indexedFiles);
            } else {
                logger.info("Merging files " + fileIds);
            }
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
    public void securePostLoad(List<Integer> fileIds, StudyConfiguration studyConfiguration) throws StorageEngineException {
        super.securePostLoad(fileIds, studyConfiguration);
        boolean direct = options.getBoolean(DIRECT_LOAD.key(), DIRECT_LOAD.defaultValue());
        if (direct) {
            BatchFileOperation.Status status = setStatus(studyConfiguration, BatchFileOperation.Status.READY, DIRECT_LOAD.key(), fileIds);
            if (status != BatchFileOperation.Status.DONE) {
                logger.warn("Unexpected status " + status);
            }
        } else {
            BatchFileOperation.Status status = setStatus(studyConfiguration, BatchFileOperation.Status.READY, MERGE.key(), fileIds);
            if (status != BatchFileOperation.Status.DONE) {
                logger.warn("Unexpected status " + status);
            }
        }
        Set<String> genotypes = new HashSet<>(studyConfiguration.getAttributes().getAsStringList(LOADED_GENOTYPES.key()));
        genotypes.addAll(writeResult.getGenotypes());
        studyConfiguration.getAttributes().put(LOADED_GENOTYPES.key(), genotypes);
    }

    @Override
    public ObjectMap getLoadStats() {
        return loadStats;
    }

    @Override
    protected void checkLoadedVariants(List<Integer> fileIds, StudyConfiguration studyConfiguration)
            throws StorageEngineException {
        if (fileIds.size() == 1) {
            checkLoadedVariants(fileIds.get(0), studyConfiguration);
        } else {
            // FIXME: Check variants in this situation!
            logger.warn("Skip check loaded variants");
        }
    }

    @Override
    protected void checkLoadedVariants(int fileId, StudyConfiguration studyConfiguration) throws
            StorageEngineException {

        if (getOptions().getBoolean(POST_LOAD_CHECK_SKIP.key(), POST_LOAD_CHECK_SKIP.defaultValue())) {
            logger.warn("Skip check loaded variants");
            return;
        }

        VariantFileMetadata fileMetadata = getStudyConfigurationManager().getVariantFileMetadata(getStudyId(), fileId, null).first();

        Long count = dbAdaptor.count(new Query()
                .append(VariantQueryParam.FILE.key(), fileId)
                .append(VariantQueryParam.STUDY.key(), studyConfiguration.getStudyId())).first();
        Long overlappedCount = dbAdaptor.count(new Query()
                .append(VariantQueryParam.FILE.key(), -fileId)
                .append(VariantQueryParam.STUDY.key(), studyConfiguration.getStudyId())).first();
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

        logger.info("============================================================");
        logger.info("Check loaded file '" + fileMetadata.getPath() + "' (" + fileId + ')');
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
    /*  StudyConfiguration utils methods       */
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
     * The StudyConfiguration must be complete, with all the indexed files, and samples in files.
     * Provided StudyConfiguration won't be modified
     * Requirements:
     * - All samples in file must be or loaded or not loaded
     * - If all samples loaded, must match (same order and samples) with the last loaded file.
     *
     * @param studyConfiguration StudyConfiguration from the selected study
     * @param fileId             File to load
     * @param loadSplitData      Allow load split data
     * @return Returns if this file represents a new batch of samples
     * @throws StorageEngineException If there is any unaccomplished requirement
     */
    public static boolean checkCanLoadSampleBatch(final StudyConfiguration studyConfiguration, int fileId, boolean loadSplitData)
            throws StorageEngineException {
        LinkedHashSet<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(fileId);
        if (!sampleIds.isEmpty()) {
            boolean allSamplesRepeated = true;
            boolean someSamplesRepeated = false;

            BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);
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
                    if (studyConfiguration.getSamplesInFiles().get(fileId).size() > 100) {
                        logger.info("About to load split data for samples in file " + studyConfiguration.getSamplesInFiles().get(fileId));
                    } else {
                        String samples = studyConfiguration.getSamplesInFiles().get(fileId)
                                .stream()
                                .map(studyConfiguration.getSampleIds().inverse()::get)
                                .collect(Collectors.joining(",", "[", "]"));
                        logger.info("About to load split data for samples " + samples);
                    }
                } else {
                    throw MongoVariantStorageEngineException.alreadyLoadedSamples(studyConfiguration, fileId);
                }
                return false;
            } else if (someSamplesRepeated) {
                throw MongoVariantStorageEngineException.alreadyLoadedSomeSamples(studyConfiguration, fileId);
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
    public boolean checkCanLoadDirectly(URI input) throws StorageEngineException {
        boolean doDirectLoad;

        // Direct load can be avoided from outside, but can not be forced.
        if (!getOptions().getBoolean(DIRECT_LOAD.key(), true)) {
            doDirectLoad = false;
        } else {
            StudyConfiguration studyConfiguration = getStudyConfiguration();

            // Direct load if loading one file, and there were no other indexed file in the study.
            if ((studyConfiguration == null || studyConfiguration.getIndexedFiles().isEmpty())) {
                doDirectLoad = true;
            } else if (getOptions().getBoolean(LOAD_SPLIT_DATA.key(), LOAD_SPLIT_DATA.defaultValue())) {
                LinkedHashSet<Integer> sampleIds = readVariantFileMetadata(input).getSampleIds().stream()
                        .map(studyConfiguration.getSampleIds()::get).collect(Collectors.toCollection(LinkedHashSet::new));
                doDirectLoad = true;
                for (Integer fileId : studyConfiguration.getIndexedFiles()) {
                    if (!sampleIds.equals(studyConfiguration.getSamplesInFiles().get(fileId))) {
                        doDirectLoad = false;
                        break;
                    }
                }
            } else {
                doDirectLoad = false;
            }
        }
        return doDirectLoad;
    }

}
