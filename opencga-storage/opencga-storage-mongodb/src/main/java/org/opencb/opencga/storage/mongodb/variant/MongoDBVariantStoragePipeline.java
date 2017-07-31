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

import com.google.common.collect.BiMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.MergeMode;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.exceptions.MongoVariantStorageEngineException;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantWriteResult;
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
import java.util.function.Predicate;

import static org.opencb.opencga.storage.core.metadata.StudyConfigurationManager.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.*;

/**
 * Created on 30/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStoragePipeline extends VariantStoragePipeline {

    // Type of variants that won't be loaded.
    public static final EnumSet<VariantType> SKIPPED_VARIANTS = EnumSet.of(
            VariantType.NO_VARIATION,
            VariantType.SYMBOLIC,
//            VariantType.CNV,
//            VariantType.DUPLICATION,
            VariantType.INVERSION,
            VariantType.TRANSLOCATION,
            VariantType.BREAKEND);

    private final VariantMongoDBAdaptor dbAdaptor;
    private final ObjectMap loadStats = new ObjectMap();
    private final Logger logger = LoggerFactory.getLogger(MongoDBVariantStoragePipeline.class);
    private MongoDBVariantWriteResult writeResult;

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
    protected void securePreLoad(StudyConfiguration studyConfiguration, VariantSource source) throws StorageEngineException {
        super.securePreLoad(studyConfiguration, source);
        int fileId = options.getInt(Options.FILE_ID.key());

        if (studyConfiguration.getAttributes().containsKey(Options.MERGE_MODE.key())
                || studyConfiguration.getAttributes().containsKey(MERGE_IGNORE_OVERLAPPING_VARIANTS.key())) {
            if (studyConfiguration.getAttributes().getBoolean(MERGE_IGNORE_OVERLAPPING_VARIANTS.key())) {
                studyConfiguration.getAttributes().put(Options.MERGE_MODE.key(), MergeMode.BASIC);
                logger.debug("Do not merge overlapping variants, as said in the StudyConfiguration");
            } else {
                studyConfiguration.getAttributes().put(Options.MERGE_MODE.key(), MergeMode.ADVANCED);
                logger.debug("Merge overlapping variants, as said in the StudyConfiguration");
            }
        } else {
            MergeMode mergeMode = MergeMode.from(options);
            studyConfiguration.getAttributes().put(Options.MERGE_MODE.key(), mergeMode);
            switch (mergeMode) {
                case BASIC:
                    studyConfiguration.getAttributes().put(MERGE_IGNORE_OVERLAPPING_VARIANTS.key(), true);
                    // When ignoring overlapping variants, the default genotype MUST be the UNKNOWN_GENOTYPE (?/?).
                    // Otherwise, a "fillGaps" step will be needed afterwards
                    studyConfiguration.getAttributes().put(DEFAULT_GENOTYPE.key(), DocumentToSamplesConverter.UNKNOWN_GENOTYPE);
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
                VariantStudy.StudyType studyType = options.get(Options.STUDY_TYPE.key(), VariantStudy.StudyType.class, Options.STUDY_TYPE
                        .defaultValue());
                switch (studyType) {
                    case FAMILY:
                    case TRIO:
                    case PAIRED:
                    case PAIRED_TUMOR:
                        defaultGenotype = Collections.singleton(DocumentToSamplesConverter.UNKNOWN_GENOTYPE);
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

        boolean newSampleBatch = checkCanLoadSampleBatch(studyConfiguration, fileId);

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

        securePreStage(fileId, studyConfiguration);
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

        boolean doMerge = options.getBoolean(MERGE.key(), false);
        boolean doStage = options.getBoolean(STAGE.key(), false);

        final int fileId = options.getInt(Options.FILE_ID.key());

        logger.info("Loading variants...");
        long start = System.currentTimeMillis();

        if (doStage) {
            stage(inputUri);
        }

        long skippedVariants = options.getLong("skippedVariants");
        if (doMerge) {
            MongoDBVariantWriteResult writeResult = merge(Collections.singletonList(fileId), skippedVariants);
        }
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants loaded!");


        return inputUri; //TODO: Return something like this: mongo://<host>/<dbName>/<collectionName>
    }

    public void stage(URI inputUri) throws StorageEngineException {
        final int fileId = options.getInt(Options.FILE_ID.key());

        if (!options.getBoolean(STAGE.key(), false)) {
            // Do not stage!
            return;
        }

        Path input = Paths.get(inputUri.getPath());

        VariantSource source = readVariantSource(inputUri, null);
        int numRecords = source.getStats().getNumRecords();
        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int bulkSize = options.getInt(BULK_SIZE.key(), batchSize);
        int loadThreads = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        final int numReaders = 1;
//        final int numTasks = loadThreads == 1 ? 1 : loadThreads - numReaders; //Subtract the reader thread

        MongoDBCollection stageCollection = dbAdaptor.getStageCollection();

        try {
            StudyConfiguration studyConfiguration = getStudyConfiguration();

            //Reader
            VariantReader variantReader;
            variantReader = VariantReaderUtils.getVariantReader(input, source);

            //Remapping ids task
            String fileIdStr = options.getString(Options.FILE_ID.key());
            ParallelTaskRunner.Task<Variant, Variant> remapIdsTask = batch -> {
                batch.forEach(variant -> variant.getStudies()
                        .forEach(studyEntry -> {
                            studyEntry.setStudyId(Integer.toString(studyConfiguration.getStudyId()));
                            studyEntry.getFiles().forEach(fileEntry -> fileEntry.setFileId(fileIdStr));
                        }));
                return batch;
            };


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
            if (options.getBoolean(STAGE_PARALLEL_WRITE.key(), STAGE_PARALLEL_WRITE.defaultValue())) {
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
                    throw new RuntimeException(e);
                }
            });
            try {
                Runtime.getRuntime().addShutdownHook(hook);
                ptr.run();
                stageSuccess(source);
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
                .append(VariantSourceDBAdaptor.VariantSourceQueryParam.STUDY_ID.key(), studyConfiguration.getStudyId())
                .append(VariantSourceDBAdaptor.VariantSourceQueryParam.FILE_ID.key(), fileId);
        Iterator<VariantSource> iterator = dbAdaptor.getVariantSourceDBAdaptor().iterator(query, new QueryOptions());

        boolean loadStageResume = false;
        boolean stage = true;

        BatchFileOperation operation = getBatchFileOperation(studyConfiguration.getBatches(),
                op -> op.getOperationName().equals(STAGE.key())
                        && op.getFileIds().equals(Collections.singletonList(fileId))
                        && !op.currentStatus().equals(BatchFileOperation.Status.READY)
        );

        if (iterator.hasNext()) {
            // Already indexed!
            logger.info("File \"{}\" ({}) already staged!", fileName, fileId);
            stage = false;
            if (operation != null && !operation.currentStatus().equals(BatchFileOperation.Status.READY)) {
                // There was an error writing the operation status. Restore to "READY"
                operation.addStatus(BatchFileOperation.Status.READY);
            }
        } else {
            loadStageResume = isResumeStage(options);

            if (operation != null) {
                switch (operation.currentStatus()) {
                    case RUNNING:
                        if (!loadStageResume) {
                            throw MongoVariantStorageEngineException.fileBeingStagedException(fileId, fileName);
                        }
                    case ERROR:
                        // Resume stage
                        loadStageResume = true;
                        options.put(STAGE_RESUME.key(), true);
                        break;
                    default:
                        throw new IllegalStateException("Unknown status: " + operation.currentStatus());
                }
            } else {
                operation = new BatchFileOperation(STAGE.key(), Collections.singletonList(fileId), System.currentTimeMillis(),
                        BatchFileOperation.Type.OTHER);
                studyConfiguration.getBatches().add(operation);
            }
            operation.addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.RUNNING);
        }

        if (stage) {
            BatchFileOperation mergeOperation = getBatchFileOperation(studyConfiguration.getBatches(),
                    op -> op.getOperationName().equals(MERGE.key()) && !op.currentStatus().equals(BatchFileOperation.Status.READY));
            if (mergeOperation != null) {
                // Avoid stage new files if there are ongoing merge operations
                throw MongoVariantStorageEngineException.otherOperationInProgressException(mergeOperation,
                        STAGE.key(), Collections.singletonList(fileId));
            }
        }

        options.put(STAGE.key(), stage);
        return operation;
    }

    private BatchFileOperation getBatchFileOperation(List<BatchFileOperation> batches, Predicate<BatchFileOperation> filter) {
        for (int i = batches.size() - 1; i >= 0; i--) {
            BatchFileOperation op = batches.get(i);
            if (filter.test(op)) {
                return op;
            }
        }
        return null;
    }

    public void stageError() throws StorageEngineException {
        int fileId = options.getInt(Options.FILE_ID.key());
        getStudyConfigurationManager()
                .atomicSetStatus(getStudyId(), BatchFileOperation.Status.ERROR, STAGE.key(), Collections.singletonList(fileId));
    }

    public void stageSuccess(VariantSource source) throws StorageEngineException {
        // Stage loading finished. Save VariantSource and update BatchOperation
        int fileId = options.getInt(Options.FILE_ID.key());
        source.setFileId(String.valueOf(fileId));
        source.setStudyId(String.valueOf(getStudyId()));

        getStudyConfigurationManager()
                .atomicSetStatus(getStudyId(), BatchFileOperation.Status.READY, STAGE.key(), Collections.singletonList(fileId));
        dbAdaptor.getVariantSourceDBAdaptor().updateVariantSource(source);

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
        options.put(Options.FILE_ID.key(), fileIds);

        StudyConfiguration studyConfiguration = preMerge(fileIds);

        //Stage collection where files are loaded.
        MongoDBCollection stageCollection = dbAdaptor.getStageCollection();

        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int loadThreads = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        int capacity = options.getInt("blockingQueueCapacity", loadThreads * 2);

        //Iterate over all the files
        Query query = new Query(VariantSourceDBAdaptor.VariantSourceQueryParam.STUDY_ID.key(), studyConfiguration.getStudyId());
        Iterator<VariantSource> iterator = dbAdaptor.getVariantSourceDBAdaptor().iterator(query, null);

        // List of chromosomes to be loaded
        Set<String> chromosomesToLoad = new HashSet<>();
        // List of all the indexed files that cover each chromosome
        ListMultimap<String, Integer> chromosomeInLoadedFiles = LinkedListMultimap.create();
        // List of all the indexed files that cover each chromosome
        ListMultimap<String, Integer> chromosomeInFilesToLoad = LinkedListMultimap.create();

        Set<String> wholeGenomeFiles = new HashSet<>();
        Set<String> byChromosomeFiles = new HashSet<>();
        while (iterator.hasNext()) {
            VariantSource variantSource = iterator.next();
            int fileId = Integer.parseInt(variantSource.getFileId());

            // If the file is going to be loaded, check if covers just one chromosome
            if (fileIds.contains(fileId)) {
                if (variantSource.getStats().getChromosomeCounts().size() == 1) {
                    chromosomesToLoad.addAll(variantSource.getStats().getChromosomeCounts().keySet());
                    byChromosomeFiles.add(variantSource.getFileName());
                } else {
                    wholeGenomeFiles.add(variantSource.getFileName());
                }
            }
            // If the file is indexed, add to the map of chromosome->fileId
            for (String chromosome : variantSource.getStats().getChromosomeCounts().keySet()) {
                if (studyConfiguration.getIndexedFiles().contains(fileId)) {
                    chromosomeInLoadedFiles.put(chromosome, fileId);
                } else if (fileIds.contains(fileId)) {
                    chromosomeInFilesToLoad.put(chromosome, fileId);
                } // else { ignore files that are not loaded, and are not going to be loaded }
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
                } catch (StorageEngineException e) {
                    logger.error("Failed setting status '" + MERGE.key() + "' operation over files " + fileIds
                            + " to '" + BatchFileOperation.Status.ERROR + '\'', e);
                    throw new RuntimeException(e);
                }
            });
            Runtime.getRuntime().addShutdownHook(hook);
            try {
                if (!wholeGenomeFiles.isEmpty() && !byChromosomeFiles.isEmpty()) {
                    String message = "Impossible to merge files splitted and not splitted by chromosome at the same time! "
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
        int studyId = getStudyId();
        Set<Integer> fileIdsSet = new HashSet<>(fileIds);
        return dbAdaptor.getStudyConfigurationManager().lockAndUpdate(studyId, studyConfiguration -> {
            for (Integer fileId : fileIds) {
                if (studyConfiguration.getIndexedFiles().contains(fileId)) {
                    throw StorageEngineException.alreadyLoaded(fileId, studyConfiguration);
                }
            }

            boolean loadMergeResume = isResumeMerge(options);

            List<BatchFileOperation> batches = studyConfiguration.getBatches();
            BatchFileOperation operation = null;
            for (int i = batches.size() - 1; i >= 0; i--) {
                BatchFileOperation op = batches.get(i);
                if (op.getOperationName().equals(MERGE.key())
                        && fileIds.size() == op.getFileIds().size()
                        && fileIdsSet.containsAll(op.getFileIds())) {
                    switch (op.currentStatus()) {
                        case READY:// Already indexed!
                            // TODO: Believe this ready? What if deleted?
                            // It was not "indexed" so suppose "deleted" ?
                            break;
                        case DONE:
                            // Already merged but still needs some work.
                            logger.info("Files " + fileIds + " where already merged, but where not marked as indexed files.");
                            options.put(MERGE_SKIP.key(), true);
                        case RUNNING:
                            if (!loadMergeResume) {
                                throw MongoVariantStorageEngineException.filesBeingMergedException(fileIds);
                            }
                            break;
                        case ERROR:
                            // Resume merge
                            loadMergeResume = true;
                            options.put(MERGE_RESUME.key(), loadMergeResume);
                            break;
                        default:
                            throw new IllegalStateException("Unknown status: " + op.currentStatus());
                    }
                    operation = op;
                    break;
                } else {
                    // Can not merge any file if there is an ongoing MERGE or STAGE operation
                    if (op.getOperationName().equals(MERGE.key()) || op.getOperationName().equals(STAGE.key())) {
                        if (!op.currentStatus().equals(BatchFileOperation.Status.READY)) {
                            throw MongoVariantStorageEngineException.otherOperationInProgressException(op, MERGE.key(), fileIds);
                        }
                    }
                }
            }

            if (operation == null) {
                operation = new BatchFileOperation(MERGE.key(), fileIds, System.currentTimeMillis(), BatchFileOperation.Type.LOAD);
                studyConfiguration.getBatches().add(operation);
                operation.addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.RUNNING);
            } else if (operation.currentStatus() == BatchFileOperation.Status.ERROR) {
                // Only set to RUNNING if it was on ERROR
                operation.addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.RUNNING);
            }
            return studyConfiguration;
        });
    }

    private MongoDBVariantWriteResult mergeByChromosome(List<Integer> fileIds, int batchSize, int loadThreads,
            StudyConfiguration studyConfiguration, String chromosomeToLoad, Set<Integer> indexedFiles)
            throws StorageEngineException {
        MongoDBCollection stageCollection = dbAdaptor.getStageCollection();
        MongoDBVariantStageReader reader = new MongoDBVariantStageReader(stageCollection, studyConfiguration.getStudyId(),
                chromosomeToLoad == null ? Collections.emptyList() : Collections.singletonList(chromosomeToLoad));
        MergeMode mergeMode = MergeMode.from(options);
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
        MongoDBVariantMerger variantMerger = new MongoDBVariantMerger(dbAdaptor, studyConfiguration, fileIds, indexedFiles, resume,
                ignoreOverlapping);
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
            if (options.getBoolean(MERGE_PARALLEL_WRITE.key(), MERGE_PARALLEL_WRITE.defaultValue())) {
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

        if (options.getBoolean(MERGE.key())) {
            return super.postLoad(input, output);
        } else {
            return input;
        }
    }

    @Override
    public void securePostLoad(List<Integer> fileIds, StudyConfiguration studyConfiguration) throws StorageEngineException {
        super.securePostLoad(fileIds, studyConfiguration);
        BatchFileOperation.Status status = setStatus(studyConfiguration, BatchFileOperation.Status.READY, MERGE.key(), fileIds);
        if (status != BatchFileOperation.Status.DONE) {
            logger.warn("Unexpected status " + status);
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
        VariantSource variantSource = dbAdaptor.getVariantSourceDBAdaptor().get(String.valueOf(fileId), null).first();

        Long count = dbAdaptor.count(new Query()
                .append(VariantQueryParam.FILES.key(), fileId)
                .append(VariantQueryParam.STUDIES.key(), studyConfiguration.getStudyId())).first();
        Long overlappedCount = dbAdaptor.count(new Query()
                .append(VariantQueryParam.FILES.key(), -fileId)
                .append(VariantQueryParam.STUDIES.key(), studyConfiguration.getStudyId())).first();
        long variantsToLoad = 0;

        long expectedSkippedVariants = 0;
        long alreadyLoadedVariants = options.getLong(ALREADY_LOADED_VARIANTS.key(), 0L);

        for (Map.Entry<String, Integer> entry : variantSource.getStats().getVariantTypeCounts().entrySet()) {
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
        logger.info("Check loaded file '" + variantSource.getFileName() + "' (" + fileId + ')');
        if (expectedSkippedVariants != writeResult.getSkippedVariants()) {
            logger.error("Wrong number of skipped variants. Expected " + expectedSkippedVariants + " and got " + writeResult
                    .getSkippedVariants());
        } else if (writeResult.getSkippedVariants() > 0) {
            logger.warn("There were " + writeResult.getSkippedVariants() + " skipped variants.");
            for (VariantType type : SKIPPED_VARIANTS) {
                Integer countByType = variantSource.getStats().getVariantTypeCounts().get(type.toString());
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
     * @return Returns if this file represents a new batch of samples
     * @throws StorageEngineException If there is any unaccomplished requirement
     */
    public static boolean checkCanLoadSampleBatch(final StudyConfiguration studyConfiguration, int fileId) throws StorageEngineException {
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
                ArrayList<Integer> indexedFiles = new ArrayList<>(studyConfiguration.getIndexedFiles());
                if (!indexedFiles.isEmpty()) {
                    int lastIndexedFile = indexedFiles.get(indexedFiles.size() - 1);
                    //Check that are the same samples in the same order
                    if (!new ArrayList<>(studyConfiguration.getSamplesInFiles().get(lastIndexedFile)).equals(new ArrayList<>(sampleIds))) {
                        //ERROR
                        if (studyConfiguration.getSamplesInFiles().get(lastIndexedFile).containsAll(sampleIds)) {
                            throw new StorageEngineException("Unable to load this batch. Wrong samples order"); //TODO: Should it care?
                        } else {
                            throw new StorageEngineException("Unable to load this batch. Another sample batch has been loaded already.");
                        }
                    }
                    //Ok, the batch of samples matches with the last loaded batch of samples.
                    return false; // This is NOT a new batch of samples
                }
            } else if (someSamplesRepeated) {
                throw new StorageEngineException("There was some already indexed samples, but not all of them. "
                        + "Unable to load in Storage-MongoDB");
            }
        }
        return true; // This is a new batch of samples
    }

}
