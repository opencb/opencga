package org.opencb.opencga.storage.mongodb.variant;

import com.google.common.collect.BiMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.bson.Document;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageETL;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantMerger;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantStageLoader;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantStageReader;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager.MongoDBVariantOptions.*;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager.Options;

/**
 * Created on 30/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStorageETL extends VariantStorageETL {

    private final VariantMongoDBAdaptor dbAdaptor;

    public MongoDBVariantStorageETL(StorageConfiguration configuration, String storageEngineId,
                                    VariantMongoDBAdaptor dbAdaptor) {
        super(configuration, storageEngineId, LoggerFactory.getLogger(MongoDBVariantStorageETL.class), dbAdaptor, new VariantReaderUtils());
        this.dbAdaptor = dbAdaptor;
    }

    protected VariantMongoDBWriter getDBWriter(String dbName, int fileId, StudyConfiguration studyConfiguration)
            throws StorageManagerException {
        return new VariantMongoDBWriter(fileId, studyConfiguration, dbAdaptor, true, false);
    }

    public URI preLoad(URI input, URI output) throws StorageManagerException {
        URI uri = super.preLoad(input, output);

//        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        int fileId = options.getInt(Options.FILE_ID.key());

        boolean newSampleBatch = checkCanLoadSampleBatch(studyConfiguration, fileId);

        if (options.containsKey(Options.EXTRA_GENOTYPE_FIELDS.key())) {
            List<String> extraFields = options.getAsStringList(Options.EXTRA_GENOTYPE_FIELDS.key());
            if (studyConfiguration.getIndexedFiles().isEmpty()) {
                studyConfiguration.getAttributes().put(Options.EXTRA_GENOTYPE_FIELDS.key(), extraFields);
            } else {
                if (!extraFields.equals(studyConfiguration.getAttributes().getAsStringList(Options.EXTRA_GENOTYPE_FIELDS.key()))) {
                    throw new StorageManagerException("Unable to change Stored Extra Fields if there are already indexed files.");
                }
            }
            if (!studyConfiguration.getAttributes().containsKey(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key())) {
                VariantSource source = VariantStorageManager.readVariantSource(Paths.get(input.getPath()), null);
                List<String> extraFieldsType = new ArrayList<>(extraFields.size());
                for (String extraField : extraFields) {
                    List<Map<String, Object>> formats = (List) source.getHeader().getMeta().get("FORMAT");
                    String type = "String";
                    for (Map<String, Object> format : formats) {
                        if (format.get("ID").toString().equals(extraField)) {
                            if ("1".equals(format.get("Number"))) {
                                type = Objects.toString(format.get("Type"));
                            } else {
                                //Fields with arity != 1 are loaded as String
                                type = "String";
                            }
                            break;
                        }
                    }
                    switch (type) {

                        case "String":
                        case "Float":
                        case "Integer":
                            break;
                        case "Character":
                        default:
                            type = "String";
                            break;

                    }
                    extraFieldsType.add(type);
                    System.err.println(extraField + " : " + type);
                }
                studyConfiguration.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), extraFieldsType);
            }
        }

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

//        VariantMongoDBAdaptor dbAdaptor = getDBAdaptor(options.getString(Options.DB_NAME.key()));

        // TODO: Resume mode if there are files in STAGE collection
        // Save current status: Save if some load has been started.

        QueryResult<Long> countResult = dbAdaptor.count(new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration
                .getStudyId())
                .append(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileId));
        Long count = countResult.first();
        if (count != 0) {
            logger.warn("Resume mode. There are already loaded variants from the file "
                    + studyConfiguration.getFileIds().inverse().get(fileId) + " : " + fileId + " ");
            options.put(ALREADY_LOADED_VARIANTS.key(), count);
        }

        return uri;
    }

    @Override
    public URI load(URI inputUri) throws IOException, StorageManagerException {
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);

        Path input = Paths.get(inputUri.getPath());

//        boolean includeSamples = options.getBoolean(Options.INCLUDE_GENOTYPES.key(), Options.INCLUDE_GENOTYPES.defaultValue());
        boolean includeStats = options.getBoolean(Options.INCLUDE_STATS.key(), Options.INCLUDE_STATS.defaultValue());
//        boolean includeSrc = options.getBoolean(Options.INCLUDE_SRC.key(), Options.INCLUDE_SRC.defaultValue());
//        boolean compressGenotypes = options.getBoolean(Options.COMPRESS_GENOTYPES.key(), false);
//        boolean compressGenotypes = defaultGenotype != null && !defaultGenotype.isEmpty();

        Set<String> defaultGenotype;
        if (studyConfiguration.getAttributes().containsKey(DEFAULT_GENOTYPE.key())) {
            defaultGenotype = new HashSet<>(studyConfiguration.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key()));
            logger.debug("Using default genotype from study configuration: {}", defaultGenotype);
        } else {
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
                        defaultGenotype = new HashSet<>(Arrays.asList("0/0", "0|0"));
                        logger.debug("No default genotype found. Using default genotype: {}", defaultGenotype);
                        break;
                }
            }
            studyConfiguration.getAttributes().put(DEFAULT_GENOTYPE.key(), defaultGenotype);
        }

        //Create a new VariantSource. This object will be
        // filled at the VariantJsonReader in the pre()
        VariantSource source = new VariantSource(inputUri.getPath(), "", "", "");
        final int fileId = options.getInt(Options.FILE_ID.key());


//        VariantSource variantSource = readVariantSource(input, null);
//        new StudyInformation(variantSource.getStudyId())

        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int bulkSize = options.getInt(BULK_SIZE.key(), batchSize);
        int loadThreads = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        int capacity = options.getInt("blockingQueueCapacity", loadThreads * 2);
        final int numReaders = 1;
        final int numWriters = loadThreads == 1 ? 1 : loadThreads - numReaders; //Subtract the reader thread


        //Reader
        VariantReader variantReader;
        variantReader = VariantReaderUtils.getVariantReader(input, source);


        Task<Variant> remapIdsTask = new Task<Variant>() {
            private final String fileIdStr = options.getString(Options.FILE_ID.key());
            @Override
            public boolean apply(List<Variant> variants) {
                variants.forEach(variant -> variant.getStudies()
                        .forEach(studyEntry -> {
                            studyEntry.setStudyId(Integer.toString(studyConfiguration.getStudyId()));
                            studyEntry.getFiles().forEach(fileEntry -> fileEntry.setFileId(fileIdStr));
                        }));
                return true;
            }
        };

        logger.info("Loading variants...");
        long start = System.currentTimeMillis();

        //Runner
        logger.info("Multi thread load... [{} readerThreads, {} writerThreads]", numReaders, numWriters);

        MongoDBCollection stageCollection = dbAdaptor.getDB().getCollection(
                options.getString(COLLECTION_STAGE.key(), COLLECTION_STAGE.defaultValue()));
        ParallelTaskRunner<Variant, Variant> ptr;
        MongoDBVariantStageLoader stageWriter;
        int numRecords = readVariantSource(inputUri, null).getStats().getNumRecords();
        try {
            stageWriter = new MongoDBVariantStageLoader(stageCollection, studyConfiguration.getStudyId(), fileId, numRecords);
            class TaskWriter implements ParallelTaskRunner.Task<Variant, Variant> {
                public List<Variant> apply(List<Variant> batch) {
                    try {
                        remapIdsTask.apply(batch);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e); // IMPOSSIBLE
                    }
                    stageWriter.insert(batch);
                    return batch;
                }
            }

            ptr = new ParallelTaskRunner<>(
                    variantReader,
                    new TaskWriter(),
                    null,
                    new ParallelTaskRunner.Config(loadThreads, batchSize, capacity, false)
            );
        } catch (Exception e) {
            e.printStackTrace();
            throw new StorageManagerException("Error while creating ParallelTaskRunner", e);
        }

        try {
            ptr.run();
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new StorageManagerException("Error while executing LoadVariants in ParallelTaskRunner", e);
        }

        int skippedVariants = (int) stageWriter.getWriteResult().getSkippedVariants();
        if (options.getBoolean("merge", true)) {
            MongoDBVariantWriteResult writeResult = merge(Collections.singletonList(fileId), batchSize, loadThreads, capacity,
                    numRecords, skippedVariants, stageCollection);
        }
        options.put("skippedVariants", skippedVariants);

        logger.info("Stage Write result: {}", skippedVariants);

        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants loaded!");

        source.setFileId(options.getString(Options.FILE_ID.key()));
        source.setStudyId(options.getString(Options.STUDY_ID.key()));
        dbAdaptor.getVariantSourceDBAdaptor().updateVariantSource(source);

        return inputUri; //TODO: Return something like this: mongo://<host>/<dbName>/<collectionName>
    }

    /**
     * Merge staged files into Variant collection.
     *
     * @param fileIds           FileIDs of the files to be merged
     * @return                  Write Result with times and count
     * @throws StorageManagerException  If there is a problem executing the {@link ParallelTaskRunner}
     */
    public MongoDBVariantWriteResult merge(List<Integer> fileIds) throws StorageManagerException {
        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int loadThreads = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        int capacity = options.getInt("blockingQueueCapacity", loadThreads * 2);
        MongoDBCollection collection = dbAdaptor.getDB().getCollection(
                options.getString(COLLECTION_STAGE.key(), COLLECTION_STAGE.defaultValue()));

        return merge(fileIds, batchSize, loadThreads, capacity, 0, options.getInt("skippedVariants", 0), collection);
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
     * @param batchSize         Batch size
     * @param loadThreads       Number of load threads
     * @param capacity          Capacity of the intermedial queue
     * @param numRecords        Number of variant records in the intermediate file
     * @param skippedVariants   Number of skipped variants into the Stage
     * @param stageCollection   Stage collection where files are loaded.
     * @return                  Write Result with times and count
     * @throws StorageManagerException  If there is a problem executing the {@link ParallelTaskRunner}
     */
    protected MongoDBVariantWriteResult merge(List<Integer> fileIds, int batchSize, int loadThreads, int capacity,
                                           int numRecords, int skippedVariants, MongoDBCollection stageCollection)
            throws StorageManagerException {

        long start = System.currentTimeMillis();
        StudyConfiguration studyConfiguration = getStudyConfiguration();


        //Iterate over all the files
        Query query = new Query(VariantSourceDBAdaptor.VariantSourceQueryParam.STUDY_ID.key(), studyConfiguration.getStudyId());
        Iterator<VariantSource> iterator = dbAdaptor.getVariantSourceDBAdaptor().iterator(query, null);

        // List of chromosomes to be loaded
        Set<String> chromosomesToLoad = new HashSet<>();
        // List of all the indexed files that cover each chromosome
        ListMultimap<String, Integer> chromosomeInLoadedFiles = LinkedListMultimap.create();
        // List of all the indexed files that cover each chromosome
        ListMultimap<String, Integer> chromosomeInFilesToLoad = LinkedListMultimap.create();

        boolean wholeGenomeFiles = false;
        while (iterator.hasNext()) {
            VariantSource variantSource = iterator.next();
            int fileId = Integer.parseInt(variantSource.getFileId());

            // If the file is going to be loaded, check if covers just one chromosome
            if (fileIds.contains(fileId)) {
                if (variantSource.getStats().getChromosomeCounts().size() == 1) {
                    chromosomesToLoad.addAll(variantSource.getStats().getChromosomeCounts().keySet());
                } else {
                    wholeGenomeFiles = true;
                }
            }
            // If the file is indexed, add to the map of chromosome->fileId
            for (String chromosome : variantSource.getStats().getChromosomeCounts().keySet()) {
                if (studyConfiguration.getIndexedFiles().contains(fileId)) {
                    chromosomeInLoadedFiles.put(chromosome, fileId);
                } else {
                    chromosomeInFilesToLoad.put(chromosome, fileId);
                }
            }
        }

        if (wholeGenomeFiles && !chromosomesToLoad.isEmpty()) {
            String message = "Impossible to merge files splitted and not splitted by chromosome at the same time!";
            logger.error(message);
            throw new StorageManagerException(message);
        }

        final MongoDBVariantWriteResult writeResult;
        if (chromosomesToLoad.isEmpty()) {
            writeResult = mergeByChromosome(fileIds, batchSize, loadThreads, capacity, stageCollection,
                    studyConfiguration, null, studyConfiguration.getIndexedFiles());
        } else {
            writeResult = new MongoDBVariantWriteResult();
            for (String chromosome : chromosomesToLoad) {
                List<Integer> filesToLoad = chromosomeInFilesToLoad.get(chromosome);
                Set<Integer> indexedFiles = new HashSet<>(chromosomeInLoadedFiles.get(chromosome));
                MongoDBVariantWriteResult aux = mergeByChromosome(filesToLoad, batchSize, loadThreads, capacity, stageCollection,
                        studyConfiguration, chromosome, indexedFiles);
                writeResult.merge(aux);
            }
        }

        long startTime = System.currentTimeMillis();
        logger.info("Deleting variant records from Stage collection");
        long modifiedCount = MongoDBVariantStageLoader.cleanStageCollection(stageCollection, studyConfiguration.getStudyId(), fileIds);
        logger.info("Delete variants time: " + (System.currentTimeMillis() - startTime) / 1000 + "s , CleanDocuments: " + modifiedCount);

        writeResult.setSkippedVariants(skippedVariants);

        logger.info("Write result: {}", writeResult.toString());
        logger.info("Write result: {}", writeResult.toTSV());
        logger.info("Write result: {}", writeResult.toJson());
        options.put("writeResult", writeResult);

        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants merged!");
        return writeResult;
    }

    private MongoDBVariantWriteResult mergeByChromosome(
            List<Integer> fileIds, int batchSize, int loadThreads, int capacity, MongoDBCollection stageCollection,
            StudyConfiguration studyConfiguration, String chromosomeToLoad, Set<Integer> indexedFiles)
            throws StorageManagerException {

        MongoDBVariantStageReader reader = new MongoDBVariantStageReader(stageCollection, studyConfiguration.getStudyId(),
                chromosomeToLoad == null ? Collections.emptyList() : Collections.singletonList(chromosomeToLoad));
        MongoDBVariantMerger variantWriter = new MongoDBVariantMerger(dbAdaptor, studyConfiguration, fileIds,
                dbAdaptor.getVariantsCollection(), reader.countNumVariants(), reader.countAproxNumVariants(), indexedFiles);

        ParallelTaskRunner<Document, MongoDBVariantWriteResult> ptrMerge;
        try {
            ptrMerge = new ParallelTaskRunner<>(reader, variantWriter, null,
                    new ParallelTaskRunner.Config(loadThreads, batchSize, capacity, false));
        } catch (Exception e) {
            e.printStackTrace();
            throw new StorageManagerException("Error while creating ParallelTaskRunner", e);
        }

        try {
            if (chromosomeToLoad != null) {
                logger.info("Merging files {} in the the chromosomes: {}. IndexedFiles in this chromosome: {}",
                        fileIds, chromosomeToLoad, indexedFiles);
            } else {
                logger.info("Merging files " + fileIds);
            }
            ptrMerge.run();
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new StorageManagerException("Error while executing LoadVariants in ParallelTaskRunner", e);
        }
        return variantWriter.getResult();
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageManagerException {

        if (options.getBoolean("merge")) {
            return super.postLoad(input, output);
        } else {
            return input;
        }
    }

    @Override
    protected void checkLoadedVariants(URI input, List<Integer> fileIds, StudyConfiguration studyConfiguration, ObjectMap options)
            throws StorageManagerException {
        if (fileIds.size() == 1) {
            checkLoadedVariants(input, fileIds.get(0), studyConfiguration, options);
        } else {
            // FIXME: Check variants in this situation!
            logger.warn("Skip check loaded variants");
        }
    }

    @Override
    protected void checkLoadedVariants(URI input, int fileId, StudyConfiguration studyConfiguration, ObjectMap options) throws
            StorageManagerException {
        VariantSource variantSource = VariantReaderUtils.readVariantSource(Paths.get(input.getPath()), null);

//        VariantMongoDBAdaptor dbAdaptor = getDBAdaptor(options.getString(VariantStorageManager.Options.DB_NAME.key()));
        Long count = dbAdaptor.count(new Query()
                .append(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileId)
                .append(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId())).first();
        long expectedCount = 0;
        long expectedSkippedVariants = 0;
        int symbolicVariants = 0;
        int nonVariants = 0;
        long alreadyLoadedVariants = options.getLong(ALREADY_LOADED_VARIANTS.key(), 0L);

        for (Map.Entry<String, Integer> entry : variantSource.getStats().getVariantTypeCounts().entrySet()) {
            if (entry.getKey().equals(VariantType.SYMBOLIC.toString())) {
                expectedSkippedVariants += entry.getValue();
                symbolicVariants = entry.getValue();
            } else if (entry.getKey().equals(VariantType.NO_VARIATION.toString())) {
                expectedSkippedVariants += entry.getValue();
                nonVariants = entry.getValue();
            } else {
                expectedCount += entry.getValue();
            }
        }
        MongoDBVariantWriteResult writeResult = options.get("writeResult", MongoDBVariantWriteResult.class);

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
        if (expectedSkippedVariants != writeResult.getSkippedVariants()) {
            logger.error("Wrong number of skipped variants. Expected " + expectedSkippedVariants + " and got " + writeResult
                    .getSkippedVariants());
        } else if (writeResult.getSkippedVariants() > 0) {
            logger.warn("There were " + writeResult.getSkippedVariants() + " skipped variants.");
            if (symbolicVariants > 0) {
                logger.info("  * Of which " + symbolicVariants + " are " + VariantType.SYMBOLIC.toString() + " variants.");
            }
            if (nonVariants > 0) {
                logger.info("  * Of which " + nonVariants + " are " + VariantType.NO_VARIATION.toString() + " variants.");
            }
        }

        if (writeResult.getNonInsertedVariants() != 0) {
            logger.error("There were " + writeResult.getNonInsertedVariants() + " duplicated variants not inserted. ");
        }

        if (alreadyLoadedVariants != 0) {
            logger.info("Resume mode. Previously loaded variants: " + alreadyLoadedVariants);
        }

        StorageManagerException exception = null;
        if (expectedCount != count) {
            String message = "Wrong number of loaded variants. Expected: " + expectedCount + " and got: " + count;
            logger.error(message);
//            exception = new StorageManagerException(message);
        } else {
            logger.info("Final number of loaded variants: " + count);
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
     * @throws StorageManagerException If there is any unaccomplished requirement
     */
    public static boolean checkCanLoadSampleBatch(final StudyConfiguration studyConfiguration, int fileId) throws StorageManagerException {
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
                            throw new StorageManagerException("Unable to load this batch. Wrong samples order"); //TODO: Should it care?
                        } else {
                            throw new StorageManagerException("Unable to load this batch. Another sample batch has been loaded already.");
                        }
                    }
                    //Ok, the batch of samples matches with the last loaded batch of samples.
                    return false; // This is NOT a new batch of samples
                }
            } else if (someSamplesRepeated) {
                throw new StorageManagerException("There was some already indexed samples, but not all of them. "
                        + "Unable to load in Storage-MongoDB");
            }
        }
        return true; // This is a new batch of samples
    }

    //    @Override
//    public void checkStudyConfiguration(StudyConfiguration studyConfiguration, VariantDBAdaptor dbAdaptor) throws
// StorageManagerException {
//        super.checkStudyConfiguration(studyConfiguration, dbAdaptor);
//        if (dbAdaptor == null) {
//            logger.debug("Do not check StudyConfiguration against the loaded in MongoDB");
//        } else {
//            if (dbAdaptor instanceof VariantMongoDBAdaptor) {
//                VariantMongoDBAdaptor mongoDBAdaptor = (VariantMongoDBAdaptor) dbAdaptor;
//                StudyConfigurationManager studyConfigurationDBAdaptor = mongoDBAdaptor.getStudyConfigurationManager();
//                StudyConfiguration studyConfigurationFromMongo = studyConfigurationDBAdaptor.getStudyConfiguration(studyConfiguration
// .getStudyId(), null).first();
//
//                //Check that the provided StudyConfiguration has the same or more information that the stored in MongoDB.
//                for (Map.Entry<String, Integer> entry : studyConfigurationFromMongo.getFileIds().entrySet()) {
//                    if (!studyConfiguration.getFileIds().containsKey(entry.getKey())) {
//                        throw new StorageManagerException("StudyConfiguration do not have the file " + entry.getKey());
//                    }
//                    if (!studyConfiguration.getFileIds().get(entry.getKey()).equals(entry.getValue())) {
//                        throw new StorageManagerException("StudyConfiguration changes the fileId of '" + entry.getKey() + "' from " +
// entry.getValue() + " to " + studyConfiguration.getFileIds().get(entry.getKey()));
//                    }
//                }
//                for (Map.Entry<String, Integer> entry : studyConfigurationFromMongo.getCohortIds().entrySet()) {
//                    if (!studyConfiguration.getCohortIds().containsKey(entry.getKey())) {
//                        throw new StorageManagerException("StudyConfiguration do not have the cohort " + entry.getKey());
//                    }
//                    if (!studyConfiguration.getCohortIds().get(entry.getKey()).equals(entry.getValue())) {
//                        throw new StorageManagerException("StudyConfiguration changes the cohortId of '" + entry.getKey() + "' from " +
// entry.getValue() + " to " + studyConfiguration.getCohortIds().get(entry.getKey()));
//                    }
//                }
//                for (Map.Entry<String, Integer> entry : studyConfigurationFromMongo.getSampleIds().entrySet()) {
//                    if (!studyConfiguration.getSampleIds().containsKey(entry.getKey())) {
//                        throw new StorageManagerException("StudyConfiguration do not have the sample " + entry.getKey());
//                    }
//                    if (!studyConfiguration.getSampleIds().get(entry.getKey()).equals(entry.getValue())) {
//                        throw new StorageManagerException("StudyConfiguration changes the sampleId of '" + entry.getKey() + "' from " +
// entry.getValue() + " to " + studyConfiguration.getSampleIds().get(entry.getKey()));
//                    }
//                }
//                studyConfigurationDBAdaptor.updateStudyConfiguration(studyConfiguration, null);
//            } else {
//                throw new StorageManagerException("Unknown VariantDBAdaptor '" + dbAdaptor.getClass().toString() + "'. Expected '" +
// VariantMongoDBAdaptor.class + "'");
//            }
//        }
//    }
}
