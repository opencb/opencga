package org.opencb.opencga.storage.mongodb.variant;

import com.google.common.collect.BiMap;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.tasks.VariantRunner;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageETL;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantMerger;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantStageLoader;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

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
        // input: getDBSchemaReader
        // output: getDBWriter()
//        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);

        Path input = Paths.get(inputUri.getPath());

//        boolean includeSamples = options.getBoolean(Options.INCLUDE_GENOTYPES.key(), Options.INCLUDE_GENOTYPES.defaultValue());
        boolean includeStats = options.getBoolean(Options.INCLUDE_STATS.key(), Options.INCLUDE_STATS.defaultValue());
//        boolean includeSrc = options.getBoolean(Options.INCLUDE_SRC.key(), Options.INCLUDE_SRC.defaultValue());

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

//        boolean compressGenotypes = options.getBoolean(Options.COMPRESS_GENOTYPES.key(), false);
//        boolean compressGenotypes = defaultGenotype != null && !defaultGenotype.isEmpty();

        VariantSource source = new VariantSource(inputUri.getPath(), "", "", "");       //Create a new VariantSource. This object will be
        // filled at the VariantJsonReader in the pre()
//        params.put(VARIANT_SOURCE, source);
        String dbName = options.getString(Options.DB_NAME.key(), null);

//        VariantSource variantSource = readVariantSource(input, null);
//        new StudyInformation(variantSource.getStudyId())

        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), 100);
        int bulkSize = options.getInt(BULK_SIZE.key(), batchSize);
        int loadThreads = options.getInt(Options.LOAD_THREADS.key(), 8);
        int capacity = options.getInt("blockingQueueCapacity", loadThreads * 2);
//        int numWriters = params.getInt(WRITE_MONGO_THREADS, Integer.parseInt(properties.getProperty
// (OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_WRITE_THREADS, "8")));
        final int numReaders = 1;
        final int numWriters = loadThreads == 1 ? 1 : loadThreads - numReaders; //Subtract the reader thread


        //Reader
        VariantReader variantReader;
        variantReader = VariantReaderUtils.getVariantReader(input, source);

        //Tasks
        List<Task<Variant>> taskList = new SortedList<>();


        //Writers
        List<VariantMongoDBWriter> writers = new LinkedList<>();
        List<DataWriter> writerList = new LinkedList<>();
        AtomicBoolean atomicBoolean = new AtomicBoolean();
        for (int i = 0; i < numWriters; i++) {
            VariantMongoDBWriter variantDBWriter = this.getDBWriter(dbName, options.getInt(VariantStorageManager.Options.FILE_ID.key()),
                    studyConfiguration);
//            variantDBWriter.setBulkSize(bulkSize);
//            variantDBWriter.includeSrc(includeSrc);
//            variantDBWriter.includeSamples(includeSamples);
            variantDBWriter.includeStats(includeStats);
//            variantDBWriter.setCompressDefaultGenotype(compressGenotypes);
//            variantDBWriter.setDefaultGenotype(defaultGenotype);
//            variantDBWriter.setVariantSource(source);
//            variantDBWriter.setSamplesIds(samplesIds);
            variantDBWriter.setThreadSynchronizationBoolean(atomicBoolean);
            writerList.add(variantDBWriter);
            writers.add(variantDBWriter);
        }


        final int fileId = options.getInt(Options.FILE_ID.key());
        final String fileIdStr = options.getString(Options.FILE_ID.key());
        Task<Variant> remapIdsTask = new Task<Variant>() {
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
        taskList.add(remapIdsTask);

        logger.info("Loading variants...");
        long start = System.currentTimeMillis();

        //Runner
        if (loadThreads == 1) {
            logger.info("Single thread load...");
            List<Task<Variant>> ts = Collections.singletonList(remapIdsTask);
            VariantRunner vr = new VariantRunner(source, (VariantReader) variantReader, null, (List) writers, taskList, batchSize);
            vr.run();
        } else {
            logger.info("Multi thread load... [{} readerThreads, {} writerThreads]", numReaders, numWriters);
//            ThreadRunner runner = new ThreadRunner(Executors.newFixedThreadPool(loadThreads), batchSize);
//            ThreadRunner.ReadNode<Variant> variantReadNode = runner.newReaderNode(variantJsonReader, 1);
//            ThreadRunner.WriterNode<Variant> variantWriterNode = runner.newWriterNode(writerList);
//
//            variantReadNode.append(variantWriterNode);
//            runner.run();

/*
            ParallelTaskRunner<Variant, Variant> ptr;
            try {
                class TaskWriter implements ParallelTaskRunner.Task<Variant, Variant> {
                    private DataWriter<Variant> writer;

                    public TaskWriter(DataWriter<Variant> writer) {
                        this.writer = writer;
                    }

                    @Override
                    public void pre() {
                        writer.pre();
                    }

                    @Override
                    public List<Variant> apply(List<Variant> batch) {
                        try {
                            remapIdsTask.apply(batch);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e); // IMPOSSIBLE
                        }
                        writer.write(batch);
                        return batch;
                    }

                    @Override
                    public void post() {
//                        writer.post();
                    }
                }

                List<ParallelTaskRunner.Task<Variant, Variant>> tasks = new LinkedList<>();
                for (VariantWriter writer : writers) {
                    tasks.add(new TaskWriter(writer));
                }

                ptr = new ParallelTaskRunner<>(
                        variantReader,
                        tasks,
                        null,
                        new ParallelTaskRunner.Config(loadThreads, batchSize, capacity, false)
                );
            } catch (Exception e) {
                e.printStackTrace();
                throw new StorageManagerException("Error while creating ParallelTaskRunner", e);
            }

            try {
                writers.forEach(DataWriter::open);
                ptr.run();
                writers.forEach(DataWriter::post);
                writers.forEach(DataWriter::close);
            } catch (ExecutionException e) {
                e.printStackTrace();
                throw new StorageManagerException("Error while executing LoadVariants in ParallelTaskRunner", e);
            }
*/


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
            MongoDBVariantWriteResult writeResult = merge(fileId, batchSize, loadThreads, capacity, numRecords, skippedVariants,
                    stageCollection);

            logger.info("Stage Write result: {}", skippedVariants);

        }

        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants loaded!");

        return inputUri; //TODO: Return something like this: mongo://<host>/<dbName>/<collectionName>
    }

    /**
     * Merge staged files into Variant collection.
     *
     * @param fileId            FileID of the file to be merged
     * @param batchSize         Batch size
     * @param loadThreads       Number of load threads
     * @param capacity          Capacity of the intermedial queue
     * @param numRecords        Number of variant records in the intermediate file
     * @param skippedVariants   Number of skipped variants into the Stage
     * @param stageCollection   Stage collection where files are loaded.
     * @return                  Write Result with times and count
     * @throws StorageManagerException  If there is a problem executing the {@link ParallelTaskRunner}
     */
    public MongoDBVariantWriteResult merge(int fileId, int batchSize, int loadThreads, int capacity,
                                           int numRecords, int skippedVariants, MongoDBCollection stageCollection)
            throws StorageManagerException {

        StudyConfiguration studyConfiguration = getStudyConfiguration();
        MongoDBVariantMerger variantWriter = new MongoDBVariantMerger(studyConfiguration, Collections.singletonList(fileId),
                dbAdaptor.getVariantsCollection(), numRecords - skippedVariants);
        ParallelTaskRunner<Document, MongoDBVariantWriteResult> ptrMerge;
        try {
            FindIterable<Document> iterable = stageCollection.nativeQuery().find(new Document(), new QueryOptions());
            MongoCursor<Document> iterator = iterable.iterator();
            ptrMerge = new ParallelTaskRunner<>(b -> {
                List<Document> list = new ArrayList<>(b);
                for (int i = 0; i < batchSize; i++) {
                    if (iterator.hasNext()) {
                        list.add(iterator.next());
                    }
                }
                return list;
            }, variantWriter, null, new ParallelTaskRunner.Config(loadThreads, batchSize, capacity, false));
        } catch (Exception e) {
            e.printStackTrace();
            throw new StorageManagerException("Error while creating ParallelTaskRunner", e);
        }

        /*
            List<Document> documents = new ArrayList<>(batchSize);
            for (Document document : stageCollection.nativeQuery().find(new Document(), new QueryOptions())) {
                documents.add(document);
                if (documents.size() == batchSize) {
                    variantWriter.load(documents.stream());
                    documents.clear();
                }
            }
            variantWriter.load(documents.stream());
        */

        try {
            ptrMerge.run();
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new StorageManagerException("Error while executing LoadVariants in ParallelTaskRunner", e);
        }

        long startTime = System.currentTimeMillis();
        MongoDBVariantStageLoader.deleteFiles(stageCollection, studyConfiguration.getStudyId(), fileId);
        logger.info("Delete variants time: " + (System.currentTimeMillis() - startTime) / 1000 + "s");

        MongoDBVariantWriteResult writeResult = variantWriter.getResult();
        writeResult.setSkippedVariants(skippedVariants);

        logger.info("Write result: {}", writeResult.toString());
        logger.info("Write result: {}", writeResult.toTSV());
        logger.info("Write result: {}", writeResult.toJson());
        options.put("writeResult", writeResult);
        return writeResult;
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageManagerException {
        return super.postLoad(input, output);
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
            exception = new StorageManagerException(message);
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
