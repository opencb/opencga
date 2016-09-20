package org.opencb.opencga.storage.hadoop.variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantNormalizer;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.VariantFileUtils;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.biodata.tools.variant.converter.VariantToVcfSliceConverter;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.core.io.ProtoFileWriter;
import org.opencb.opencga.core.common.ProgressLogger;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.VariantStorageETL;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.json.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.exceptions.StorageHadoopException;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHbaseTransformTask;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager.*;

/**
 * Created by mh719 on 13/05/2016.
 */
public abstract class AbstractHadoopVariantStorageETL extends VariantStorageETL {
    protected final VariantHadoopDBAdaptor dbAdaptor;
    protected final Configuration conf;
    protected final HBaseCredentials archiveTableCredentials;
    protected final HBaseCredentials variantsTableCredentials;
    protected MRExecutor mrExecutor = null;

    public AbstractHadoopVariantStorageETL(
            StorageConfiguration configuration, String storageEngineId, Logger logger,
            VariantHadoopDBAdaptor dbAdaptor,
            VariantReaderUtils variantReaderUtils, ObjectMap options,
            HBaseCredentials archiveCredentials, MRExecutor mrExecutor,
            Configuration conf) {
        super(configuration, storageEngineId, logger, dbAdaptor, variantReaderUtils, options);
        this.archiveTableCredentials = archiveCredentials;
        this.mrExecutor = mrExecutor;
        this.dbAdaptor = dbAdaptor;
        this.variantsTableCredentials = dbAdaptor == null ? null : dbAdaptor.getCredentials();
        this.conf = new Configuration(conf);
    }

    @Override
    public URI preTransform(URI input) throws StorageManagerException, IOException, FileFormatException {
        logger.info("PreTransform: " + input);
//        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        if (!options.containsKey(VariantStorageManager.Options.TRANSFORM_FORMAT.key())) {
            options.put(VariantStorageManager.Options.TRANSFORM_FORMAT.key(),
                    VariantStorageManager.Options.TRANSFORM_FORMAT.defaultValue());
        }
        String transVal = options.getString(VariantStorageManager.Options.TRANSFORM_FORMAT.key());
        switch (transVal){
            case "avro":
            case "proto":
                break;
            default:
                throw new NotImplementedException(String.format("Output format %s not supported for Hadoop!", transVal));
        }
        if (!options.containsKey(VariantStorageManager.Options.GVCF.key())) {
            options.put(VariantStorageManager.Options.GVCF.key(), true);
        }
        boolean isGvcf = options.getBoolean(VariantStorageManager.Options.GVCF.key());
        if (!isGvcf) {
            throw new NotImplementedException("Only GVCF format supported!!!");
        }
        return super.preTransform(input);
    }

    @Override
    protected Pair<Long, Long> processProto(Path input, String fileName, Path output, VariantSource source, Path outputVariantsFile,
                                            Path outputMetaFile, boolean includeSrc, String parser, boolean generateReferenceBlocks,
                                            int batchSize, String extension, String compression,
                                            BiConsumer<String, RuntimeException> malformatedHandler, boolean failOnError)
            throws StorageManagerException {

        //Writer
        DataWriter<VcfSliceProtos.VcfSlice> dataWriter = new ProtoFileWriter<>(outputVariantsFile, compression);

        // Normalizer
        VariantNormalizer normalizer = new VariantNormalizer();
        normalizer.setGenerateReferenceBlocks(generateReferenceBlocks);

        // Stats calculator
        VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(source);

        VariantReader dataReader = null;
        try {
            if (VariantReaderUtils.isVcf(input.toString())) {
                InputStream inputStream = FileUtils.newInputStream(input);

                VariantVcfHtsjdkReader reader = new VariantVcfHtsjdkReader(inputStream, source, normalizer);
                if (null != malformatedHandler) {
                    reader.registerMalformatedVcfHandler(malformatedHandler);
                    reader.setFailOnError(failOnError);
                }
                dataReader = reader;
            } else {
                dataReader = VariantReaderUtils.getVariantReader(input, source);
            }
        } catch (IOException e) {
            throw new StorageManagerException("Unable to read from " + input, e);
        }

        // Transformer
        VcfMeta meta = new VcfMeta(source);
        ArchiveHelper helper = new ArchiveHelper(conf, meta);
        ProgressLogger progressLogger = new ProgressLogger("Transform proto:").setBatchSize(100000);

        logger.info("Generating output file {}", outputVariantsFile);

        long start = System.currentTimeMillis();
        long end;
        // FIXME
        if (options.getBoolean("transform.proto.parallel")) {
            VariantSliceReader sliceReader = new VariantSliceReader(helper.getChunkSize(), dataReader);

            // Use a supplier to avoid concurrent modifications of non thread safe objects.
            Supplier<ParallelTaskRunner.TaskWithException<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice, ?>> supplier =
                    () -> {
                        VariantToVcfSliceConverter converter = new VariantToVcfSliceConverter();
                        return batch -> {
                            List<VcfSliceProtos.VcfSlice> slices = new ArrayList<>(batch.size());
                            for (ImmutablePair<Long, List<Variant>> pair : batch) {
                                slices.add(converter.convert(pair.getRight(), pair.getLeft().intValue()));
                                progressLogger.increment(pair.getRight().size());
                            }
                            return slices;
                        };
                    };

            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                    .setNumTasks(options.getInt(Options.TRANSFORM_THREADS.key(), 1))
                    .setBatchSize(1)
                    .setAbortOnFail(true)
                    .setSorted(false)
                    .setCapacity(1)
                    .build();

            ParallelTaskRunner<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice> ptr;
            ptr = new ParallelTaskRunner<>(sliceReader, supplier, dataWriter, config);

            try {
                ptr.run();
            } catch (ExecutionException e) {
                throw new StorageManagerException(String.format("Error while Transforming file %s into %s", input, outputVariantsFile), e);
            }
            end = System.currentTimeMillis();
        } else {
            VariantHbaseTransformTask transformTask = new VariantHbaseTransformTask(helper, null);
            long[] t = new long[]{0, 0, 0};
            long last = System.nanoTime();

            try {
                dataReader.open();
                dataReader.pre();
                dataWriter.open();
                dataWriter.pre();
                transformTask.pre();
                statsCalculator.pre();

                start = System.currentTimeMillis();
                last = System.nanoTime();
                // Process data
                List<Variant> read = dataReader.read(batchSize);
                t[0] += System.nanoTime() - last;
                last = System.nanoTime();
                while (!read.isEmpty()) {
                    progressLogger.increment(read.size());
                    statsCalculator.apply(read);
                    List<VcfSliceProtos.VcfSlice> slices = transformTask.apply(read);
                    t[1] += System.nanoTime() - last;
                    last = System.nanoTime();
                    dataWriter.write(slices);
                    t[2] += System.nanoTime() - last;
                    last = System.nanoTime();
                    read = dataReader.read(batchSize);
                    t[0] += System.nanoTime() - last;
                    last = System.nanoTime();
                }
                List<VcfSliceProtos.VcfSlice> drain = transformTask.drain();
                t[1] += System.nanoTime() - last;
                last = System.nanoTime();
                dataWriter.write(drain);
                t[2] += System.nanoTime() - last;

                end = System.currentTimeMillis();

                source.getMetadata().put(VariantFileUtils.VARIANT_FILE_HEADER, dataReader.getHeader());
                statsCalculator.post();
                transformTask.post();
                dataReader.post();
                dataWriter.post();

                end = System.currentTimeMillis();
                logger.info("Times for reading: {}, transforming {}, writing {}",
                        TimeUnit.NANOSECONDS.toSeconds(t[0]),
                        TimeUnit.NANOSECONDS.toSeconds(t[1]),
                        TimeUnit.NANOSECONDS.toSeconds(t[2]));
            } catch (Exception e) {
                throw new StorageManagerException(String.format("Error while Transforming file %s into %s", input, outputVariantsFile), e);
            } finally {
                dataWriter.close();
                dataReader.close();
            }
        }

        ObjectMapper jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);

        ObjectWriter variantSourceObjectWriter = jsonObjectMapper.writerFor(VariantSource.class);
        try {
            String sourceJsonString = variantSourceObjectWriter.writeValueAsString(source);
            StringDataWriter.write(outputMetaFile, Collections.singletonList(sourceJsonString));
        } catch (IOException e) {
            throw new StorageManagerException("Error writing meta file", e);
        }
        return new ImmutablePair<>(start, end);
    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageManagerException {
        boolean loadArch = options.getBoolean(HADOOP_LOAD_ARCHIVE);
        boolean loadVar = options.getBoolean(HADOOP_LOAD_VARIANT);

        if (!loadArch && !loadVar) {
            loadArch = true;
            loadVar = true;
            options.put(HADOOP_LOAD_ARCHIVE, loadArch);
            options.put(HADOOP_LOAD_VARIANT, loadVar);
        }

        if (loadArch) {
            super.preLoad(input, output);

            if (needLoadFromHdfs() && !input.getScheme().equals("hdfs")) {
                if (!StringUtils.isEmpty(options.getString(OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY))) {
                    output = URI.create(options.getString(OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY));
                }
                if (output.getScheme() != null && !output.getScheme().equals("hdfs")) {
                    throw new StorageManagerException("Output must be in HDFS");
                }

                try {
                    long startTime = System.currentTimeMillis();
//                    Configuration conf = getHadoopConfiguration(options);
                    FileSystem fs = FileSystem.get(conf);
                    org.apache.hadoop.fs.Path variantsOutputPath = new org.apache.hadoop.fs.Path(
                            output.resolve(Paths.get(input.getPath()).getFileName().toString()));
                    logger.info("Copy from {} to {}", new org.apache.hadoop.fs.Path(input).toUri(), variantsOutputPath.toUri());
                    fs.copyFromLocalFile(false, new org.apache.hadoop.fs.Path(input), variantsOutputPath);
                    logger.info("Copied to hdfs in {}s", (System.currentTimeMillis() - startTime) / 1000.0);

                    startTime = System.currentTimeMillis();
                    URI fileInput = URI.create(VariantReaderUtils.getMetaFromTransformedFile(input.toString()));
                    org.apache.hadoop.fs.Path fileOutputPath = new org.apache.hadoop.fs.Path(
                            output.resolve(Paths.get(fileInput.getPath()).getFileName().toString()));
                    logger.info("Copy from {} to {}", new org.apache.hadoop.fs.Path(fileInput).toUri(), fileOutputPath.toUri());
                    fs.copyFromLocalFile(false, new org.apache.hadoop.fs.Path(fileInput), fileOutputPath);
                    logger.info("Copied to hdfs in {}s", (System.currentTimeMillis() - startTime) / 1000.0);

                    input = variantsOutputPath.toUri();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            ArchiveDriver.createArchiveTableIfNeeded(dbAdaptor.getGenomeHelper(), archiveTableCredentials.getTable(),
                    dbAdaptor.getConnection());
        } catch (IOException e) {
            throw new StorageHadoopException("Issue creating table " + archiveTableCredentials.getTable(), e);
        }
        try {
            VariantTableDriver.createVariantTableIfNeeded(dbAdaptor.getGenomeHelper(), variantsTableCredentials.getTable(),
                    dbAdaptor.getConnection());
        } catch (IOException e) {
            throw new StorageHadoopException("Issue creating table " + variantsTableCredentials.getTable(), e);
        }

        if (loadVar) {
            preMerge(input);
        }

        return input;
    }

    protected void preMerge(URI input) throws StorageManagerException {
        int studyId = getStudyId();
        long lock = dbAdaptor.getStudyConfigurationManager().lockStudy(studyId);

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        try {
            StudyConfiguration studyConfiguration = checkOrCreateStudyConfiguration(true);
            VariantSource source = readVariantSource(input, options);
            securePreMerge(studyConfiguration, source);
            dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
        } finally {
            dbAdaptor.getStudyConfigurationManager().unLockStudy(studyId, lock);
        }

    }

    protected void securePreMerge(StudyConfiguration studyConfiguration, VariantSource source) throws StorageManagerException {

        boolean loadArch = options.getBoolean(HADOOP_LOAD_ARCHIVE);
        boolean loadVar = options.getBoolean(HADOOP_LOAD_VARIANT);

        if (loadVar) {
            // Load into variant table
            // Update the studyConfiguration with data from the Archive Table.
            // Reads the VcfMeta documents, and populates the StudyConfiguration if needed.
            // Obtain the list of pending files.

            int studyId = options.getInt(VariantStorageManager.Options.STUDY_ID.key(), -1);
            int fileId = options.getInt(VariantStorageManager.Options.FILE_ID.key(), -1);
            boolean missingFilesDetected = false;


            HadoopVariantSourceDBAdaptor fileMetadataManager = dbAdaptor.getVariantSourceDBAdaptor();
            Set<Integer> files = null;
            try {
                files = fileMetadataManager.getLoadedFiles(studyId);
            } catch (IOException e) {
                throw new StorageHadoopException("Unable to read loaded files", e);
            }

            logger.info("Found files in Archive DB: " + files);

            // Pending files, not in analysis but in archive.
            List<Integer> pendingFiles = new LinkedList<>();
            logger.info("Found registered indexed files: {}", studyConfiguration.getIndexedFiles());
            for (Integer loadedFileId : files) {
                VariantSource readSource;
                try {
                    readSource = fileMetadataManager.getVariantSource(studyId, loadedFileId, null);
                } catch (IOException e) {
                    throw new StorageHadoopException("Unable to read file VcfMeta for file : " + loadedFileId, e);
                }

                Integer readFileId = Integer.parseInt(readSource.getFileId());
                logger.info("Found source for file id {} with registered id {} ", loadedFileId, readFileId);
                if (!studyConfiguration.getFileIds().inverse().containsKey(readFileId)) {
                    checkNewFile(studyConfiguration, readFileId, readSource.getFileName());
                    studyConfiguration.getFileIds().put(readSource.getFileName(), readFileId);
                    studyConfiguration.getHeaders().put(readFileId, readSource.getMetadata()
                            .get(VariantFileUtils.VARIANT_FILE_HEADER).toString());
                    checkAndUpdateStudyConfiguration(studyConfiguration, readFileId, readSource, options);
                    missingFilesDetected = true;
                }
                if (!studyConfiguration.getIndexedFiles().contains(readFileId)) {
                    pendingFiles.add(readFileId);
                }
            }

//            //VariantSource source = readVariantSource(input, options);
            fileId = checkNewFile(studyConfiguration, fileId, source.getFileName());


            logger.info("Found pending in DB: " + pendingFiles);
//            if (missingFilesDetected) {
//            }

            if (!loadArch) {
                //If skip archive loading, input fileId must be already in archiveTable, so "pending to be loaded"
                if (!pendingFiles.contains(fileId)) {
                    throw new StorageManagerException("File " + fileId + " is not loaded in archive table "
                            + getArchiveTableName(studyId, options) + "");
                }
            } else {
                //If don't skip archive, input fileId must not be pending, because must not be in the archive table.
                if (pendingFiles.contains(fileId)) {
                    // set loadArch to false?
                    throw new StorageManagerException("File " + fileId + " is not loaded in archive table");
                } else {
                    pendingFiles.add(fileId);
                }
            }

            //If there are some given pending files, load only those files, not all pending files
            List<Integer> givenPendingFiles = options.getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES);
            if (!givenPendingFiles.isEmpty()) {
                logger.info("Given Pending file list: " + givenPendingFiles);
                for (Integer pendingFile : givenPendingFiles) {
                    if (!pendingFiles.contains(pendingFile)) {
                        throw new StorageManagerException("File " + pendingFile + " is not pending to be loaded in variant table");
                    }
                }
                pendingFiles = givenPendingFiles;
            } else {
                options.put(HADOOP_LOAD_VARIANT_PENDING_FILES, pendingFiles);
            }

            boolean resume = conf.getBoolean(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT_RESUME, false);
            BatchFileOperation op = addBatchOperation(studyConfiguration, VariantTableDriver.JOB_OPERATION_NAME, pendingFiles, resume);

            options.put(AbstractVariantTableDriver.TIMESTAMP, op.getTimestamp());

        }
    }

    /**
     * Adds a new {@BatchOperation} to the StudyConfiguration.
     *
     * Only allow one running operation at the same time
     * If the last operation is ready, continue
     * If the last operation is in ERROR, continue if is the same operation and files.
     * If the last operation is running, continue only if resume=true
     *
     * If is a new operation, increment the TimeStamp
     *
     * @param studyConfiguration StudyConfiguration
     * @param jobOperationName   Job operation name used to create the jobName and as {@link BatchFileOperation#operationName}
     * @param fileIds            Files to be processed in this batch.
     * @param resume             Resume operation. Assume that previous operation went wrong.
     * @return                   The current batchOperation
     * @throws StorageManagerException if the operation can't be executed
     */
    protected BatchFileOperation addBatchOperation(StudyConfiguration studyConfiguration, String jobOperationName, List<Integer> fileIds,
                                                   boolean resume)
            throws StorageManagerException {

        List<BatchFileOperation> batches = studyConfiguration.getBatches();
        BatchFileOperation batchFileOperation;
        boolean newOperation = false;
        if (!batches.isEmpty()) {
            batchFileOperation = batches.get(batches.size() - 1);
            BatchFileOperation.Status currentStatus = batchFileOperation.currentStatus();

            switch (currentStatus) {
                case READY:
                    batchFileOperation = new BatchFileOperation(jobOperationName, fileIds, batchFileOperation.getTimestamp() + 1);
                    newOperation = true;
                    break;
                case DONE:
                case RUNNING:
                    if (!resume) {
                        throw new StorageHadoopException("Unable to process a new batch. Ongoing batch operation: "
                                + batchFileOperation);
                    }
                    // DO NOT BREAK!. Resuming last loading, go to error case.
                case ERROR:
                    Collections.sort(fileIds);
                    Collections.sort(batchFileOperation.getFileIds());
                    if (batchFileOperation.getFileIds().equals(fileIds)) {
                        logger.info("Resuming Last batch loading due to error.");
                    } else {
                        throw new StorageHadoopException("Unable to resume last batch operation. "
                                + "Must have the same files from the previous batch: " + batchFileOperation);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Status " + currentStatus);
            }
        } else {
            batchFileOperation = new BatchFileOperation(jobOperationName, fileIds, 1);
            newOperation = true;
        }
        batchFileOperation.addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.RUNNING);
        if (newOperation) {
            batches.add(batchFileOperation);
        }
        return batchFileOperation;
    }

    /**
     * Specify if the current class needs to move the file to load to HDFS.
     *
     * If true, the transformed file will be copied to hdfs during the {@link #preLoad}
     *
     * @return boolean
     */
    protected abstract boolean needLoadFromHdfs();

    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        int studyId = getStudyId();

        boolean loadArch = options.getBoolean(HADOOP_LOAD_ARCHIVE);
        boolean loadVar = options.getBoolean(HADOOP_LOAD_VARIANT);

        ArchiveHelper.setChunkSize(conf, conf.getInt(ArchiveDriver.CONFIG_ARCHIVE_CHUNK_SIZE, ArchiveDriver.DEFAULT_CHUNK_SIZE));
        ArchiveHelper.setStudyId(conf, studyId);

        if (loadArch) {
            loadArch(input);
        }

        if (loadVar) {
            List<Integer> pendingFiles = options.getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES);
            merge(studyId, pendingFiles);
        }

        return input; // TODO  change return value?
    }

    protected abstract void loadArch(URI input) throws StorageManagerException;

    public void merge(int studyId, List<Integer> pendingFiles) throws StorageManagerException {
        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jar = getJarWithDependencies();
        options.put(HADOOP_LOAD_VARIANT_PENDING_FILES, pendingFiles);

        Class execClass = VariantTableDriver.class;
        String args = VariantTableDriver.buildCommandLineArgs(variantsTableCredentials.getHostUri().toString(),
                archiveTableCredentials.getTable(),
                variantsTableCredentials.getTable(), studyId, pendingFiles, options);
        String executable = hadoopRoute + " jar " + jar + ' ' + execClass.getName();

        long startTime = System.currentTimeMillis();
        Thread hook = newShutdownHook(VariantTableDriver.JOB_OPERATION_NAME, pendingFiles);
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            logger.info("------------------------------------------------------");
            logger.info("Loading files {} into analysis table '{}'", pendingFiles, variantsTableCredentials.getTable());
            logger.info(executable + " " + args);
            logger.info("------------------------------------------------------");
            int exitValue = mrExecutor.run(executable, args);
            logger.info("------------------------------------------------------");
            logger.info("Exit value: {}", exitValue);
            logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
            if (exitValue != 0) {
                throw new StorageManagerException("Error loading files " + pendingFiles + " into variant table \""
                        + variantsTableCredentials.getTable() + "\"");
            }
            setStatus(BatchFileOperation.Status.DONE, VariantTableDriver.JOB_OPERATION_NAME, pendingFiles);
        } catch (Exception e) {
            setStatus(BatchFileOperation.Status.ERROR, VariantTableDriver.JOB_OPERATION_NAME, pendingFiles);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    public String getJarWithDependencies() throws StorageManagerException {
        return getJarWithDependencies(options);
    }

    public static String getJarWithDependencies(ObjectMap options) throws StorageManagerException {
        String jar = options.getString(OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES, null);
        if (jar == null) {
            throw new StorageManagerException("Missing option " + OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES);
        }
        if (!Paths.get(jar).isAbsolute()) {
            jar = System.getProperty("app.home", "") + "/" + jar;
        }
        return jar;
    }

    @Override
    protected void checkLoadedVariants(URI input, int fileId, StudyConfiguration studyConfiguration, ObjectMap options) throws
            StorageManagerException {
        logger.warn("Skip check loaded variants");
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageManagerException {
        if (options.getBoolean(HADOOP_LOAD_VARIANT)) {
            // Current StudyConfiguration may be outdated. Remove it.
            options.remove(VariantStorageManager.Options.STUDY_CONFIGURATION.key());

//            HadoopCredentials dbCredentials = getDbCredentials();
//            VariantHadoopDBAdaptor dbAdaptor = getDBAdaptor(dbCredentials);
            int studyId = getStudyId();

            VariantPhoenixHelper phoenixHelper = new VariantPhoenixHelper(dbAdaptor.getGenomeHelper());
            try {
                phoenixHelper.registerNewStudy(dbAdaptor.getJdbcConnection(), variantsTableCredentials.getTable(), studyId);
            } catch (SQLException e) {
                throw new StorageManagerException("Unable to register study in Phoenix", e);
            }

            options.put(VariantStorageManager.Options.FILE_ID.key(), options.getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES));

            return super.postLoad(input, output);
        } else {
            System.out.println(Thread.currentThread().getName() + " - DO NOTHING!");
            return input;
        }
    }

    @Override
    public void securePostLoad(List<Integer> fileIds, StudyConfiguration studyConfiguration) throws StorageManagerException {
        super.securePostLoad(fileIds, studyConfiguration);
        BatchFileOperation.Status status = secureSetStatus(studyConfiguration, BatchFileOperation.Status.READY,
                VariantTableDriver.JOB_OPERATION_NAME, fileIds);
        if (status != BatchFileOperation.Status.DONE) {
            logger.warn("Unexpected status " + status);
        }
    }

}
