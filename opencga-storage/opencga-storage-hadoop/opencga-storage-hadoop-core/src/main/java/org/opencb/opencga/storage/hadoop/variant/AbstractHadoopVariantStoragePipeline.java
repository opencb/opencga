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
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.biodata.tools.variant.stats.VariantSetStatsCalculator;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.core.io.ProtoFileWriter;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.plain.StringDataWriter;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.exceptions.StorageHadoopException;
import org.opencb.opencga.storage.hadoop.utils.HBaseLock;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHbaseTransformTask;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantToVcfSliceConverterTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.MERGE_MODE;
import static org.opencb.opencga.storage.hadoop.variant.GenomeHelper.PHOENIX_INDEX_LOCK_COLUMN;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.*;

/**
 * Created by mh719 on 13/05/2016.
 */
public abstract class AbstractHadoopVariantStoragePipeline extends VariantStoragePipeline {
    protected final VariantHadoopDBAdaptor dbAdaptor;
    protected final Configuration conf;
    protected final HBaseCredentials archiveTableCredentials;
    protected final HBaseCredentials variantsTableCredentials;
    protected MRExecutor mrExecutor = null;
    protected boolean loadArch;
    protected boolean loadVar;

    private final Logger logger = LoggerFactory.getLogger(AbstractHadoopVariantStoragePipeline.class);

    public AbstractHadoopVariantStoragePipeline(
            StorageConfiguration configuration,
            VariantHadoopDBAdaptor dbAdaptor,
            VariantReaderUtils variantReaderUtils, ObjectMap options,
            HBaseCredentials archiveCredentials, MRExecutor mrExecutor,
            Configuration conf) {
        super(configuration, STORAGE_ENGINE_ID, dbAdaptor, variantReaderUtils, options);
        this.archiveTableCredentials = archiveCredentials;
        this.mrExecutor = mrExecutor;
        this.dbAdaptor = dbAdaptor;
        this.variantsTableCredentials = dbAdaptor == null ? null : dbAdaptor.getCredentials();
        this.conf = new Configuration(conf);

        loadArch = this.options.getBoolean(HADOOP_LOAD_ARCHIVE, false);
        loadVar = this.options.getBoolean(HADOOP_LOAD_VARIANT, false);

        if (!loadArch && !loadVar) {
            loadArch = true;
            loadVar = true;
            this.options.put(HADOOP_LOAD_ARCHIVE, loadArch);
            this.options.put(HADOOP_LOAD_VARIANT, loadVar);
        }
    }

    @Override
    public URI preTransform(URI input) throws StorageEngineException, IOException, FileFormatException {
        logger.info("PreTransform: " + input);
//        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        if (!options.containsKey(VariantStorageEngine.Options.TRANSFORM_FORMAT.key())) {
            options.put(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(),
                    VariantStorageEngine.Options.TRANSFORM_FORMAT.defaultValue());
        }
        String transVal = options.getString(VariantStorageEngine.Options.TRANSFORM_FORMAT.key());
        switch (transVal){
            case "avro":
            case "proto":
                break;
            default:
                throw new NotImplementedException(String.format("Output format %s not supported for Hadoop!", transVal));
        }
        // non gVCF files are supported. Don't force all files to be gVCF
//        if (!options.containsKey(VariantStorageEngine.Options.GVCF.key())) {
//            options.put(VariantStorageEngine.Options.GVCF.key(), true);
//        }
//        boolean isGvcf = options.getBoolean(VariantStorageEngine.Options.GVCF.key());
//        if (!isGvcf) {
//            throw new NotImplementedException("Only GVCF format supported!!!");
//        }
        return super.preTransform(input);
    }

    @Override
    protected Pair<Long, Long> processProto(Path input, String fileName, Path output,
                                            VariantFileMetadata fileMetadata, Path outputVariantsFile,
                                            Path outputMetaFile, boolean includeSrc, String parser, boolean generateReferenceBlocks,
                                            int batchSize, String extension, String compression,
                                            BiConsumer<String, RuntimeException> malformatedHandler, boolean failOnError)
            throws StorageEngineException {

        //Writer
        DataWriter<VcfSliceProtos.VcfSlice> dataWriter = new ProtoFileWriter<>(outputVariantsFile, compression);

        // Normalizer
        VariantNormalizer normalizer = new VariantNormalizer(true, true, false);
        normalizer.configure(fileMetadata.getHeader());
        normalizer.setGenerateReferenceBlocks(generateReferenceBlocks);

        // Stats calculator
        VariantSetStatsCalculator statsCalculator = new VariantSetStatsCalculator(String.valueOf(getStudyId()), fileMetadata);

        final VariantReader dataReader;
        try {
            String studyId = String.valueOf(getStudyId());
            if (VariantReaderUtils.isVcf(input.toString())) {
                InputStream inputStream = FileUtils.newInputStream(input);

                VariantVcfHtsjdkReader reader = new VariantVcfHtsjdkReader(inputStream, fileMetadata.toVariantStudyMetadata(studyId),
                        normalizer);
                if (null != malformatedHandler) {
                    reader.registerMalformatedVcfHandler(malformatedHandler);
                    reader.setFailOnError(failOnError);
                }
                dataReader = reader;
            } else {
                dataReader = VariantReaderUtils.getVariantReader(input, fileMetadata.toVariantStudyMetadata(studyId));
            }
        } catch (IOException e) {
            throw new StorageEngineException("Unable to read from " + input, e);
        }

        // Transformer
        ArchiveTableHelper helper = new ArchiveTableHelper(conf, getStudyId(), fileMetadata);
        ProgressLogger progressLogger = new ProgressLogger("Transform proto:").setBatchSize(100000);

        logger.info("Generating output file {}", outputVariantsFile);

        long start = System.currentTimeMillis();
        long end;
        // FIXME
        if (options.getBoolean("transform.proto.parallel")) {
            VariantSliceReader sliceReader = new VariantSliceReader(helper.getChunkSize(), dataReader,
                    helper.getStudyId(), Integer.valueOf(helper.getFileMetadata().getId()));

            // Use a supplier to avoid concurrent modifications of non thread safe objects.
            Supplier<ParallelTaskRunner.TaskWithException<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice, ?>> supplier =
                    () -> new VariantToVcfSliceConverterTask(progressLogger);

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
                throw new StorageEngineException(String.format("Error while Transforming file %s into %s", input, outputVariantsFile), e);
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

//                fileMetadata.getMetadata().put(VariantFileUtils.VARIANT_FILE_HEADER, dataReader.getHeader());
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
                throw new StorageEngineException(String.format("Error while Transforming file %s into %s", input, outputVariantsFile), e);
            } finally {
                dataWriter.close();
                dataReader.close();
            }
        }

        ObjectMapper jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);

        ObjectWriter variantSourceObjectWriter = jsonObjectMapper.writerFor(VariantFileMetadata.class);
        try {
            String sourceJsonString = variantSourceObjectWriter.writeValueAsString(fileMetadata);
            StringDataWriter.write(outputMetaFile, Collections.singletonList(sourceJsonString));
        } catch (IOException e) {
            throw new StorageEngineException("Error writing meta file", e);
        }
        return new ImmutablePair<>(start, end);
    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageEngineException {

        if (loadArch) {
            super.preLoad(input, output);

            if (needLoadFromHdfs() && !input.getScheme().equals("hdfs")) {
                if (!StringUtils.isEmpty(options.getString(INTERMEDIATE_HDFS_DIRECTORY))) {
                    output = URI.create(options.getString(INTERMEDIATE_HDFS_DIRECTORY));
                }
                if (output.getScheme() != null && !output.getScheme().equals("hdfs")) {
                    throw new StorageEngineException("Output must be in HDFS");
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
            ArchiveTableHelper.createArchiveTableIfNeeded(dbAdaptor.getGenomeHelper(), archiveTableCredentials.getTable(),
                    dbAdaptor.getConnection());
        } catch (IOException e) {
            throw new StorageHadoopException("Issue creating table " + archiveTableCredentials.getTable(), e);
        }
        try {
            VariantTableHelper.createVariantTableIfNeeded(dbAdaptor.getGenomeHelper(), variantsTableCredentials.getTable(),
                    dbAdaptor.getConnection());
        } catch (IOException e) {
            throw new StorageHadoopException("Issue creating table " + variantsTableCredentials.getTable(), e);
        }

        if (loadVar) {
            preMerge(input);
        }

        return input;
    }

    @Override
    protected void securePreLoad(StudyConfiguration studyConfiguration, VariantFileMetadata fileMetadata) throws StorageEngineException {
        // Provided Extra fields
        List<String> providedExtraFields = getOptions().getAsStringList(EXTRA_GENOTYPE_FIELDS.key());

        super.securePreLoad(studyConfiguration, fileMetadata);

        if (!studyConfiguration.getAttributes().containsKey(MERGE_LOAD_SAMPLE_COLUMNS)) {
            boolean loadSampleColumns = getOptions().getBoolean(MERGE_LOAD_SAMPLE_COLUMNS, DEFAULT_MERGE_LOAD_SAMPLE_COLUMNS);
            studyConfiguration.getAttributes().put(MERGE_LOAD_SAMPLE_COLUMNS, loadSampleColumns);
        }
        MergeMode mergeMode;
        if (!studyConfiguration.getAttributes().containsKey(Options.MERGE_MODE.key())) {
            mergeMode = MergeMode.from(options);
            studyConfiguration.getAttributes().put(Options.MERGE_MODE.key(), mergeMode);
        } else {
            options.put(MERGE_MODE.key(), MergeMode.from(studyConfiguration.getAttributes()));
        }

        Stream<String> stream;
        // If ExtraGenotypeFields are provided by command line, check that those fields are going to be loaded.
        if (!providedExtraFields.isEmpty()) {
            stream = providedExtraFields.stream();
        } else {
            // Otherwise, add all format fields
            stream = fileMetadata.getHeader().getComplexLines()
                    .stream()
                    .filter(line -> line.getKey().equals("FORMAT"))
                    .map(VariantFileHeaderComplexLine::getId);
        }
        List<String> extraGenotypeFields = studyConfiguration.getAttributes().getAsStringList(EXTRA_GENOTYPE_FIELDS.key());
        stream.forEach(format -> {
            if (!extraGenotypeFields.contains(format) && !format.equals(VariantMerger.GT_KEY)) {
                extraGenotypeFields.add(format);
            }
        });
        studyConfiguration.getAttributes().put(EXTRA_GENOTYPE_FIELDS.key(), extraGenotypeFields);
        getOptions().put(EXTRA_GENOTYPE_FIELDS.key(), extraGenotypeFields);

    }

    @Override
    public void securePostLoad(List<Integer> fileIds, StudyConfiguration studyConfiguration) throws StorageEngineException {
        super.securePostLoad(fileIds, studyConfiguration);
        studyConfiguration.getAttributes().put(MISSING_GENOTYPES_UPDATED, false);
    }

    protected void preMerge(URI input) throws StorageEngineException {
        int studyId = getStudyId();

        long lock = dbAdaptor.getStudyConfigurationManager().lockStudy(studyId);

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        try {
            StudyConfiguration studyConfiguration = checkOrCreateStudyConfiguration(true);
            VariantFileMetadata fileMetadata = readVariantFileMetadata(input, options);
            securePreMerge(studyConfiguration, fileMetadata);
            dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
        } finally {
            dbAdaptor.getStudyConfigurationManager().unLockStudy(studyId, lock);
        }

    }

    protected void securePreMerge(StudyConfiguration studyConfiguration, VariantFileMetadata fileMetadata) throws StorageEngineException {

        if (loadVar) {
            // Load into variant table
            // Update the studyConfiguration with data from the Archive Table.
            // Reads the VcfMeta documents, and populates the StudyConfiguration if needed.
            // Obtain the list of pending files.

            int studyId = options.getInt(VariantStorageEngine.Options.STUDY_ID.key(), -1);
            int fileId = options.getInt(VariantStorageEngine.Options.FILE_ID.key(), -1);
            boolean missingFilesDetected = false;


            HadoopVariantFileMetadataDBAdaptor fileMetadataManager = dbAdaptor.getVariantFileMetadataDBAdaptor();
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
                VariantFileMetadata readFileMetadata;
                try {
                    readFileMetadata = fileMetadataManager.getVariantFileMetadata(studyId, loadedFileId, null);
                } catch (IOException e) {
                    throw new StorageHadoopException("Unable to read file VcfMeta for file : " + loadedFileId, e);
                }

                Integer readFileId = Integer.parseInt(readFileMetadata.getId());
                logger.debug("Found fileMetadata for file id {} with registered id {} ", loadedFileId, readFileId);
                if (!studyConfiguration.getFileIds().inverse().containsKey(readFileId)) {
                    StudyConfigurationManager.checkNewFile(studyConfiguration, readFileId, readFileMetadata.getPath());
                    studyConfiguration.getFileIds().put(readFileMetadata.getPath(), readFileId);
//                    studyConfiguration.getHeaders().put(readFileId, readFileMetadata.getMetadata()
//                            .get(VariantFileUtils.VARIANT_FILE_HEADER).toString());
                    StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, readFileId, readFileMetadata, options);
                    missingFilesDetected = true;
                }
                if (!studyConfiguration.getIndexedFiles().contains(readFileId)) {
                    pendingFiles.add(readFileId);
                }
            }
            logger.info("Found pending in DB: " + pendingFiles);

            fileId = StudyConfigurationManager.checkNewFile(studyConfiguration, fileId, fileMetadata.getPath());

            if (!loadArch) {
                //If skip archive loading, input fileId must be already in archiveTable, so "pending to be loaded"
                if (!pendingFiles.contains(fileId)) {
                    throw new StorageEngineException("File " + fileId + " is not loaded in archive table "
                            + getArchiveTableName(studyId, options) + "");
                }
            } else {
                //If don't skip archive, input fileId must not be pending, because must not be in the archive table.
                if (pendingFiles.contains(fileId)) {
                    // set loadArch to false?
                    throw new StorageEngineException("File " + fileId + " is not loaded in archive table");
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
                        throw new StorageEngineException("File " + pendingFile + " is not pending to be loaded in variant table");
                    }
                }
                pendingFiles = givenPendingFiles;
            } else {
                options.put(HADOOP_LOAD_VARIANT_PENDING_FILES, pendingFiles);
            }

            boolean resume = options.getBoolean(Options.RESUME.key(), Options.RESUME.defaultValue())
                    || options.getBoolean(HadoopVariantStorageEngine.HADOOP_LOAD_VARIANT_RESUME, false);
            long timestamp = studyConfiguration.getBatches().stream().map(BatchFileOperation::getTimestamp).max(Long::compareTo).orElse(0L);
            BatchFileOperation op = StudyConfigurationManager.addBatchOperation(
                    studyConfiguration, VariantTableDriver.JOB_OPERATION_NAME, pendingFiles, resume,
                    BatchFileOperation.Type.LOAD);
            // Overwrite default timestamp. Use custom monotonic timestamp. (lastTimestamp + 1)
            if (op.getTimestamp() > timestamp) {
                op.setTimestamp(timestamp + 1);
            }
            options.put(HADOOP_LOAD_VARIANT_STATUS, op.currentStatus());
            options.put(AbstractAnalysisTableDriver.TIMESTAMP, op.getTimestamp());

        }
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
    public URI load(URI input) throws IOException, StorageEngineException {
        int studyId = getStudyId();
        int fileId = options.getInt(Options.FILE_ID.key());

        ArchiveTableHelper.setChunkSize(conf, conf.getInt(ARCHIVE_CHUNK_SIZE, DEFAULT_ARCHIVE_CHUNK_SIZE));
        ArchiveTableHelper.setStudyId(conf, studyId);

        if (loadArch) {
            Set<Integer> loadedFiles = dbAdaptor.getVariantFileMetadataDBAdaptor().getLoadedFiles(studyId);
            if (!loadedFiles.contains(fileId)) {
                loadArch(input);
            } else {
                logger.info("File {} already loaded in archive table. Skip this step!",
                        Paths.get(input.getPath()).getFileName().toString());
            }
        }

        if (loadVar) {
            List<Integer> pendingFiles = options.getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES);
            merge(studyId, pendingFiles);
        }

        return input; // TODO  change return value?
    }

    protected abstract void loadArch(URI input) throws StorageEngineException;

    public void merge(int studyId, List<Integer> pendingFiles) throws StorageEngineException {
        // Check if status is "DONE"
        if (options.get(HADOOP_LOAD_VARIANT_STATUS, BatchFileOperation.Status.class).equals(BatchFileOperation.Status.DONE)) {
            // Merge operation status : DONE, not READY or RUNNING
            // Don't need to merge again. Skip merge and run post-load/post-merge step
            logger.info("Files {} already merged!", pendingFiles);
            return;
        }
        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jar = getJarWithDependencies();
        options.put(HADOOP_LOAD_VARIANT_PENDING_FILES, pendingFiles);

        Class execClass = VariantTableDriver.class;
        String args = VariantTableDriver.buildCommandLineArgs(
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
                throw new StorageEngineException("Error loading files " + pendingFiles + " into variant table \""
                        + variantsTableCredentials.getTable() + "\"");
            }
            getStudyConfigurationManager()
                    .atomicSetStatus(getStudyId(), BatchFileOperation.Status.DONE, VariantTableDriver.JOB_OPERATION_NAME, pendingFiles);
        } catch (Exception e) {
            getStudyConfigurationManager()
                    .atomicSetStatus(getStudyId(), BatchFileOperation.Status.ERROR, VariantTableDriver.JOB_OPERATION_NAME, pendingFiles);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    public String getJarWithDependencies() throws StorageEngineException {
        return HadoopVariantStorageEngine.getJarWithDependencies(options);
    }

    @Override
    protected void checkLoadedVariants(int fileId, StudyConfiguration studyConfiguration) throws
            StorageEngineException {
        logger.warn("Skip check loaded variants");
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageEngineException {
        if (loadArch) {
            logger.debug("Nothing to do");
        }

        if (loadVar) {
            return postMerge(input, output);
        }
        return input;
    }

    public URI postMerge(URI input, URI output) throws StorageEngineException {
        final List<Integer> fileIds = getLoadedFiles();
        if (fileIds.isEmpty()) {
            logger.debug("Skip post load");
            return input;
        }

        registerLoadedFiles(fileIds);

        dbAdaptor.getStudyConfigurationManager().lockAndUpdate(getStudyId(), studyConfiguration -> {
            securePostMerge(fileIds, studyConfiguration);
            return studyConfiguration;
        });

        return input;
    }

    protected void registerLoadedFiles(List<Integer> fileIds) throws StorageEngineException {
        // Current StudyConfiguration may be outdated. Force fetch.
        StudyConfiguration studyConfiguration = getStudyConfiguration(true);

        VariantPhoenixHelper phoenixHelper = new VariantPhoenixHelper(dbAdaptor.getGenomeHelper());

        String tableName = variantsTableCredentials.getTable();
        try (Connection jdbcConnection = phoenixHelper.newJdbcConnection()) {
            HBaseLock hBaseLock = new HBaseLock(dbAdaptor.getHBaseManager(), tableName,
                    dbAdaptor.getGenomeHelper().getColumnFamily(),
                    VariantPhoenixKeyFactory.generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0));
            Long lock;
            try {
                long lockDuration = TimeUnit.MINUTES.toMillis(5);
                try {
                    lock = hBaseLock.lock(GenomeHelper.PHOENIX_LOCK_COLUMN, lockDuration, TimeUnit.SECONDS.toMillis(5));
                } catch (TimeoutException e) {
                    int duration = 10;
                    logger.info("Waiting to get Lock over HBase table {} up to {} minutes ...", tableName, duration);
                    lock = hBaseLock.lock(GenomeHelper.PHOENIX_LOCK_COLUMN, lockDuration, TimeUnit.MINUTES.toMillis(duration));
                }
                logger.debug("Winning lock {}", lock);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StorageEngineException("Error locking table to modify Phoenix columns!", e);
            } catch (TimeoutException | IOException e) {
                throw new StorageEngineException("Error locking table to modify Phoenix columns!", e);
            }

            try {
                if (MergeMode.from(studyConfiguration.getAttributes()).equals(MergeMode.ADVANCED)) {
                    try {
                        phoenixHelper.registerNewStudy(jdbcConnection, tableName, studyConfiguration.getStudyId());
                    } catch (SQLException e) {
                        throw new StorageEngineException("Unable to register study in Phoenix", e);
                    }
                }

                if (studyConfiguration.getAttributes().getBoolean(HadoopVariantStorageEngine.MERGE_LOAD_SAMPLE_COLUMNS)) {
                    try {
                        Set<Integer> previouslyIndexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration).values();
                        Set<Integer> newSamples = new HashSet<>();
                        for (Integer fileId : fileIds) {
                            for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fileId)) {
                                if (!previouslyIndexedSamples.contains(sampleId)) {
                                    newSamples.add(sampleId);
                                }
                            }
                        }
                        phoenixHelper.registerNewFiles(jdbcConnection, tableName, studyConfiguration.getStudyId(), fileIds, newSamples);

                    } catch (SQLException e) {
                        throw new StorageEngineException("Unable to register samples in Phoenix", e);
                    }
                }
            } finally {
                try {
                    hBaseLock.unlock(GenomeHelper.PHOENIX_LOCK_COLUMN, lock);
                } catch (HBaseLock.IllegalLockStatusException e) {
                    logger.warn(e.getMessage());
                    logger.debug(e.getMessage(), e);
                }
            }

            if (!options.getBoolean(VARIANT_TABLE_INDEXES_SKIP, false)) {
                try {
                    lock = hBaseLock.lock(PHOENIX_INDEX_LOCK_COLUMN, TimeUnit.MINUTES.toMillis(60), TimeUnit.SECONDS.toMillis(5));
                    if (options.getString(VariantAnnotationManager.SPECIES, "hsapiens").equalsIgnoreCase("hsapiens")) {
                        List<PhoenixHelper.Column> columns = VariantPhoenixHelper.getHumanPopulationFrequenciesColumns();
                        phoenixHelper.getPhoenixHelper().addMissingColumns(jdbcConnection, tableName, columns, true);
                        List<PhoenixHelper.Index> popFreqIndices = VariantPhoenixHelper.getPopFreqIndices(tableName);
                        phoenixHelper.getPhoenixHelper().createIndexes(jdbcConnection, tableName, popFreqIndices, false);
                    }
                    phoenixHelper.createVariantIndexes(jdbcConnection, tableName);
                } catch (SQLException e) {
                    throw new StorageEngineException("Unable to create Phoenix Indexes", e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StorageEngineException("Unable to create Phoenix Indexes", e);
                } catch (TimeoutException e) {
                    // Indices are been created by another instance. Don't need to create twice.
                    logger.info("Unable to get lock to create PHOENIX INDICES. Already been created by another instance. "
                            + "Skip create indexes!");
                    lock = null;
                } finally {
                    if (lock != null) {
                        hBaseLock.unlock(PHOENIX_INDEX_LOCK_COLUMN, lock);
                    }
                }
            } else {
                logger.info("Skip create indexes!!");
            }

        } catch (SQLException | ClassNotFoundException | IOException e) {
            throw new StorageEngineException("Error with Phoenix connection", e);
        }

        // This method checks the loaded variants (if possible) and adds the loaded files to the studyConfiguration
        super.postLoad(null, null);
    }

    protected List<Integer> getLoadedFiles() {
        List<Integer> fileIds;
        if (options.getBoolean(HADOOP_LOAD_VARIANT)) {
            fileIds = options.getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES);
            options.put(Options.FILE_ID.key(), fileIds);
        } else {
            fileIds = Collections.emptyList();
        }
        return fileIds;
    }

    protected void securePostMerge(List<Integer> fileIds, StudyConfiguration studyConfiguration) {
        BatchFileOperation.Status status = dbAdaptor.getStudyConfigurationManager()
                .setStatus(studyConfiguration, BatchFileOperation.Status.READY, VariantTableDriver.JOB_OPERATION_NAME, fileIds);
        if (status != BatchFileOperation.Status.DONE) {
            logger.warn("Unexpected status " + status);
        }
    }

}
