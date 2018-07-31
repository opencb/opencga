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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.schema.PTableType;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.biodata.tools.variant.stats.VariantSetStatsCalculator;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.core.io.ProtoFileWriter;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.plain.StringDataWriter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.exceptions.StorageHadoopException;
import org.opencb.opencga.storage.hadoop.utils.HBaseLock;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHbaseTransformTask;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.MERGE_MODE;
import static org.opencb.opencga.storage.hadoop.variant.GenomeHelper.PHOENIX_INDEX_LOCK_COLUMN;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.*;

/**
 * Created by mh719 on 13/05/2016.
 */
public abstract class HadoopVariantStoragePipeline extends VariantStoragePipeline {
    protected final VariantHadoopDBAdaptor dbAdaptor;
    protected final Configuration conf;
    protected final HBaseCredentials variantsTableCredentials;
    protected MRExecutor mrExecutor = null;

    private final Logger logger = LoggerFactory.getLogger(HadoopVariantStoragePipeline.class);

    public HadoopVariantStoragePipeline(
            StorageConfiguration configuration,
            VariantHadoopDBAdaptor dbAdaptor,
            VariantReaderUtils variantReaderUtils, ObjectMap options,
            MRExecutor mrExecutor,
            Configuration conf) {
        super(configuration, STORAGE_ENGINE_ID, dbAdaptor, variantReaderUtils, options);
        this.mrExecutor = mrExecutor;
        this.dbAdaptor = dbAdaptor;
        this.variantsTableCredentials = dbAdaptor == null ? null : dbAdaptor.getCredentials();
        this.conf = new Configuration(conf);

    }

    @Override
    public VariantHadoopDBAdaptor getDBAdaptor() {
        return dbAdaptor;
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
        if (options.getBoolean("transform.proto.parallel", true)) {
            VariantSliceReader sliceReader = new VariantSliceReader(helper.getChunkSize(), dataReader,
                    helper.getStudyId(), Integer.valueOf(helper.getFileMetadata().getId()));

            // Use a supplier to avoid concurrent modifications of non thread safe objects.
            Supplier<Task<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice>> supplier =
                    () -> ((Task<ImmutablePair<Long, List<Variant>>, ImmutablePair<Long, List<Variant>>>) ((batch) -> {
                        for (ImmutablePair<Long, List<Variant>> pair : batch) {
                            statsCalculator.apply(pair.getRight()
                                    .stream()
                                    .filter(variant -> variant.getStart() >= pair.getKey())
                                    .collect(Collectors.toList()));
                        }
                        return batch;
                    })).then(new VariantToVcfSliceConverterTask(progressLogger));

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
            VariantHbaseTransformTask transformTask = new VariantHbaseTransformTask(helper);
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
        super.preLoad(input, output);

        try {
            ArchiveTableHelper.createArchiveTableIfNeeded(dbAdaptor.getGenomeHelper(), getArchiveTable(),
                    dbAdaptor.getConnection());
        } catch (IOException e) {
            throw new StorageHadoopException("Issue creating table " + getArchiveTable(), e);
        }
        try {
            VariantTableHelper.createVariantTableIfNeeded(dbAdaptor.getGenomeHelper(), variantsTableCredentials.getTable(),
                    dbAdaptor.getConnection());
        } catch (IOException e) {
            throw new StorageHadoopException("Issue creating table " + variantsTableCredentials.getTable(), e);
        }

        return input;
    }

    @Override
    protected void securePreLoad(StudyConfiguration studyConfiguration, VariantFileMetadata fileMetadata) throws StorageEngineException {
        super.securePreLoad(studyConfiguration, fileMetadata);

        MergeMode mergeMode;
        if (!studyConfiguration.getAttributes().containsKey(Options.MERGE_MODE.key())) {
            mergeMode = MergeMode.from(options);
            studyConfiguration.getAttributes().put(Options.MERGE_MODE.key(), mergeMode);
        } else {
            options.put(MERGE_MODE.key(), MergeMode.from(studyConfiguration.getAttributes()));
        }
    }

    @Override
    public void securePostLoad(List<Integer> fileIds, StudyConfiguration studyConfiguration) throws StorageEngineException {
        super.securePostLoad(fileIds, studyConfiguration);
        studyConfiguration.getAttributes().put(MISSING_GENOTYPES_UPDATED, false);
    }

    @Override
    public URI load(URI input) throws IOException, StorageEngineException {
        int studyId = getStudyId();
        int fileId = getFileId();

        ArchiveTableHelper.setChunkSize(conf, getOptions().getInt(ARCHIVE_CHUNK_SIZE, DEFAULT_ARCHIVE_CHUNK_SIZE));
        ArchiveTableHelper.setStudyId(conf, studyId);

        Set<Integer> loadedFiles = dbAdaptor.getVariantFileMetadataDBAdaptor().getLoadedFiles(studyId);
        if (!loadedFiles.contains(fileId)) {
            load(input, studyId, fileId);
        } else {
            logger.info("File {} already loaded. Skip this step!",
                    Paths.get(input.getPath()).getFileName().toString());
        }

        return input; // TODO  change return value?
    }

    protected abstract void load(URI input, int studyId, int fileId) throws StorageEngineException;

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
        StudyConfigurationManager scm = getStudyConfigurationManager();

        try {
            int studyId = getStudyId();
            VariantFileMetadata fileMetadata = readVariantFileMetadata(input);
            fileMetadata.setId(String.valueOf(getFileId()));
            scm.updateVariantFileMetadata(studyId, fileMetadata);
            dbAdaptor.getVariantFileMetadataDBAdaptor().updateLoadedFilesSummary(studyId, Collections.singletonList(getFileId()));
        } catch (IOException e) {
            throw new StorageEngineException("Error storing VariantFileMetadata for file " + getFileId(), e);
        }

        registerLoadedFiles(Collections.singletonList(getFileId()));

        // This method checks the loaded variants (if possible) and adds the loaded files to the studyConfiguration
        super.postLoad(input, output);

        return input;
    }

    protected void registerLoadedFiles(List<Integer> fileIds) throws StorageEngineException {
        // Current StudyConfiguration may be outdated. Force fetch.
        StudyConfiguration studyConfiguration = getStudyConfiguration(true);

        VariantPhoenixHelper phoenixHelper = new VariantPhoenixHelper(dbAdaptor.getGenomeHelper());


        String metaTableName = dbAdaptor.getTableNameGenerator().getMetaTableName();
        String variantsTableName = dbAdaptor.getTableNameGenerator().getVariantTableName();
        Connection jdbcConnection = dbAdaptor.getJdbcConnection();

        final String species = getStudyConfigurationManager().getProjectMetadata().first().getSpecies();

        Long lock = null;
        try {
            long lockDuration = TimeUnit.MINUTES.toMillis(5);
            try {
                lock = getStudyConfigurationManager().lockStudy(studyConfiguration.getStudyId(), lockDuration,
                        TimeUnit.SECONDS.toMillis(5), GenomeHelper.PHOENIX_LOCK_COLUMN);
            } catch (TimeoutException e) {
                int timeout = 10;
                logger.info("Waiting to get Lock over HBase table {} up to {} minutes ...", metaTableName, timeout);
                lock = getStudyConfigurationManager().lockStudy(studyConfiguration.getStudyId(), lockDuration,
                        TimeUnit.MINUTES.toMillis(timeout), GenomeHelper.PHOENIX_LOCK_COLUMN);
            }
            logger.debug("Winning lock {}", lock);

            try {
                phoenixHelper.registerNewStudy(jdbcConnection, variantsTableName, studyConfiguration.getStudyId());
            } catch (SQLException e) {
                throw new StorageEngineException("Unable to register study in Phoenix", e);
            }

            try {
                if (species.equals("hsapiens")) {
                    List<PhoenixHelper.Column> columns = VariantPhoenixHelper.getHumanPopulationFrequenciesColumns();
                    phoenixHelper.addMissingColumns(jdbcConnection, variantsTableName, columns, true);
                }
            } catch (SQLException e) {
                throw new StorageEngineException("Unable to register population frequency columns in Phoenix", e);
            }

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
                phoenixHelper.registerNewFiles(jdbcConnection, variantsTableName, studyConfiguration.getStudyId(), fileIds,
                        newSamples);

                int release = getStudyConfigurationManager().getProjectMetadata().first().getRelease();
                phoenixHelper.registerRelease(jdbcConnection, variantsTableName, release);

            } catch (SQLException e) {
                throw new StorageEngineException("Unable to register samples in Phoenix", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageEngineException("Error locking table to modify Phoenix columns!", e);
        } catch (TimeoutException e) {
            throw new StorageEngineException("Error locking table to modify Phoenix columns!", e);
        } finally {
            try {
                if (lock != null) {
                    getStudyConfigurationManager().unLockStudy(studyConfiguration.getStudyId(), lock, GenomeHelper.PHOENIX_LOCK_COLUMN);
                }
            } catch (HBaseLock.IllegalLockStatusException e) {
                logger.warn(e.getMessage());
                logger.debug(e.getMessage(), e);
            }
        }

        if (VariantPhoenixHelper.DEFAULT_TABLE_TYPE == PTableType.VIEW) {
            logger.debug("Skip create indexes for VIEW table");
        } else if (options.getBoolean(VARIANT_TABLE_INDEXES_SKIP, false)) {
            logger.info("Skip create indexes!!");
        } else {
            lock = null;
            try {
                lock = getStudyConfigurationManager().lockStudy(studyConfiguration.getStudyId(), TimeUnit.MINUTES.toMillis(60),
                        TimeUnit.SECONDS.toMillis(5), PHOENIX_INDEX_LOCK_COLUMN);
                if (species.equals("hsapiens")) {
                    List<PhoenixHelper.Index> popFreqIndices = VariantPhoenixHelper.getPopFreqIndices(variantsTableName);
                    phoenixHelper.getPhoenixHelper().createIndexes(jdbcConnection, VariantPhoenixHelper.DEFAULT_TABLE_TYPE,
                            variantsTableName, popFreqIndices, false);
                }
                phoenixHelper.createVariantIndexes(jdbcConnection, variantsTableName);
            } catch (SQLException e) {
                throw new StorageEngineException("Unable to create Phoenix Indexes", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StorageEngineException("Unable to create Phoenix Indexes", e);
            } catch (TimeoutException e) {
                // Indices are been created by another instance. Don't need to create twice.
                logger.info("Unable to get lock to create PHOENIX INDICES. Already been created by another instance. "
                        + "Skip create indexes!");
            } finally {
                if (lock != null) {
                    getStudyConfigurationManager().unLockStudy(studyConfiguration.getStudyId(), lock, PHOENIX_INDEX_LOCK_COLUMN);
                }
            }
        }
    }

    @Override
    public void close() throws StorageEngineException {
        // Do not close VariantDBAdaptor
    }

    protected String getArchiveTable() throws StorageEngineException {
        return getDBAdaptor().getArchiveTableName(getStudyId());
    }
}
