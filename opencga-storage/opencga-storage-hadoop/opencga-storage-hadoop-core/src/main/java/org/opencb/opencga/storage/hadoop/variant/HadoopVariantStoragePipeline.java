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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.io.proto.ProtoFileWriter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.hadoop.exceptions.StorageHadoopException;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantToVcfSliceConverterTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.*;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.*;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.ARCHIVE_SLICE_BUFFER_SIZE;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.VARIANT_TABLE_LOAD_REFERENCE;

/**
 * Created by mh719 on 13/05/2016.
 */
public abstract class HadoopVariantStoragePipeline extends VariantStoragePipeline {
    protected final VariantHadoopDBAdaptor dbAdaptor;
    protected final Configuration conf;
    protected MRExecutor mrExecutor = null;

    private final Logger logger = LoggerFactory.getLogger(HadoopVariantStoragePipeline.class);

    public HadoopVariantStoragePipeline(
            StorageConfiguration configuration,
            VariantHadoopDBAdaptor dbAdaptor,
            ObjectMap options,
            MRExecutor mrExecutor,
            Configuration conf, IOConnectorProvider ioConnectorProvider) {
        super(configuration, STORAGE_ENGINE_ID, dbAdaptor, ioConnectorProvider, options);
        this.mrExecutor = mrExecutor;
        this.dbAdaptor = dbAdaptor;
        this.conf = new Configuration(conf);

    }

    @Override
    public VariantHadoopDBAdaptor getDBAdaptor() {
        return dbAdaptor;
    }

    @Override
    public URI preTransform(URI input) throws StorageEngineException, IOException, FileFormatException {
        logger.info("PreTransform: " + input);
//        ObjectMap options = configuration.getVariantEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        if (!options.containsKey(VariantStorageOptions.TRANSFORM_FORMAT.key())) {
            options.put(VariantStorageOptions.TRANSFORM_FORMAT.key(),
                    VariantStorageOptions.TRANSFORM_FORMAT.defaultValue());
        }
        String transVal = options.getString(VariantStorageOptions.TRANSFORM_FORMAT.key());
        switch (transVal){
            case "avro":
            case "proto":
            case "json":
                break;
            default:
                throw new IllegalArgumentException("Output format " + transVal + " not supported for Hadoop!");
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
    protected ParallelTaskRunner transformProto(VariantFileMetadata fileMetadata, URI outputVariantsFile,
                                                DataReader<String> stringReader, Supplier<Task<Pair<Integer, List<String>>, Variant>> task)
            throws StorageEngineException {

        int sliceBufferSize = options.getInt(ARCHIVE_SLICE_BUFFER_SIZE.key(), ARCHIVE_SLICE_BUFFER_SIZE.defaultValue());
        //Writer
        DataWriter<VcfSliceProtos.VcfSlice> dataWriter;
        try {
            dataWriter = new ProtoFileWriter<>(ioConnectorProvider.newOutputStream(outputVariantsFile), true);
        } catch (IOException e) {
            throw StorageEngineException.ioException(e);
        }

        AtomicInteger lineNumber = new AtomicInteger(0);
        final DataReader<Variant> dataReader = stringReader.then(list -> {
            int i = lineNumber.addAndGet(list.size());
            return Collections.singletonList(Pair.of(i, list));
        }).then(task.get());

        // Transformer
        ArchiveTableHelper helper = new ArchiveTableHelper(conf, getStudyId(), fileMetadata);

        logger.info("Generating output file {}", outputVariantsFile);

        VariantSliceReader sliceReader = new VariantSliceReader(helper.getChunkSize(), dataReader,
                helper.getStudyId(), Integer.valueOf(helper.getFileMetadata().getId()), sliceBufferSize);

        // Use a supplier to avoid concurrent modifications of non thread safe objects.
        Supplier<Task<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice>> supplier = VariantToVcfSliceConverterTask::new;

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(options.getInt(VariantStorageOptions.TRANSFORM_THREADS.key(), 1))
                .setBatchSize(1)
                .setAbortOnFail(true)
                .setSorted(true)
                .setCapacity(1)
                .build();

        return new ParallelTaskRunner<>(sliceReader, supplier, dataWriter, config);


    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageEngineException {
        super.preLoad(input, output);

        try {
            if (YesNoAuto.parse(getOptions(), LOAD_ARCHIVE.key()).orYes().booleanValue()) {
                ArchiveTableHelper.createArchiveTableIfNeeded(getOptions(), getArchiveTable(), dbAdaptor.getConnection());
            } else {
                logger.info("Skip archive table");
            }
        } catch (IOException e) {
            throw new StorageHadoopException("Issue creating table " + getArchiveTable(), e);
        }
        try {
            VariantTableHelper.createVariantTableIfNeeded(dbAdaptor.getVariantTable(),
                    dbAdaptor.getConnection(), dbAdaptor.getConfiguration());
        } catch (IOException e) {
            throw new StorageHadoopException("Issue creating table " + dbAdaptor.getVariantTable(), e);
        }

        return input;
    }

    @Override
    protected void securePreLoad(StudyMetadata studyMetadata, VariantFileMetadata fileMetadata) throws StorageEngineException {
        super.securePreLoad(studyMetadata, fileMetadata);

        if (fileMetadata.getSampleIds().isEmpty()) {
            YesNoAuto loadArchive = YesNoAuto.parse(getOptions(), LOAD_ARCHIVE.key());
            if (loadArchive == YesNoAuto.AUTO) {
                logger.info("Loading aggregated file with no samples. Skip Archive Table.");
                getOptions().put(LOAD_ARCHIVE.key(), YesNoAuto.NO);
            }
        }

        YesNoAuto loadHomRef = YesNoAuto.parse(getOptions(), LOAD_HOM_REF.key());
        if (loadHomRef != YesNoAuto.AUTO) {
            getOptions().put(VARIANT_TABLE_LOAD_REFERENCE.key(), loadHomRef.booleanValue());
        }
        // loadHomRef == auto;
        // Else use configuration value.

        logger.info(LOAD_HOM_REF.key() + " : "
                + getOptions().getBoolean(VARIANT_TABLE_LOAD_REFERENCE.key(), VARIANT_TABLE_LOAD_REFERENCE.defaultValue()));

        MergeMode mergeMode;
        if (!studyMetadata.getAttributes().containsKey(VariantStorageOptions.MERGE_MODE.key())) {
            mergeMode = MergeMode.from(options);
            studyMetadata.getAttributes().put(VariantStorageOptions.MERGE_MODE.key(), mergeMode);
        } else {
            options.put(MERGE_MODE.key(), MergeMode.from(studyMetadata.getAttributes()));
        }
    }

    @Override
    protected void securePostLoad(List<Integer> fileIds, StudyMetadata studyMetadata) throws StorageEngineException {
        super.securePostLoad(fileIds, studyMetadata);
        studyMetadata.getAttributes().put(MISSING_GENOTYPES_UPDATED, false);
    }



    @Override
    public URI load(URI input, URI outdir) throws StorageEngineException {
        int studyId = getStudyId();
        int fileId = getFileId();

        int chunkSize = getOptions().getInt(HadoopVariantStorageOptions.ARCHIVE_CHUNK_SIZE.key(),
                HadoopVariantStorageOptions.ARCHIVE_CHUNK_SIZE.defaultValue());
        ArchiveTableHelper.setChunkSize(conf, chunkSize);
        ArchiveTableHelper.setStudyId(conf, studyId);

        FileMetadata fileMetadata = getMetadataManager().getFileMetadata(studyId, fileId);
        if (!fileMetadata.isIndexed()) {
            load(input, outdir, studyId, fileId);
        } else {
            logger.info("File {} already loaded. Skip this step!", UriUtils.fileName(input));
        }

        return input; // TODO  change return value?
    }

    protected abstract void load(URI input, URI outdir, int studyId, int fileId) throws StorageEngineException;

    @Override
    protected void checkLoadedVariants(int fileId, StudyMetadata studyMetadata) throws
            StorageEngineException {
        logger.warn("Skip check loaded variants");
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();

        int studyId = getStudyId();
        VariantFileMetadata fileMetadata = readVariantFileMetadata(input);
        fileMetadata.setId(String.valueOf(getFileId()));
        metadataManager.updateVariantFileMetadata(studyId, fileMetadata);

        registerLoadedFiles(Collections.singletonList(getFileId()));
        metadataManager.updateProjectMetadata(projectMetadata -> {
            projectMetadata.getAttributes().put(LAST_LOADED_FILE_TS, System.currentTimeMillis());
            return projectMetadata;
        });

        // This method checks the loaded variants (if possible) and adds the loaded files to the metadata
        super.postLoad(input, output);

        return input;
    }

    protected void registerLoadedFiles(List<Integer> fileIds) throws StorageEngineException {
        int studyId = getStudyId();

        try (VariantPhoenixSchemaManager schemaManager = new VariantPhoenixSchemaManager(dbAdaptor)) {
            schemaManager.registerNewFiles(studyId, fileIds);
            if (options.getBoolean(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), false)) {
                logger.info("Skip create indexes!!");
            } else {
                schemaManager.createPhoenixIndexes();
            }
        } catch (SQLException e) {
            throw new StorageEngineException("Error closing schema manager", e);
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
