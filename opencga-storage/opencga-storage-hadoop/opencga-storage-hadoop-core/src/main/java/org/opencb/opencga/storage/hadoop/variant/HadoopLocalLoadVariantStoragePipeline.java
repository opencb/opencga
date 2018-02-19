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
import org.apache.hadoop.conf.Configuration;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.tools.variant.converters.proto.VcfSliceToVariantListConverter;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.ParallelTaskRunner.Task;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHBaseArchiveDataWriter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHadoopDBWriter;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantToVcfSliceConverterTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.STORAGE_ENGINE_ID;

/**
 * Created on 06/06/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopLocalLoadVariantStoragePipeline extends HadoopVariantStoragePipeline {

    private final Logger logger = LoggerFactory.getLogger(HadoopLocalLoadVariantStoragePipeline.class);
    private static final String OPERATION_NAME = "Load";

    /**
     * @param configuration      {@link StorageConfiguration}
     * @param dbAdaptor          {@link VariantHadoopDBAdaptor}
     * @param conf               {@link Configuration}
     * @param archiveCredentials {@link HBaseCredentials}
     * @param variantReaderUtils {@link VariantReaderUtils}
     * @param options            {@link ObjectMap}
     */
    public HadoopLocalLoadVariantStoragePipeline(StorageConfiguration configuration,
                                                 VariantHadoopDBAdaptor dbAdaptor, Configuration conf,
                                                 HBaseCredentials archiveCredentials, VariantReaderUtils variantReaderUtils,
                                                 ObjectMap options) {
        super(configuration, dbAdaptor, variantReaderUtils, options, archiveCredentials, null, conf);
    }

    @Override
    protected void securePreLoad(StudyConfiguration studyConfiguration, VariantFileMetadata fileMetadata) throws StorageEngineException {
        super.securePreLoad(studyConfiguration, fileMetadata);

        if (options.getBoolean(VariantStorageEngine.Options.LOAD_SPLIT_DATA.key(),
                VariantStorageEngine.Options.LOAD_SPLIT_DATA.defaultValue())) {
            throw new StorageEngineException("Unable to load split data in " + STORAGE_ENGINE_ID);
        }

        final AtomicInteger ongoingLoads = new AtomicInteger(1); // this
        boolean resume = options.getBoolean(VariantStorageEngine.Options.RESUME.key(), VariantStorageEngine.Options.RESUME.defaultValue());
        List<Integer> fileIds = Collections.singletonList(options.getInt(VariantStorageEngine.Options.FILE_ID.key()));

        StudyConfigurationManager.addBatchOperation(studyConfiguration, OPERATION_NAME, fileIds, resume, BatchFileOperation.Type.LOAD,
                operation -> {
                    if (operation.getOperationName().equals(OPERATION_NAME)) {
                        if (operation.currentStatus().equals(BatchFileOperation.Status.ERROR)) {
                            Integer fileId = operation.getFileIds().get(0);
                            String fileName = studyConfiguration.getFileIds().inverse().get(fileId);
                            logger.warn("Pending load operation for file " + fileName + " (" + fileId + ')');
                        } else {
                            ongoingLoads.incrementAndGet();
                        }
                        return true;
                    } else {
                        return false;
                    }
                });

        if (ongoingLoads.get() > 1) {
            logger.info("There are " + ongoingLoads.get() + " concurrent load operations");
        }
    }

    @Override
    protected void load(URI inputUri, int studyId, int fileId) throws StorageEngineException {

        Thread hook = newShutdownHook(OPERATION_NAME, Collections.singletonList(fileId));
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            Path input = Paths.get(inputUri.getPath());
            String table = archiveTableCredentials.getTable();
            String fileName = input.getFileName().toString();

            VariantFileMetadata fileMetadata = variantReaderUtils.readVariantFileMetadata(inputUri);
            fileMetadata.setId(String.valueOf(fileId));
//            fileMetadata.setStudyId(Integer.toString(studyId));

            long start = System.currentTimeMillis();
            if (VariantReaderUtils.isProto(fileName)) {
                ArchiveTableHelper helper = new ArchiveTableHelper(dbAdaptor.getGenomeHelper(), studyId, fileMetadata);

                ProgressLogger progressLogger = new ProgressLogger("Loaded slices:");
                if (fileMetadata.getStats() != null) {
                    progressLogger.setApproximateTotalCount(fileMetadata.getStats().getNumVariants());
                }

                loadFromProto(input, table, helper, progressLogger);
            } else {
                ArchiveTableHelper helper = new ArchiveTableHelper(dbAdaptor.getGenomeHelper(), studyId, fileMetadata);

                ProgressLogger progressLogger;
                if (fileMetadata.getStats() != null) {
                    progressLogger = new ProgressLogger("Loaded variants for file \"" + input.getFileName() + "\" :",
                            fileMetadata.getStats().getNumVariants());
                } else {
                    progressLogger = new ProgressLogger("Loaded variants for file \"" + input.getFileName() + "\" :");
                }

                loadFromAvro(input, table, helper, progressLogger);
            }
            long end = System.currentTimeMillis();
            logger.info("end - start = " + (end - start) / 1000.0 + "s");

        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }


    protected void loadFromProto(Path input, String table, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        long counter = 0;

        VariantHBaseArchiveDataWriter archiveWriter = new VariantHBaseArchiveDataWriter(helper, table, dbAdaptor.getHBaseManager());
        VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(helper.getStudyMetadata());
        VariantHadoopDBWriter variantsWriter = newVariantHadoopDBWriter();

//        ((Task<VcfSlice, VcfSlice>) t -> t)
//                .then(archiveWriter)
//                .then((Task<VcfSlice, Variant>) slices -> slices
//                        .stream()
//                        .flatMap(slice -> converter.convert(slice).stream())
//                        .filter(variant -> !variant.getType().equals(VariantType.NO_VARIATION))
//                        .collect(Collectors.toList())).then(variantsWriter);

        try (InputStream in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(input.toFile())))) {
            archiveWriter.open();
            archiveWriter.pre();
            variantsWriter.open();
            variantsWriter.pre();
            VcfSlice slice = VcfSlice.parseDelimitedFrom(in);
            while (null != slice) {
                ++counter;
                archiveWriter.write(slice);

                List<Variant> variants = converter.convert(slice);
                variantsWriter.write(variants);

                progressLogger.increment(slice.getRecordsCount());
                slice = VcfSlice.parseDelimitedFrom(in);
            }
            archiveWriter.post();
            variantsWriter.post();
        } catch (IOException e) {
            throw new StorageEngineException("Problems reading " + input, e);
        } finally {
            archiveWriter.close();
            variantsWriter.close();
        }
        logger.info("Read {} slices", counter);
    }

    protected void loadFromAvro(Path input, String table, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        VariantReader variantReader = VariantReaderUtils.getVariantReader(input, helper.getStudyMetadata());
        int studyId = helper.getStudyId();
        int fileId = Integer.valueOf(helper.getFileMetadata().getId());
        VariantSliceReader sliceReader = new VariantSliceReader(helper.getChunkSize(), variantReader, studyId, fileId, progressLogger);

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(1)
                .setBatchSize(1)
                .setReadQueuePutTimeout(1000).build();

        VariantHBaseArchiveDataWriter archiveWriter = new VariantHBaseArchiveDataWriter(helper, table, dbAdaptor.getHBaseManager());
        VariantHadoopDBWriter hadoopDBWriter = newVariantHadoopDBWriter();
        GroupedVariantsTask task = new GroupedVariantsTask(archiveWriter, hadoopDBWriter, null);

        ParallelTaskRunner<ImmutablePair<Long, List<Variant>>, VcfSlice> ptr =
                new ParallelTaskRunner<>(sliceReader, task, null, config);
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error loading file " + input, e);
        }
    }

    @Override
    public void securePostLoad(List<Integer> fileIds, StudyConfiguration studyConfiguration) throws StorageEngineException {
        super.securePostLoad(fileIds, studyConfiguration);
        StudyConfigurationManager.setStatus(studyConfiguration, BatchFileOperation.Status.READY, OPERATION_NAME, fileIds);
    }

    private VariantHadoopDBWriter newVariantHadoopDBWriter() throws StorageEngineException {
        StudyConfiguration studyConfiguration = getStudyConfiguration();
        return new VariantHadoopDBWriter(
                dbAdaptor.getGenomeHelper(),
                dbAdaptor.getCredentials().getTable(),
                studyConfiguration,
                dbAdaptor.getHBaseManager());
    }

    protected static class GroupedVariantsTask implements Task<ImmutablePair<Long, List<Variant>>, VcfSlice> {
        private final VariantToVcfSliceConverterTask converterTask;
        private final VariantHBaseArchiveDataWriter archiveWriter;
        private final VariantHadoopDBWriter hadoopDBWriter;

        GroupedVariantsTask(VariantHBaseArchiveDataWriter archiveWriter, VariantHadoopDBWriter hadoopDBWriter,
                            ProgressLogger progressLogger) {
            this.converterTask = new VariantToVcfSliceConverterTask(progressLogger);
            this.archiveWriter = Objects.requireNonNull(archiveWriter);
            this.hadoopDBWriter = Objects.requireNonNull(hadoopDBWriter);
        }

        @Override
        public void pre() throws Exception {
            archiveWriter.open();
            archiveWriter.pre();

            hadoopDBWriter.open();
            hadoopDBWriter.pre();

            converterTask.pre();
        }

        @Override
        public List<VcfSlice> apply(List<ImmutablePair<Long, List<Variant>>> batch) {
            for (ImmutablePair<Long, List<Variant>> pair : batch) {
                hadoopDBWriter.write(pair.getRight());
            }
            List<VcfSlice> slices = converterTask.apply(batch);
            archiveWriter.write(slices);
            return slices;
        }

        @Override
        public void post() throws Exception {
            archiveWriter.post();
            archiveWriter.close();

            hadoopDBWriter.post();
            hadoopDBWriter.close();

            converterTask.post();
        }
    }

}
