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
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.VariantDeduplicationTask;
import org.opencb.biodata.tools.variant.converters.proto.VcfSliceToVariantListConverter;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOManagerProvider;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.transform.DiscardDuplicatedVariantsResolver;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHBaseArchiveDataWriter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;
import org.opencb.opencga.storage.hadoop.variant.load.VariantHadoopDBWriter;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantToVcfSliceConverterTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.STDIN;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.*;

/**
 * Created on 06/06/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopLocalLoadVariantStoragePipeline extends HadoopVariantStoragePipeline {

    private final Logger logger = LoggerFactory.getLogger(HadoopLocalLoadVariantStoragePipeline.class);
    private static final String OPERATION_NAME = "Load";
    private int taskId;
    private HashSet<String> loadedGenotypes;

    public HadoopLocalLoadVariantStoragePipeline(StorageConfiguration configuration,
                                                 VariantHadoopDBAdaptor dbAdaptor, IOManagerProvider ioManagerProvider, Configuration conf,
                                                 ObjectMap options) {
        super(configuration, dbAdaptor, options, null, conf, ioManagerProvider);
    }

    @Override
    protected void securePreLoad(StudyMetadata studyMetadata, VariantFileMetadata fileMetadata) throws StorageEngineException {
        super.securePreLoad(studyMetadata, fileMetadata);

        if (options.getBoolean(VariantStorageEngine.Options.LOAD_SPLIT_DATA.key(),
                VariantStorageEngine.Options.LOAD_SPLIT_DATA.defaultValue())) {
            throw new StorageEngineException("Unable to load split data in " + STORAGE_ENGINE_ID);
        }

        final AtomicInteger ongoingLoads = new AtomicInteger(1); // this
        boolean resume = options.getBoolean(VariantStorageEngine.Options.RESUME.key(), VariantStorageEngine.Options.RESUME.defaultValue());
        List<Integer> fileIds = Collections.singletonList(getFileId());

        taskId = getMetadataManager()
                .addRunningTask(getStudyId(), OPERATION_NAME, fileIds, resume, TaskMetadata.Type.LOAD,
                operation -> {
                    if (operation.getName().equals(OPERATION_NAME)) {
                        if (operation.currentStatus().equals(TaskMetadata.Status.ERROR)) {
                            Integer fileId = operation.getFileIds().get(0);
                            String fileName = getMetadataManager().getFileName(studyMetadata.getId(), fileId);
                            logger.warn("Pending load operation for file " + fileName + " (" + fileId + ')');
                        } else {
                            ongoingLoads.incrementAndGet();
                        }
                        return true;
                    } else {
                        return false;
                    }
                }).getId();

        if (ongoingLoads.get() > 1) {
            logger.info("There are " + ongoingLoads.get() + " concurrent load operations");
        }
    }

    @Override
    protected void load(URI inputUri, int studyId, int fileId) throws StorageEngineException {

        if (getMetadataManager().getTask(studyId, taskId).currentStatus().equals(TaskMetadata.Status.DONE)) {
            logger.info("File {} already loaded. Skip this step!",
                    UriUtils.fileName(inputUri));
            return;
        }

        Thread hook = getMetadataManager().buildShutdownHook(OPERATION_NAME, getStudyId(), taskId);
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            String table = getArchiveTable();
            String fileName = UriUtils.fileName(inputUri);

            VariantFileMetadata fileMetadata = variantReaderUtils.readVariantFileMetadata(inputUri);
            fileMetadata.setId(String.valueOf(fileId));
//            fileMetadata.setStudyId(Integer.toString(studyId));

            ArchiveTableHelper helper = new ArchiveTableHelper(dbAdaptor.getGenomeHelper(), studyId, fileMetadata);
            long start = System.currentTimeMillis();
            if (VariantReaderUtils.isProto(fileName)) {
                ProgressLogger progressLogger = new ProgressLogger("Loaded slices:");
                if (fileMetadata.getStats() != null) {
                    progressLogger.setApproximateTotalCount(fileMetadata.getStats().getNumVariants());
                }

                loadFromProto(inputUri, table, helper, progressLogger);
            } else {
                ProgressLogger progressLogger;
                if (fileMetadata.getStats() != null) {
                    progressLogger = new ProgressLogger("Loaded variants for file \"" + fileName + "\" :",
                            fileMetadata.getStats().getNumVariants());
                } else {
                    progressLogger = new ProgressLogger("Loaded variants for file \"" + fileName + "\" :");
                }

                loadFromAvro(inputUri, table, helper, progressLogger);
            }
            long end = System.currentTimeMillis();
            logger.info("end - start = " + (end - start) / 1000.0 + "s");

            // Mark file as DONE
            getMetadataManager().setStatus(getStudyId(), taskId, TaskMetadata.Status.DONE);
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }


    protected void loadFromProto(URI input, String table, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        long counter = 0;

        VariantHBaseArchiveDataWriter archiveWriter = new VariantHBaseArchiveDataWriter(helper, table, dbAdaptor.getHBaseManager());
        VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(helper.getStudyMetadata());
        VariantHadoopDBWriter variantsWriter = newVariantHadoopDBWriter();
        List<Integer> sampleIds = new ArrayList<>(getMetadataManager().getFileMetadata(getStudyId(), getFileId()).getSamples());
        SampleIndexDBLoader sampleIndexDBLoader;
        if (sampleIds.isEmpty()) {
            sampleIndexDBLoader = null;
        } else {
            sampleIndexDBLoader = new SampleIndexDBLoader(dbAdaptor.getHBaseManager(),
                    dbAdaptor.getTableNameGenerator().getSampleIndexTableName(helper.getStudyId()), sampleIds,
                    dbAdaptor.getGenomeHelper().getColumnFamily(),
                    getOptions());
        }

//        ((TaskMetadata<VcfSlice, VcfSlice>) t -> t)
//                .then(archiveWriter)
//                .then((TaskMetadata<VcfSlice, Variant>) slices -> slices
//                        .stream()
//                        .flatMap(slice -> converter.convert(slice).stream())
//                        .filter(variant -> !variant.getType().equals(VariantType.NO_VARIATION))
//                        .collect(Collectors.toList())).then(variantsWriter);

        try (InputStream in = ioManagerProvider.newInputStream(input)) {
            archiveWriter.open();
            archiveWriter.pre();
            variantsWriter.open();
            variantsWriter.pre();
            if (sampleIndexDBLoader != null) {
                sampleIndexDBLoader.open();
                sampleIndexDBLoader.pre();
            }
            VcfSlice slice = VcfSlice.parseDelimitedFrom(in);
            while (null != slice) {
                ++counter;
                archiveWriter.write(slice);

                List<Variant> variants = converter.convert(slice);
                variantsWriter.write(variants);
                if (sampleIndexDBLoader != null) {
                    sampleIndexDBLoader.write(variants);
                }

                progressLogger.increment(slice.getRecordsCount());
                slice = VcfSlice.parseDelimitedFrom(in);
            }
            archiveWriter.post();
            variantsWriter.post();
            if (sampleIndexDBLoader != null) {
                sampleIndexDBLoader.post();
            }
        } catch (IOException e) {
            throw new StorageEngineException("Problems reading " + input, e);
        } finally {
            archiveWriter.close();
            variantsWriter.close();
            if (sampleIndexDBLoader != null) {
                sampleIndexDBLoader.close();
            }
        }
        logger.info("Read {} slices", counter);

        if (sampleIndexDBLoader != null) {
            // Update list of loaded genotypes
            this.loadedGenotypes = sampleIndexDBLoader.getLoadedGenotypes();
        }
    }

    protected void loadFromAvro(URI input, String table, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        boolean stdin = options.getBoolean(STDIN.key(), STDIN.defaultValue());

        VariantReader variantReader = variantReaderUtils.getVariantReader(input, helper.getStudyMetadata(), stdin);
        int studyId = helper.getStudyId();
        int fileId = Integer.valueOf(helper.getFileMetadata().getId());

        // Config
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(1) // Increasing the numTasks may produce wrong results writing the sampleIndex
                .setBatchSize(1)
                .setReadQueuePutTimeout(1000).build();

        // Reader
        VariantDeduplicationTask dedupTask = new VariantDeduplicationTask(new DiscardDuplicatedVariantsResolver(fileId));
        VariantSliceReader sliceReader = new VariantSliceReader(
                helper.getChunkSize(), variantReader.then(dedupTask), studyId, fileId, progressLogger);

        // Archive Writer
        VariantHBaseArchiveDataWriter archiveWriter = new VariantHBaseArchiveDataWriter(helper, table, dbAdaptor.getHBaseManager());
        // Variants Writer
        VariantHadoopDBWriter hadoopDBWriter = newVariantHadoopDBWriter();
        // Sample Index Writer
        List<Integer> sampleIds = new ArrayList<>(getMetadataManager().getFileMetadata(studyId, fileId).getSamples());
        SampleIndexDBLoader sampleIndexDBLoader;
        if (sampleIds.isEmpty()) {
            sampleIndexDBLoader = null;
        } else {
            sampleIndexDBLoader = new SampleIndexDBLoader(dbAdaptor.getHBaseManager(),
                    dbAdaptor.getTableNameGenerator().getSampleIndexTableName(studyId), sampleIds,
                    dbAdaptor.getGenomeHelper().getColumnFamily(),
                    getOptions());
        }

        // TaskMetadata
        String archiveFields = options.getString(ARCHIVE_FIELDS);
        String nonRefFilter = options.getString(ARCHIVE_NON_REF_FILTER);
        GroupedVariantsTask task = new GroupedVariantsTask(archiveWriter, hadoopDBWriter, sampleIndexDBLoader,
                null, archiveFields, nonRefFilter);


        ParallelTaskRunner<ImmutablePair<Long, List<Variant>>, VcfSlice> ptr =
                new ParallelTaskRunner<>(sliceReader, task, null, config);
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error loading file " + input, e);
        }

        logLoadResults(variantReader.getVariantFileMetadata(), dedupTask.getDiscardedVariants(), hadoopDBWriter.getSkippedRefBlock(),
                hadoopDBWriter.getLoadedVariants(), hadoopDBWriter.getSkippedRefVariants());

        if (sampleIndexDBLoader != null) {
            // Update list of loaded genotypes
            this.loadedGenotypes = sampleIndexDBLoader.getLoadedGenotypes();
        }
    }

    private void logLoadResults(VariantFileMetadata variantFileMetadata, int duplicatedVariants, int skipped, int loadedVariants,
                                int skippedRefVariants) {
        // TODO: Check if the expectedCount matches with the count from HBase?
        // @see this.checkLoadedVariants
        logger.info("============================================================");
        int expectedCount = 0;
        for (VariantType variantType : TARGET_VARIANT_TYPE_SET) {
            expectedCount += variantFileMetadata.getStats().getVariantTypeCounts().getOrDefault(variantType.toString(), 0);
        }
        expectedCount -= duplicatedVariants;
        expectedCount -= skippedRefVariants;
        if (expectedCount == loadedVariants) {
            logger.info("Number of loaded variants: " + loadedVariants);
        } else {
            logger.warn("Wrong number of loaded variants. Expected: " + expectedCount + " but loaded " + loadedVariants);
        }
        if (duplicatedVariants > 0) {
            logger.warn("Found duplicated variants while loading the file. Discarded variants: " + duplicatedVariants);
        }
        if (skipped > 0) {
            logger.info("There were " + skipped + " skipped variants");
            for (VariantType type : VariantType.values()) {
                if (!TARGET_VARIANT_TYPE_SET.contains(type)) {
                    Integer countByType = variantFileMetadata.getStats().getVariantTypeCounts().get(type.toString());
                    if (countByType != null && countByType > 0) {
                        logger.info("  * Of which " + countByType + " are " + type.toString() + " variants.");
                    }
                }
            }
        }
        logger.info("============================================================");
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageEngineException {
        URI uri = super.postLoad(input, output);

        // Mark the load task as READY
        getMetadataManager().setStatus(getStudyId(), taskId, TaskMetadata.Status.READY);

        return uri;
    }

    @Override
    protected void securePostLoad(List<Integer> fileIds, StudyMetadata studyMetadata) throws StorageEngineException {
        super.securePostLoad(fileIds, studyMetadata);

        if (loadedGenotypes != null) {
            loadedGenotypes.addAll(studyMetadata.getAttributes().getAsStringList(VariantStorageEngine.Options.LOADED_GENOTYPES.key()));
            studyMetadata.getAttributes().put(VariantStorageEngine.Options.LOADED_GENOTYPES.key(), loadedGenotypes);
        }
    }

    private VariantHadoopDBWriter newVariantHadoopDBWriter() throws StorageEngineException {
        boolean includeReferenceVariantsData = getOptions().getBoolean(VARIANT_TABLE_LOAD_REFERENCE, false);
        return new VariantHadoopDBWriter(
                dbAdaptor.getGenomeHelper(),
                dbAdaptor.getCredentials().getTable(),
                getStudyId(),
                getMetadataManager(),
                dbAdaptor.getHBaseManager(), includeReferenceVariantsData);
    }

    protected static class GroupedVariantsTask implements Task<ImmutablePair<Long, List<Variant>>, VcfSlice> {
        private final VariantToVcfSliceConverterTask converterTask;
        private final VariantHBaseArchiveDataWriter archiveWriter;
        private final VariantHadoopDBWriter hadoopDBWriter;
        private final SampleIndexDBLoader sampleIndexDBLoader;

        GroupedVariantsTask(VariantHBaseArchiveDataWriter archiveWriter, VariantHadoopDBWriter hadoopDBWriter,
                            SampleIndexDBLoader sampleIndexDBLoader, ProgressLogger progressLogger) {
            this(archiveWriter, hadoopDBWriter, sampleIndexDBLoader, progressLogger, null, null);
        }

        GroupedVariantsTask(VariantHBaseArchiveDataWriter archiveWriter, VariantHadoopDBWriter hadoopDBWriter,
                            SampleIndexDBLoader sampleIndexDBLoader, ProgressLogger progressLogger, String fields, String nonRefFilter) {
            this.converterTask = new VariantToVcfSliceConverterTask(progressLogger, fields, nonRefFilter);
            this.archiveWriter = Objects.requireNonNull(archiveWriter);
            this.hadoopDBWriter = Objects.requireNonNull(hadoopDBWriter);
            this.sampleIndexDBLoader = sampleIndexDBLoader;
        }

        @Override
        public void pre() throws Exception {
            archiveWriter.open();
            archiveWriter.pre();

            hadoopDBWriter.open();
            hadoopDBWriter.pre();

            if (sampleIndexDBLoader != null) {
                sampleIndexDBLoader.open();
                sampleIndexDBLoader.pre();
            }

            converterTask.pre();
        }

        @Override
        public List<VcfSlice> apply(List<ImmutablePair<Long, List<Variant>>> batch) {
            for (ImmutablePair<Long, List<Variant>> pair : batch) {
                List<Variant> variants = pair.getRight();
                hadoopDBWriter.write(variants);

                if (sampleIndexDBLoader != null) {
                    sampleIndexDBLoader.write(variants);
                }
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

            if (sampleIndexDBLoader != null) {
                sampleIndexDBLoader.post();
                sampleIndexDBLoader.close();
            }

            converterTask.post();
        }
    }

}
