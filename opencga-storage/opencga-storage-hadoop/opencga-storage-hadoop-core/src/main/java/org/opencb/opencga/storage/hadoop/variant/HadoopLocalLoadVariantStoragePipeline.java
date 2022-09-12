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

import org.apache.commons.lang3.time.StopWatch;
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
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.dedup.AbstractDuplicatedVariantsResolver;
import org.opencb.opencga.storage.core.variant.dedup.DuplicatedVariantsResolverFactory;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHBaseArchiveDataWriter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;
import org.opencb.opencga.storage.hadoop.variant.load.VariantHadoopDBWriter;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantToVcfSliceConverterTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import static org.opencb.opencga.storage.core.metadata.models.TaskMetadata.Status;
import static org.opencb.opencga.storage.core.metadata.models.TaskMetadata.Type;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.*;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.TARGET_VARIANT_TYPE_SET;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.*;

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
    private int sampleIndexVersion;

    public HadoopLocalLoadVariantStoragePipeline(StorageConfiguration configuration,
                                                 VariantHadoopDBAdaptor dbAdaptor, IOConnectorProvider ioConnectorProvider,
                                                 Configuration conf, ObjectMap options) {
        super(configuration, dbAdaptor, options, null, conf, ioConnectorProvider);
    }

    @Override
    protected void preLoadRegisterAndValidateFile(int studyId, VariantFileMetadata variantFileMetadata) throws StorageEngineException {
        super.preLoadRegisterAndValidateFile(studyId, variantFileMetadata);
        boolean loadSampleIndex = YesNoAuto.parse(getOptions(), LOAD_SAMPLE_INDEX.key()).orYes().booleanValue();
        FileMetadata fileMetadata = getMetadataManager().getFileMetadata(studyId, getFileId());

        int version = getMetadataManager().getStudyMetadata(studyId).getSampleIndexConfigurationLatest().getVersion();
        Set<String> alreadyIndexedSamples = new LinkedHashSet<>();
        Set<Integer> processedSamples = new LinkedHashSet<>();
        Set<Integer> samplesWithoutSplitData = new LinkedHashSet<>();
        VariantStorageEngine.SplitData splitData = VariantStorageEngine.SplitData.from(options);
        for (String sample : variantFileMetadata.getSampleIds()) {
            Integer sampleId = getMetadataManager().getSampleId(studyId, sample);
            SampleMetadata sampleMetadata = getMetadataManager().getSampleMetadata(studyId, sampleId);
            if (splitData != null && sampleMetadata.getSplitData() != null) {
                if (!splitData.equals(sampleMetadata.getSplitData())) {
                    throw new StorageEngineException("Incompatible split data methods. "
                            + "Unable to mix requested " + splitData
                            + " with existing " + sampleMetadata.getSplitData());
                }
            }
            if (sampleMetadata.isIndexed()) {
                if (sampleMetadata.getFiles().size() == 1 && sampleMetadata.getFiles().contains(fileMetadata.getId())) {
                    // It might happen that the sample is marked as INDEXED, but not the file.
                    // If the sample only belongs to this file (i.e. it's only file is this file), then ignore
                    // the overwrite the current sample metadata index status
                    sampleMetadata = getMetadataManager().updateSampleMetadata(studyId, sampleId,
                            sm -> sm.setIndexStatus(fileMetadata.getIndexStatus()));
                }
            }
            if (sampleMetadata.isIndexed()) {
                alreadyIndexedSamples.add(sample);
                if (sampleMetadata.isAnnotated()
                        || !loadSampleIndex && sampleMetadata.getSampleIndexStatus(version) == Status.READY
                        || sampleMetadata.getSampleIndexAnnotationStatus(version) == Status.READY
                        || sampleMetadata.getFamilyIndexStatus(version) == Status.READY
                        || sampleMetadata.isFamilyIndexDefined()) {
                    processedSamples.add(sampleMetadata.getId());
                }
            }

            if (splitData != null && splitData != sampleMetadata.getSplitData()) {
                samplesWithoutSplitData.add(sampleId);
            }
        }

        if (!alreadyIndexedSamples.isEmpty()) {
            if (splitData != null) {
                logger.info("Loading split data");
            } else {
                String fileName = Paths.get(variantFileMetadata.getPath()).getFileName().toString();
                throw StorageEngineException.alreadyLoadedSamples(fileName, new ArrayList<>(alreadyIndexedSamples));
            }
            for (Integer sampleId : processedSamples) {
                getMetadataManager().updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                    if (!loadSampleIndex) {
                        for (Integer v : sampleMetadata.getSampleIndexVersions()) {
                            sampleMetadata.setSampleIndexStatus(Status.NONE, v);
                        }
                    }
                    for (Integer v : sampleMetadata.getSampleIndexAnnotationVersions()) {
                        sampleMetadata.setSampleIndexAnnotationStatus(Status.NONE, v);
                    }
                    for (Integer v : sampleMetadata.getFamilyIndexVersions()) {
                        sampleMetadata.setFamilyIndexStatus(Status.NONE, v);
                    }
                    sampleMetadata.setAnnotationStatus(Status.NONE);
                    sampleMetadata.setMendelianErrorStatus(Status.NONE);
                });
            }
        }

        if (splitData != null) {
            // Register loadSplitData
            for (Integer sampleId : samplesWithoutSplitData) {
                getMetadataManager().updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                    sampleMetadata.setSplitData(splitData);
                });
            }
        }
    }

    @Override
    protected void securePreLoad(StudyMetadata studyMetadata, VariantFileMetadata fileMetadata) throws StorageEngineException {
        super.securePreLoad(studyMetadata, fileMetadata);

        int studyId = getStudyId();

        final AtomicInteger ongoingLoads = new AtomicInteger(1); // this
        boolean resume = options.getBoolean(VariantStorageOptions.RESUME.key(), VariantStorageOptions.RESUME.defaultValue());
        List<Integer> fileIds = Collections.singletonList(getFileId());

        taskId = getMetadataManager()
                .addRunningTask(studyId, OPERATION_NAME, fileIds, resume, Type.LOAD,
                operation -> {
                    if (operation.getName().equals(OPERATION_NAME)) {
                        if (operation.currentStatus().equals(Status.ERROR)) {
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
    protected void load(URI inputUri, URI outdir, int studyId, int fileId) throws StorageEngineException {

        if (getMetadataManager().getTask(studyId, taskId).currentStatus().equals(Status.DONE)) {
            logger.info("File {} already loaded. Skip this step!",
                    UriUtils.fileName(inputUri));
            return;
        }

        Thread hook = getMetadataManager().buildShutdownHook(OPERATION_NAME, getStudyId(), taskId);
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            String fileName = UriUtils.fileName(inputUri);

            VariantFileMetadata fileMetadata = variantReaderUtils.readVariantFileMetadata(inputUri);
            fileMetadata.setId(String.valueOf(fileId));
//            fileMetadata.setStudyId(Integer.toString(studyId));

            ArchiveTableHelper helper = new ArchiveTableHelper(dbAdaptor.getConfiguration(), studyId, fileMetadata);
            StopWatch stopWatch = StopWatch.createStarted();
            if (VariantReaderUtils.isProto(fileName)) {
                ProgressLogger progressLogger = new ProgressLogger("Loaded slices:");
                if (fileMetadata.getStats() != null) {
                    progressLogger.setApproximateTotalCount(fileMetadata.getStats().getVariantCount());
                }

                loadFromProto(inputUri, outdir, helper, progressLogger);
            } else {
                ProgressLogger progressLogger;
                if (fileMetadata.getStats() != null) {
                    progressLogger = new ProgressLogger("Loaded variants for file \"" + fileName + "\" :",
                            fileMetadata.getStats().getVariantCount());
                } else {
                    progressLogger = new ProgressLogger("Loaded variants for file \"" + fileName + "\" :");
                }

                loadFromAvro(inputUri, outdir, helper, progressLogger);
            }
            logger.info("File \"{}\" loaded in {}", Paths.get(inputUri).getFileName(), TimeUtils.durationToString(stopWatch));

            // Mark file as DONE
            getMetadataManager().setStatus(getStudyId(), taskId, Status.DONE);
        } catch (Exception e) {
            // Mark file as ERROR
            getMetadataManager().setStatus(getStudyId(), taskId, Status.ERROR);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }


    protected void loadFromProto(URI input, URI outdir, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        long counter = 0;


        VariantHBaseArchiveDataWriter archiveWriter = newArchiveDBWriter(getArchiveTable(), helper);
        VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(helper.getStudyMetadata());
        VariantHadoopDBWriter variantsWriter = newVariantHadoopDBWriter();
        List<Integer> sampleIds = new ArrayList<>(getMetadataManager().getFileMetadata(getStudyId(), getFileId()).getSamples());
        SampleIndexDBLoader sampleIndexDBLoader = newSampleIndexDBLoader(sampleIds);

//        ((TaskMetadata<VcfSlice, VcfSlice>) t -> t)
//                .then(archiveWriter)
//                .then((TaskMetadata<VcfSlice, Variant>) slices -> slices
//                        .stream()
//                        .flatMap(slice -> converter.convert(slice).stream())
//                        .filter(variant -> !variant.getType().equals(VariantType.NO_VARIATION))
//                        .collect(Collectors.toList())).then(variantsWriter);

        try (InputStream in = ioConnectorProvider.newInputStream(input)) {
            if (archiveWriter != null) {
                archiveWriter.open();
                archiveWriter.pre();
            }
            variantsWriter.open();
            variantsWriter.pre();
            if (sampleIndexDBLoader != null) {
                sampleIndexDBLoader.open();
                sampleIndexDBLoader.pre();
            }
            VcfSlice slice = VcfSlice.parseDelimitedFrom(in);
            while (null != slice) {
                ++counter;
                if (archiveWriter != null) {
                    archiveWriter.write(slice);
                }

                List<Variant> variants = converter.convert(slice);
                variants = VariantHadoopDBWriter.filterVariantsNotFromThisSlice(slice.getPosition(), variants);
                variantsWriter.write(variants);
                if (sampleIndexDBLoader != null) {
                    sampleIndexDBLoader.write(variants);
                }

                progressLogger.increment(slice.getRecordsCount());
                slice = VcfSlice.parseDelimitedFrom(in);
            }
            if (archiveWriter != null) {
                archiveWriter.post();
            }
            variantsWriter.post();
            if (sampleIndexDBLoader != null) {
                sampleIndexDBLoader.post();
            }
        } catch (IOException e) {
            throw new StorageEngineException("Problems reading " + input, e);
        } finally {
            if (archiveWriter != null) {
                archiveWriter.close();
            }
            variantsWriter.close();
            if (sampleIndexDBLoader != null) {
                sampleIndexDBLoader.close();
            }
        }
        logger.info("Read {} slices", counter);

        if (sampleIndexDBLoader != null) {
            // Update list of loaded genotypes
            this.loadedGenotypes = sampleIndexDBLoader.getLoadedGenotypes();
            this.sampleIndexVersion = sampleIndexDBLoader.getSampleIndexVersion();
        }
    }

    protected void loadFromAvro(URI input, URI outdir, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        if (YesNoAuto.parse(getOptions(), LOAD_ARCHIVE.key()).orYes().booleanValue()) {
            loadFromAvroWithArchive(input, outdir, helper, progressLogger);
        } else {
            loadFromAvroWithoutArchive(input, outdir, helper, progressLogger);
        }
    }

    protected void loadFromAvroWithArchive(URI input, URI outdir, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        int studyId = helper.getStudyId();
        int fileId = Integer.parseInt(helper.getFileMetadata().getId());

        // Config
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(1) // Increasing the numTasks may produce wrong results writing the sampleIndex
                .setBatchSize(1)
                .setReadQueuePutTimeout(1000).build();

        // Reader
        boolean stdin = options.getBoolean(STDIN.key(), STDIN.defaultValue());
        int sliceBufferSize = options.getInt(ARCHIVE_SLICE_BUFFER_SIZE.key(), ARCHIVE_SLICE_BUFFER_SIZE.defaultValue());
        VariantReader variantReader = variantReaderUtils.getVariantReader(input, helper.getStudyMetadata(), stdin);
        AbstractDuplicatedVariantsResolver resolver = new DuplicatedVariantsResolverFactory(getOptions(), ioConnectorProvider)
                .getResolver(UriUtils.fileName(input), outdir);
        VariantDeduplicationTask dedupTask = new DuplicatedVariantsResolverFactory(getOptions(), ioConnectorProvider)
                .getTask(resolver);
        VariantSliceReader sliceReader = new VariantSliceReader(
                helper.getChunkSize(), variantReader.then(dedupTask), studyId, fileId, sliceBufferSize, progressLogger);

        // Archive Writer
        VariantHBaseArchiveDataWriter archiveWriter = newArchiveDBWriter(getArchiveTable(), helper);
        // Variants Writer
        VariantHadoopDBWriter hadoopDBWriter = newVariantHadoopDBWriter();
        // Sample Index Writer
        List<Integer> sampleIds = new ArrayList<>(getMetadataManager().getFileMetadata(studyId, fileId).getSamples());
        SampleIndexDBLoader sampleIndexDBLoader = newSampleIndexDBLoader(sampleIds);

        // TaskMetadata
        String archiveFields = options.getString(ARCHIVE_FIELDS.key());
        String nonRefFilter = options.getString(ARCHIVE_NON_REF_FILTER.key());
        // TODO: Move "SampleIndexDBLoader" to Write step so we can increase the number of threads
        GroupedVariantsTask task = new GroupedVariantsTask(archiveWriter, hadoopDBWriter, sampleIndexDBLoader,
                null, archiveFields, nonRefFilter);

        ParallelTaskRunner<ImmutablePair<Long, List<Variant>>, Object> ptr =
                new ParallelTaskRunner<>(sliceReader, task, null, config);
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error loading file " + input, e);
        }

        logLoadResults(variantReader.getVariantFileMetadata(), resolver, hadoopDBWriter);
        if (sampleIndexDBLoader != null) {
            // Update list of loaded genotypes
            this.loadedGenotypes = sampleIndexDBLoader.getLoadedGenotypes();
            this.sampleIndexVersion = sampleIndexDBLoader.getSampleIndexVersion();
        }
    }

    protected void loadFromAvroWithoutArchive(URI input, URI outdir, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {

        int studyId = helper.getStudyId();
        int fileId = Integer.parseInt(helper.getFileMetadata().getId());


        // Reader
        boolean stdin = options.getBoolean(STDIN.key(), STDIN.defaultValue());
        VariantReader variantReader = variantReaderUtils.getVariantReader(input, helper.getStudyMetadata(), stdin);
        AbstractDuplicatedVariantsResolver resolver = new DuplicatedVariantsResolverFactory(getOptions(), ioConnectorProvider)
                .getResolver(UriUtils.fileName(input), outdir);
        VariantDeduplicationTask dedupTask = new DuplicatedVariantsResolverFactory(getOptions(), ioConnectorProvider)
                .getTask(resolver);
        DataReader<Variant> reader = variantReader.then(dedupTask);

        // Variants Writer
        VariantHadoopDBWriter hadoopDBWriter = newVariantHadoopDBWriter();
        // Sample Index Writer
        List<Integer> sampleIds = new ArrayList<>(getMetadataManager().getFileMetadata(studyId, fileId).getSamples());
        SampleIndexDBLoader sampleIndexDBLoader = newSampleIndexDBLoader(sampleIds);

        Task<Variant, Variant> progressLoggerTask = progressLogger
                .asTask(variant -> "up to position " + variant.getChromosome() + ":" + variant.getStart());

        // Config
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(options.getInt(LOAD_THREADS.key(), LOAD_THREADS.defaultValue()))
                .setBatchSize(options.getInt(LOAD_BATCH_SIZE.key(), LOAD_BATCH_SIZE.defaultValue()))
                .setSorted(sampleIndexDBLoader != null)
                .setReadQueuePutTimeout(1000).build();
        ParallelTaskRunner<Variant, Variant> ptr =
                new ParallelTaskRunner<>(reader, hadoopDBWriter.asTask().then(progressLoggerTask), sampleIndexDBLoader, config);
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error loading file " + input, e);
        }

        logLoadResults(variantReader.getVariantFileMetadata(), resolver, hadoopDBWriter);
        if (sampleIndexDBLoader != null) {
            // Update list of loaded genotypes
            this.loadedGenotypes = sampleIndexDBLoader.getLoadedGenotypes();
            this.sampleIndexVersion = sampleIndexDBLoader.getSampleIndexVersion();
        }
    }

    private void logLoadResults(VariantFileMetadata variantFileMetadata,
                                AbstractDuplicatedVariantsResolver resolver,
                                VariantHadoopDBWriter hadoopDBWriter) {
        logLoadResults(variantFileMetadata,
                resolver.getDuplicatedVariants(),
                resolver.getDuplicatedLocus(),
                resolver.getDiscardedVariants(),
                hadoopDBWriter.getSkippedRefBlock() + resolver.getExtraRefBlockDiscardedVariants(),
                hadoopDBWriter.getLoadedVariants(),
                hadoopDBWriter.getSkippedRefVariants());
    }

    private void logLoadResults(VariantFileMetadata variantFileMetadata, int duplicatedVariants, int duplicatedLocus, int discardedVariants,
                                int skipped, int loadedVariants, int skippedRefVariants) {
        getLoadStats().put("duplicatedVariants", duplicatedVariants);
        getLoadStats().put("duplicatedLocus", duplicatedLocus);
        getLoadStats().put("discardedVariants", discardedVariants);
        getLoadStats().put("skippedRefVariants", skippedRefVariants);
        getLoadStats().put("loadedVariants", loadedVariants);
        getLoadStats().put("skipped", skipped);

        // TODO: Check if the expectedCount matches with the count from HBase?
        // @see this.checkLoadedVariants
        logger.info("============================================================");
        int expectedCount = 0;
        for (VariantType variantType : TARGET_VARIANT_TYPE_SET) {
            expectedCount += variantFileMetadata.getStats().getTypeCount().getOrDefault(variantType.toString(), 0L);
        }
        expectedCount -= discardedVariants;
        expectedCount -= skippedRefVariants;
        getLoadStats().put("expectedVariants", expectedCount);
        if (expectedCount == loadedVariants) {
            logger.info("Number of loaded variants: " + loadedVariants);
        } else {
            logger.warn("Wrong number of loaded variants. Expected: " + expectedCount + " but loaded " + loadedVariants);
        }
        if (duplicatedVariants > 0) {
            logger.warn("Found {} duplicated variants in {} different locations. {} variants were discarded.",
                    duplicatedVariants, duplicatedLocus, discardedVariants);
        }
        if (skipped > 0) {
            logger.info("There were " + skipped + " skipped variants");
            for (VariantType type : VariantType.values()) {
                if (!TARGET_VARIANT_TYPE_SET.contains(type)) {
                    Long countByType = variantFileMetadata.getStats().getTypeCount().get(type.toString());
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

        VariantStorageMetadataManager metadataManager = getMetadataManager();

        // Mark the load task as READY
        metadataManager.setStatus(getStudyId(), taskId, Status.READY);

        boolean loadSampleIndex = YesNoAuto.parse(getOptions(), LOAD_SAMPLE_INDEX.key()).orYes().booleanValue();
        if (loadSampleIndex) {
            for (Integer sampleId : metadataManager.getSampleIdsFromFileId(getStudyId(), getFileId())) {
                // Worth to check first to avoid too many updates in scenarios like 1000G
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(getStudyId(), sampleId);
                if (sampleMetadata.getSampleIndexStatus(sampleIndexVersion) != Status.READY) {
                    metadataManager.updateSampleMetadata(getStudyId(), sampleId,
                            s -> s.setSampleIndexStatus(Status.READY, sampleIndexVersion));
                }
            }
        }
        boolean loadArchive = YesNoAuto.parse(getOptions(), LOAD_ARCHIVE.key()).orYes().booleanValue();
        if (loadArchive) {
            metadataManager.updateFileMetadata(getStudyId(), getFileId(), fileMetadata -> {
                fileMetadata.getAttributes().put(LOAD_ARCHIVE.key(), true);
                fileMetadata.getAttributes().remove("TASK-633"); // most likely this field doesn't exist.
            });
        }

        return uri;
    }

    @Override
    protected void securePostLoad(List<Integer> fileIds, StudyMetadata studyMetadata) throws StorageEngineException {
        super.securePostLoad(fileIds, studyMetadata);

        if (loadedGenotypes != null) {
            loadedGenotypes.addAll(studyMetadata.getAttributes().getAsStringList(VariantStorageOptions.LOADED_GENOTYPES.key()));
            studyMetadata.getAttributes().put(VariantStorageOptions.LOADED_GENOTYPES.key(), loadedGenotypes);
        }
    }

    private VariantHBaseArchiveDataWriter newArchiveDBWriter(String table, ArchiveTableHelper helper) {
        if (YesNoAuto.parse(getOptions(), LOAD_ARCHIVE.key()).orYes().booleanValue()) {
            return new VariantHBaseArchiveDataWriter(helper, table, dbAdaptor.getHBaseManager());
        } else {
            return null;
        }
    }

    private SampleIndexDBLoader newSampleIndexDBLoader(List<Integer> sampleIds) throws StorageEngineException {
        boolean loadSampleIndex = YesNoAuto.parse(getOptions(), LOAD_SAMPLE_INDEX.key()).orYes().booleanValue();
        if (!loadSampleIndex || sampleIds.isEmpty()) {
            return null;
        }
        SampleIndexDBLoader sampleIndexDBLoader;
        SampleIndexDBAdaptor sampleIndexDbAdaptor = new SampleIndexDBAdaptor(
                dbAdaptor.getHBaseManager(), dbAdaptor.getTableNameGenerator(), getMetadataManager());
        sampleIndexDBLoader = new SampleIndexDBLoader(sampleIndexDbAdaptor, dbAdaptor.getHBaseManager(),
                getMetadataManager(),
                getStudyId(), getFileId(), sampleIds,
                VariantStorageEngine.SplitData.from(getOptions()),
                getOptions(), sampleIndexDbAdaptor.getSchemaLatest(getStudyId()));
        return sampleIndexDBLoader;
    }

    private VariantHadoopDBWriter newVariantHadoopDBWriter() throws StorageEngineException {
        boolean includeReferenceVariantsData = getOptions().getBoolean(
                VARIANT_TABLE_LOAD_REFERENCE.key(),
                VARIANT_TABLE_LOAD_REFERENCE.defaultValue());
        YesNoAuto includeGenotype = YesNoAuto.parse(getOptions(), INCLUDE_GENOTYPE.key());
        boolean excludeGenotypes = includeGenotype == YesNoAuto.NO;

        return new VariantHadoopDBWriter(
                dbAdaptor.getVariantTable(),
                getStudyId(),
                getFileId(),
                getMetadataManager(),
                dbAdaptor.getHBaseManager(), includeReferenceVariantsData, excludeGenotypes);
    }

    protected static class GroupedVariantsTask implements Task<ImmutablePair<Long, List<Variant>>, Object> {
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
        public List<Object> apply(List<ImmutablePair<Long, List<Variant>>> batch) {
            for (ImmutablePair<Long, List<Variant>> pair : batch) {
                List<Variant> variants = VariantHadoopDBWriter.filterVariantsNotFromThisSlice(pair.getKey(), pair.getValue());
                hadoopDBWriter.write(variants);

                if (sampleIndexDBLoader != null) {
                    sampleIndexDBLoader.write(variants);
                }
            }

            List<VcfSlice> slices = converterTask.apply(batch);
            archiveWriter.write(slices);

            return null;
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
