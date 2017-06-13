package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.tools.variant.converters.proto.VcfSliceToVariantListConverter;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.ParallelTaskRunner.Task;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHBaseArchiveDataWriter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHadoopDBWriter;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;

import static org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.HADOOP_LOAD_ARCHIVE;

/**
 * Created on 06/06/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopMergeBasicVariantStoragePipeline extends HadoopDirectVariantStoragePipeline {

    private final Logger logger = LoggerFactory.getLogger(HadoopMergeBasicVariantStoragePipeline.class);

    /**
     * @param configuration      {@link StorageConfiguration}
     * @param dbAdaptor          {@link VariantHadoopDBAdaptor}
     * @param conf               {@link Configuration}
     * @param archiveCredentials {@link HBaseCredentials}
     * @param variantReaderUtils {@link VariantReaderUtils}
     * @param options            {@link ObjectMap}
     */
    public HadoopMergeBasicVariantStoragePipeline(StorageConfiguration configuration,
                                                  VariantHadoopDBAdaptor dbAdaptor, Configuration conf,
                                                  HBaseCredentials archiveCredentials, VariantReaderUtils variantReaderUtils,
                                                  ObjectMap options) {
        super(configuration, dbAdaptor, null, conf, archiveCredentials, variantReaderUtils, options);
        loadArch = loadArch | loadVar;
        loadVar = false;
    }


    @Override
    protected void loadFromProto(Path input, String table, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        long counter = 0;

        VariantHBaseArchiveDataWriter archiveWriter = new VariantHBaseArchiveDataWriter(helper, table, dbAdaptor.getHBaseManager());
        VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(helper.getMeta());
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

    @Override
    protected void loadFromAvro(Path input, String table, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        VariantReader variantReader = VariantReaderUtils.getVariantReader(input, helper.getMeta().getVariantSource());
        VariantSliceReader sliceReader = new VariantSliceReader(helper.getChunkSize(), variantReader, progressLogger);

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(1).setBatchSize(1).build();

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
    public URI postLoad(URI input, URI output) throws StorageEngineException {
        final List<Integer> fileIds = getLoadedFiles();
        if (fileIds.isEmpty()) {
            logger.debug("Skip post load");
            return input;
        }
        registerLoadedFiles(fileIds);
        return input;
    }

    private VariantHadoopDBWriter newVariantHadoopDBWriter() throws StorageEngineException {
        StudyConfiguration studyConfiguration = getStudyConfiguration();
        return new VariantHadoopDBWriter(
                dbAdaptor.getGenomeHelper(),
                dbAdaptor.getCredentials().getTable(),
                studyConfiguration,
                dbAdaptor.getHBaseManager());
    }

    @Override
    protected void securePreMerge(StudyConfiguration studyConfiguration, VariantSource source) throws StorageEngineException {
    }

    @Override
    public void merge(int studyId, List<Integer> pendingFiles) throws StorageEngineException {
        logger.info("Nothing else to merge!");
    }

    @Override
    public URI postMerge(URI input, URI output) throws StorageEngineException {
        return input;
    }

    @Override
    protected void securePostMerge(List<Integer> fileIds, StudyConfiguration studyConfiguration) {
    }

    @Override
    protected List<Integer> getLoadedFiles() {
        List<Integer> fileIds;
        if (options.getBoolean(HADOOP_LOAD_ARCHIVE)) {
            fileIds = options.getAsIntegerList(VariantStorageEngine.Options.FILE_ID.key());
        } else {
            fileIds = Collections.emptyList();
        }
        return fileIds;
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
        public void pre() {
            archiveWriter.open();
            archiveWriter.pre();

            hadoopDBWriter.open();
            hadoopDBWriter.pre();

            converterTask.pre();
        }

        @Override
        public List<VcfSlice> apply(List<ImmutablePair<Long, List<Variant>>> batch) {
            List<VcfSlice> slices;
            slices = converterTask.apply(batch);
            for (ImmutablePair<Long, List<Variant>> pair : batch) {
                hadoopDBWriter.write(pair.getRight());
            }
            archiveWriter.write(slices);
            return slices;
        }

        @Override
        public void post() {
            archiveWriter.post();
            archiveWriter.close();

            hadoopDBWriter.post();
            hadoopDBWriter.close();

            converterTask.post();
        }
    }

}
