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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHBaseArchiveDataWriter;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader;
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
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class HadoopDirectVariantStoragePipeline extends AbstractHadoopVariantStoragePipeline {

    private final Logger logger = LoggerFactory.getLogger(HadoopDirectVariantStoragePipeline.class);

    /**
     * @param configuration      {@link StorageConfiguration}
     * @param dbAdaptor          {@link VariantHadoopDBAdaptor}
     * @param mrExecutor         {@link MRExecutor}
     * @param conf               {@link Configuration}
     * @param archiveCredentials {@link HBaseCredentials}
     * @param variantReaderUtils {@link VariantReaderUtils}
     * @param options            {@link ObjectMap}
     */
    public HadoopDirectVariantStoragePipeline(
            StorageConfiguration configuration,
            VariantHadoopDBAdaptor dbAdaptor,
            MRExecutor mrExecutor, Configuration conf, HBaseCredentials
                    archiveCredentials, VariantReaderUtils variantReaderUtils,
            ObjectMap options) {
        super(configuration, dbAdaptor, variantReaderUtils, options, archiveCredentials, mrExecutor, conf);
    }

    @Override
    public URI preTransform(URI input) throws StorageEngineException, IOException, FileFormatException {
        if (StringUtils.isEmpty(options.getString(Options.TRANSFORM_FORMAT.key()))) {
            // Use proto by default
            options.put(Options.TRANSFORM_FORMAT.key(), "proto");
        }
        return super.preTransform(input);
    }

    /**
     * Read from VCF file, group by slice and insert into HBase table.
     *
     * @param inputUri {@link URI}
     * @throws StorageEngineException if the load fails
     */
    protected void loadArch(URI inputUri) throws StorageEngineException {
        Path input = Paths.get(inputUri.getPath());
        String table = archiveTableCredentials.getTable();
        String fileName = input.getFileName().toString();
        Path sourcePath = input.getParent().resolve(VariantReaderUtils.getMetaFromTransformedFile(fileName));

        Integer fileId;
        if (options.getBoolean(
                Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(),
                Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.defaultValue())) {
            fileId = Options.FILE_ID.defaultValue();
        } else {
            fileId = options.getInt(Options.FILE_ID.key());
        }
        int studyId = getStudyId();

        VariantSource source = VariantReaderUtils.readVariantSource(sourcePath, null);
        source.setFileId(fileId.toString());
        source.setStudyId(Integer.toString(studyId));

        long start = System.currentTimeMillis();
        if (VariantReaderUtils.isProto(fileName)) {
            ArchiveTableHelper helper = new ArchiveTableHelper(dbAdaptor.getGenomeHelper(), source);

            ProgressLogger progressLogger = new ProgressLogger("Loaded slices:");
            if (source.getStats() != null) {
                progressLogger.setApproximateTotalCount(source.getStats().getNumRecords());
            }

            loadFromProto(input, table, helper, progressLogger);
        } else {
            ArchiveTableHelper helper = new ArchiveTableHelper(dbAdaptor.getGenomeHelper(), source);

            ProgressLogger progressLogger;
            if (source.getStats() != null) {
                progressLogger = new ProgressLogger("Loaded variants for file \"" + input.getFileName() + "\" :",
                        source.getStats().getNumRecords());
            } else {
                progressLogger = new ProgressLogger("Loaded variants for file \"" + input.getFileName() + "\" :");
            }

            loadFromAvro(input, table, helper, progressLogger);
        }
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");

        HadoopVariantSourceDBAdaptor manager = dbAdaptor.getVariantSourceDBAdaptor();
        try {
            manager.updateVariantSource(source);
            manager.updateLoadedFilesSummary(studyId, Collections.singletonList(fileId));
        } catch (IOException e) {
            throw new StorageEngineException("Not able to store Variant Source for file!!!", e);
        }
    }

    protected void loadFromProto(Path input, String table, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        long counter = 0;
        VariantHBaseArchiveDataWriter archiveWriter = new VariantHBaseArchiveDataWriter(helper, table, dbAdaptor.getHBaseManager());
        try (InputStream in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(input.toFile())))) {
            archiveWriter.open();
            archiveWriter.pre();
            VcfSlice slice = VcfSlice.parseDelimitedFrom(in);
            while (null != slice) {
                ++counter;
                archiveWriter.write(slice);
                progressLogger.increment(slice.getRecordsCount());
                slice = VcfSlice.parseDelimitedFrom(in);
            }
            archiveWriter.post();
        } catch (IOException e) {
            throw new StorageEngineException("Problems reading " + input, e);
        } finally {
            archiveWriter.close();
        }
        logger.info("Read {} slices", counter);
    }


    protected void loadFromAvro(Path input, String table, ArchiveTableHelper helper, ProgressLogger progressLogger)
            throws StorageEngineException {
        VariantReader variantReader = VariantReaderUtils.getVariantReader(input, helper.getMeta().getVariantSource());
        VariantSliceReader sliceReader = new VariantSliceReader(helper.getChunkSize(), variantReader, progressLogger);

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(1).setBatchSize(1).build();

        VariantHBaseArchiveDataWriter archiveWriter = new VariantHBaseArchiveDataWriter(helper, table, dbAdaptor.getHBaseManager());
        VariantToVcfSliceConverterTask converterTask = new VariantToVcfSliceConverterTask();

        ParallelTaskRunner<ImmutablePair<Long, List<Variant>>, VcfSlice> ptr =
                new ParallelTaskRunner<>(sliceReader, converterTask, archiveWriter, config);
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error loading file " + input, e);
        }
    }

    @Override
    protected boolean needLoadFromHdfs() {
        return false;
    }

}
