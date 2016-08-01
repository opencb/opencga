package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager.Options;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHbasePutTask;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
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
import java.util.zip.GZIPInputStream;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager.*;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class HadoopDirectVariantStorageETL extends AbstractHadoopVariantStorageETL {

    /**
     * @param configuration      {@link StorageConfiguration}
     * @param storageEngineId    Id
     * @param dbAdaptor          {@link VariantHadoopDBAdaptor}
     * @param mrExecutor         {@link MRExecutor}
     * @param conf               {@link Configuration}
     * @param archiveCredentials {@link HBaseCredentials}
     * @param variantReaderUtils {@link VariantReaderUtils}
     * @param options            {@link ObjectMap}
     */
    public HadoopDirectVariantStorageETL(
            StorageConfiguration configuration, String storageEngineId,
            VariantHadoopDBAdaptor dbAdaptor,
            MRExecutor mrExecutor, Configuration conf, HBaseCredentials
                    archiveCredentials, VariantReaderUtils variantReaderUtils,
            ObjectMap options) {
        super(
                configuration, storageEngineId, LoggerFactory.getLogger(HadoopDirectVariantStorageETL.class),
                dbAdaptor, variantReaderUtils,
                options, archiveCredentials, mrExecutor, conf);
    }

    /**
     * Read from VCF file, group by slice and insert into HBase table.
     *
     * @param inputUri {@link URI}
     */
    @Override
    public URI load(URI inputUri) throws IOException, StorageManagerException {
        Path input = Paths.get(inputUri.getPath());
        int studyId = getStudyId();

        ArchiveHelper.setChunkSize(
                conf, conf.getInt(
                        ArchiveDriver.CONFIG_ARCHIVE_CHUNK_SIZE, ArchiveDriver
                                .DEFAULT_CHUNK_SIZE));
        ArchiveHelper.setStudyId(conf, studyId);

        boolean loadArch = options.getBoolean(HADOOP_LOAD_ARCHIVE);
        boolean loadVar = options.getBoolean(HADOOP_LOAD_VARIANT);

        if (loadArch) {
            loadArch(input);
        }

        if (loadVar) {
            List<Integer> pendingFiles = options.getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES);
            merge(studyId, pendingFiles);
        }
        return inputUri;
    }

    private void loadArch(Path input) throws StorageManagerException, IOException {
        String table = archiveTableCredentials.getTable();
        String fileName = input.getFileName().toString();
        Path sourcePath = input.getParent().resolve(fileName.replace(".variants.proto.gz", ".file.json.gz"));
        boolean includeSrc = false;

        String format = options.getString(Options.TRANSFORM_FORMAT.key(), Options.TRANSFORM_FORMAT.defaultValue());
        if (!StringUtils.equalsIgnoreCase(format, "proto")) {
            throw new org.apache.commons.lang3.NotImplementedException("Direct loading only available for PROTO files");
        }

        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
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
        VcfMeta meta = new VcfMeta(source);
        ArchiveHelper helper = new ArchiveHelper(dbAdaptor.getGenomeHelper(), meta);


        VariantHbasePutTask hbaseWriter = new VariantHbasePutTask(helper, table);
        long counter = 0;
        long start = System.currentTimeMillis();
        try (InputStream in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(input.toFile())))) {
            hbaseWriter.open();
            hbaseWriter.pre();
            VcfSlice slice = VcfSlice.parseDelimitedFrom(in);
            while (null != slice) {
                ++counter;
                hbaseWriter.write(slice);
                slice = VcfSlice.parseDelimitedFrom(in);
            }
            hbaseWriter.post();
        } catch (IOException e) {
            throw new StorageManagerException("Problems reading " + input, e);
        } finally {
            hbaseWriter.close();
        }
        long end = System.currentTimeMillis();
        logger.info("Read {} slices", counter);
        logger.info("end - start = " + (end - start) / 1000.0 + "s");

        HadoopVariantSourceDBAdaptor manager = dbAdaptor.getVariantSourceDBAdaptor();
        try {
            manager.updateVariantSource(source);
            manager.updateLoadedFilesSummary(studyId, Collections.singletonList(fileId));
        } catch (IOException e) {
            throw new StorageManagerException("Not able to store Variant Source for file!!!", e);
        }
    }

    @Override
    protected boolean needLoadFromHdfs() {
        return false;
    }

}
