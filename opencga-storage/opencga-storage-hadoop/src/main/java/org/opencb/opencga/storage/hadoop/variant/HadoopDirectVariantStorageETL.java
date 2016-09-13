package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.opencb.biodata.formats.io.FileFormatException;
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
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
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
import java.util.zip.GZIPInputStream;

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

    @Override
    public URI preTransform(URI input) throws StorageManagerException, IOException, FileFormatException {
        if (StringUtils.isEmpty(options.getString(Options.TRANSFORM_FORMAT.key()))) {
            options.put(Options.TRANSFORM_FORMAT.key(), "proto");
        }
        return super.preTransform(input);
    }

    /**
     * Read from VCF file, group by slice and insert into HBase table.
     *
     * @param inputUri {@link URI}
     * @throws StorageManagerException if the load fails
     */
    protected void loadArch(URI inputUri) throws StorageManagerException {
        Path input = Paths.get(inputUri.getPath());
        String table = archiveTableCredentials.getTable();
        String fileName = input.getFileName().toString();
        Path sourcePath = input.getParent().resolve(VariantReaderUtils.getMetaFromTransformedFile(fileName));

        if (!VariantReaderUtils.isProto(fileName)) {
            throw new NotImplementedException("Direct loading only available for PROTO files.");
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
