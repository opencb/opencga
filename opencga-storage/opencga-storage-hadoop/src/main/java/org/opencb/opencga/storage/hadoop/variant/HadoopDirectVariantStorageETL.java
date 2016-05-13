package org.opencb.opencga.storage.hadoop.variant;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageETL;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager.Options;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.exceptions.StorageHadoopException;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveFileMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHbasePutTask;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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

    @Override
    protected VariantSource readVariantSource(URI input, ObjectMap options) throws StorageManagerException {
        return buildVariantSource(Paths.get(input.getPath()), options);
    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageManagerException {
        boolean loadArch = options.getBoolean(HADOOP_LOAD_ARCHIVE);
        boolean loadVar = options.getBoolean(HADOOP_LOAD_VARIANT);

        int studyId = options.getInt(Options.STUDY_ID.key(), Options.STUDY_ID.defaultValue());
        long lock;
        try {
            lock = dbAdaptor.getStudyConfigurationManager().lockStudy(studyId, 10000, 10000);
        } catch (InterruptedException e) {
            throw new StorageManagerException("Problems with locking StudyConfiguration!!!");
        }

        StudyConfiguration studyConfiguration = checkOrCreateStudyConfiguration();

        VariantSource source = readVariantSource(input, options);
        if (loadArch) {
            Path inPath = Paths.get(input.getPath());
            source = loadVariantSource(inPath.getParent().resolve(
                    inPath.getFileName().toString().replace(".variants.proto.gz", ".file.json.gz")));
        }
        source.setStudyId(Integer.toString(studyId));

        /*
         * Before load file, check and add fileName to the StudyConfiguration.
         * FileID and FileName is read from the VariantSource
         * If fileId is -1, read fileId from Options
         * Will fail if:
         *     fileId is not an integer
         *     fileId was already in the studyConfiguration.indexedFiles
         *     fileId was already in the studyConfiguration.fileIds with a different fileName
         *     fileName was already in the studyConfiguration.fileIds with a different fileId
         */

        int fileId;
        String fileName = source.getFileName();
        try {
            fileId = Integer.parseInt(source.getFileId());
        } catch (NumberFormatException e) {
            throw new StorageManagerException("FileId '" + source.getFileId() + "' is not an integer", e);
        }

        if (fileId < 0) {
            fileId = options.getInt(Options.FILE_ID.key(), Options.FILE_ID.defaultValue());
        } else {
            int fileIdFromParams = options.getInt(Options.FILE_ID.key(), Options.FILE_ID.defaultValue());
            if (fileIdFromParams >= 0) {
                if (fileIdFromParams != fileId) {
                    if (!options.getBoolean(Options.OVERRIDE_FILE_ID.key(), Options.OVERRIDE_FILE_ID.defaultValue())) {
                        throw new StorageManagerException(
                                "Wrong fileId! Unable to load using fileId: "
                                        + fileIdFromParams + ". "
                                        + "The input file has fileId: " + fileId
                                        + ". Use " + Options.OVERRIDE_FILE_ID.key() + " to ignore original fileId.");
                    } else {
                        //Override the fileId
                        fileId = fileIdFromParams;
                    }
                }
            }
        }

        if (studyConfiguration.getIndexedFiles().isEmpty()) {
            // First indexed file
            // Use the EXCLUDE_GENOTYPES value from CLI. Write in StudyConfiguration.attributes
            boolean excludeGenotypes = options.getBoolean(
                    Options.EXCLUDE_GENOTYPES.key(), Options.EXCLUDE_GENOTYPES
                            .defaultValue());
            studyConfiguration.getAttributes().put(Options.EXCLUDE_GENOTYPES.key(), excludeGenotypes);
        } else {
            // Not first indexed file
            // Use the EXCLUDE_GENOTYPES value from StudyConfiguration. Ignore CLI value
            boolean excludeGenotypes = studyConfiguration.getAttributes()
                    .getBoolean(Options.EXCLUDE_GENOTYPES.key(), Options.EXCLUDE_GENOTYPES.defaultValue());
            options.put(Options.EXCLUDE_GENOTYPES.key(), excludeGenotypes);
        }


        fileId = checkNewFile(studyConfiguration, fileId, fileName);
        options.put(Options.FILE_ID.key(), fileId);
        studyConfiguration.getFileIds().put(source.getFileName(), fileId);
        studyConfiguration.getHeaders().put(fileId, source.getMetadata().get("variantFileHeader").toString()); //
// TODO laster

        checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options); // TODO ?
        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
        options.put(Options.STUDY_CONFIGURATION.key(), studyConfiguration);

        if (!loadArch && !loadVar) {
            loadArch = true;
            loadVar = true;
            options.put(HADOOP_LOAD_ARCHIVE, loadArch);
            options.put(HADOOP_LOAD_VARIANT, loadVar);
        }

        logger.info("Try to set Snappy " + Compression.Algorithm.SNAPPY.getName());
        String compressName = conf.get(
                ArchiveDriver.CONFIG_ARCHIVE_TABLE_COMPRESSION, Compression.Algorithm.SNAPPY
                        .getName());
        Algorithm compression = Compression.getCompressionAlgorithmByName(compressName);
        logger.info(
                String.format(
                        "Create table %s with %s %s", archiveTableCredentials.getTable(), compressName,
                        compression));
        try {
            this.dbAdaptor.getGenomeHelper().getHBaseManager().createTableIfNeeded(
                    archiveTableCredentials.getTable(), this.dbAdaptor.getGenomeHelper().getColumnFamily(), compression);
        } catch (IOException e1) {
            throw new RuntimeException("Issue creating table " + archiveTableCredentials.getTable(), e1);
        }

        if (loadVar) {
            // Load into variant table
            // Update the studyConfiguration with data from the Archive Table.
            // Reads the VcfMeta documents, and populates the StudyConfiguration
            // if needed.
            // Obtain the list of pending files.


            boolean missingFilesDetected = false;

            Set<Integer> files = null;
            ArchiveFileMetadataManager fileMetadataManager;
            try {
                fileMetadataManager = dbAdaptor.getArchiveFileMetadataManager(
                        getTableName(
                                studyConfiguration
                                        .getStudyId()), options);
                files = fileMetadataManager.getLoadedFiles();
            } catch (IOException e) {
                throw new StorageHadoopException("Unable to read loaded files", e);
            }

            List<Integer> pendingFiles = new LinkedList<>();

            for (Integer loadedFileId : files) {
                VcfMeta meta = null;
                try {
                    meta = fileMetadataManager.getVcfMeta(loadedFileId, options).first();
                } catch (IOException e) {
                    throw new StorageHadoopException("Unable to read file VcfMeta for file : " + loadedFileId, e);
                }

                Integer fileId1 = Integer.parseInt(source.getFileId());
                if (!studyConfiguration.getFileIds().inverse().containsKey(fileId1)) {
                    checkNewFile(studyConfiguration, fileId1, source.getFileName());
                    studyConfiguration.getFileIds().put(source.getFileName(), fileId1);
                    studyConfiguration.getHeaders().put(
                            fileId1, source.getMetadata().get("variantFileHeader")
                                    .toString());
                    checkAndUpdateStudyConfiguration(studyConfiguration, fileId1, source, options);
                    missingFilesDetected = true;
                }
                if (!studyConfiguration.getIndexedFiles().contains(fileId1)) {
                    pendingFiles.add(fileId1);
                }
            }
            if (missingFilesDetected) {
                // getStudyConfigurationManager(options).updateStudyConfiguration(studyConfiguration,
                // null);
                dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
            }

            if (!loadArch) {
                // If skip archive loading, input fileId must be already in
                // archiveTable, so "pending to be loaded"
                if (!pendingFiles.contains(fileId)) {
                    throw new StorageManagerException("File " + fileId + " is not loaded in archive table");
                }
            } else {
                // If don't skip archive, input fileId must not be pending,
                // because must not be in the archive table.
                if (pendingFiles.contains(fileId)) {
                    // set loadArch to false?
                    throw new StorageManagerException("File " + fileId + " is not loaded in archive table");
                } else {
                    pendingFiles.add(fileId);
                }
            }

            // If there are some given pending files, load only those files, not
            // all pending files
            List<Integer> givenPendingFiles = options.getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES);
            if (!givenPendingFiles.isEmpty()) {
                for (Integer pendingFile : givenPendingFiles) {
                    if (!pendingFiles.contains(pendingFile)) {
                        throw new StorageManagerException(
                                "File " + fileId + " is not pending to be loaded in variant"
                                        + " table");
                    }
                }
            } else {
                options.put(HADOOP_LOAD_VARIANT_PENDING_FILES, pendingFiles);
            }
        }
        dbAdaptor.getStudyConfigurationManager().unLockStudy(studyId, lock);
        return input;
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageManagerException {
//        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        if (options.getBoolean(HADOOP_LOAD_VARIANT)) {
            // Current StudyConfiguration may be outdated. Remove it.
            options.remove(VariantStorageManager.Options.STUDY_CONFIGURATION.key());

            int studyId = options.getInt(VariantStorageManager.Options.STUDY_ID.key());

            VariantPhoenixHelper phoenixHelper = new VariantPhoenixHelper(dbAdaptor.getGenomeHelper());
            try {
                phoenixHelper.registerNewStudy(dbAdaptor.getJdbcConnection(), variantsTableCredentials.getTable(), studyId);
            } catch (SQLException e) {
                throw new StorageManagerException("Unable to register study in Phoenix", e);
            }
            options.put(VariantStorageManager.Options.FILE_ID.key(), options.getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES));

            return super.postLoad(input, output);
        } else {
            System.out.println(Thread.currentThread().getName() + " - DO NOTHING!");
            return input;
        }
    }

    /**
     * Read from VCF file, group by slice and insert into HBase table.
     *
     * @param inputUri {@link URI}
     */
    @Override
    public URI load(URI inputUri) throws IOException, StorageManagerException {
        Path input = Paths.get(inputUri.getPath());
        int studyId = options.getInt(VariantStorageManager.Options.STUDY_ID.key());

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
        int studyId = options.getInt(VariantStorageManager.Options.STUDY_ID.key());

        VariantSource source = loadVariantSource(sourcePath);
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
            throw new IllegalStateException("Problems reading " + input, e);
        } finally {
            hbaseWriter.close();
        }
        long end = System.currentTimeMillis();
        logger.info("Read {} slices", counter);
        logger.info("end - start = " + (end - start) / 1000.0 + "s");

        try (ArchiveFileMetadataManager manager = new ArchiveFileMetadataManager(this.dbAdaptor.getConnection(), table, conf);) {
            manager.updateVcfMetaData(source);
            manager.updateLoadedFilesSummary(Collections.singletonList(fileId));
        } catch (IOException e) {
            throw new RuntimeException("Not able to store Variant Source for file!!!", e);
        }
    }

    private VariantSource loadVariantSource(Path sourcePath) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        try (InputStream ids = new GZIPInputStream(new BufferedInputStream(new FileInputStream(sourcePath.toFile())))) {
            return objectMapper.readValue(ids, VariantSource.class);
        } catch (IOException e) {
            throw new IllegalStateException("Problems reading " + sourcePath, e);
        }
    }

    @Override
    protected void checkLoadedVariants(URI input, int fileId, StudyConfiguration studyConfiguration, ObjectMap options)
            throws StorageManagerException {
        // TODO Auto-generated method stub

    }

}
