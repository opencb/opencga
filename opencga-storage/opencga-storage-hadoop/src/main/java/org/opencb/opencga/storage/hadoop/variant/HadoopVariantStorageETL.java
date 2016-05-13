package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.conf.Configuration;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.exceptions.StorageHadoopException;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveFileMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager.*;

/**
 * Created on 31/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStorageETL extends AbstractHadoopVariantStorageETL {


    public HadoopVariantStorageETL(
            StorageConfiguration configuration, String storageEngineId,
            VariantHadoopDBAdaptor dbAdaptor, MRExecutor mrExecutor,
            Configuration conf, HBaseCredentials archiveCredentials,
            VariantReaderUtils variantReaderUtils, ObjectMap options) {
        super(configuration, storageEngineId, LoggerFactory.getLogger(HadoopVariantStorageETL.class), dbAdaptor, variantReaderUtils,
                options, archiveCredentials, mrExecutor, conf);
    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageManagerException {

//        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        boolean loadArch = options.getBoolean(HADOOP_LOAD_ARCHIVE);
        boolean loadVar = options.getBoolean(HADOOP_LOAD_VARIANT);

        if (!loadArch && !loadVar) {
            loadArch = true;
            loadVar = true;
            options.put(HADOOP_LOAD_ARCHIVE, loadArch);
            options.put(HADOOP_LOAD_VARIANT, loadVar);
        }

        if (loadArch) {
            super.preLoad(input, output);
        }

        if (loadVar) {
            // Load into variant table
            // Update the studyConfiguration with data from the Archive Table.
            // Reads the VcfMeta documents, and populates the StudyConfiguration if needed.
            // Obtain the list of pending files.

            int studyId = options.getInt(VariantStorageManager.Options.STUDY_ID.key());
            int fileId = options.getInt(VariantStorageManager.Options.FILE_ID.key());
            boolean missingFilesDetected = false;

            String tableName = getTableName(studyId);

            Set<Integer> files = null;
            ArchiveFileMetadataManager fileMetadataManager;
            try {
                fileMetadataManager = dbAdaptor.getArchiveFileMetadataManager(tableName, options);
                files = fileMetadataManager.getLoadedFiles();
            } catch (IOException e) {
                throw new StorageHadoopException("Unable to read loaded files", e);
            }
            logger.info(String.format("Found files in DB: " + files));
            StudyConfiguration studyConfiguration = checkOrCreateStudyConfiguration();
            List<Integer> pendingFiles = new LinkedList<>();
            logger.info("Found registered indexed files: {}", studyConfiguration.getIndexedFiles());
            for (Integer loadedFileId : files) {
                VcfMeta meta = null;
                try {
                    meta = fileMetadataManager.getVcfMeta(loadedFileId, options).first();
                } catch (IOException e) {
                    throw new StorageHadoopException("Unable to read file VcfMeta for file : " + loadedFileId, e);
                }

                VariantSource source = meta.getVariantSource();
                Integer fileId1 = Integer.parseInt(source.getFileId());
                logger.info("Found source for file id {} with registered id {} ", loadedFileId, fileId1);
                if (!studyConfiguration.getFileIds().inverse().containsKey(fileId1)) {
                    checkNewFile(studyConfiguration, fileId1, source.getFileName());
                    studyConfiguration.getFileIds().put(source.getFileName(), fileId1);
                    studyConfiguration.getHeaders().put(fileId1, source.getMetadata().get("variantFileHeader").toString());
                    checkAndUpdateStudyConfiguration(studyConfiguration, fileId1, source, options);
                    missingFilesDetected = true;
                }
                if (!studyConfiguration.getIndexedFiles().contains(fileId1)) {
                    pendingFiles.add(fileId1);
                }
            }

            logger.info(String.format("Found pending in DB: " + pendingFiles));
            if (missingFilesDetected) {
                dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
            }

            if (!loadArch) {
                //If skip archive loading, input fileId must be already in archiveTable, so "pending to be loaded"
                if (!pendingFiles.contains(fileId)) {
                    throw new StorageManagerException("File " + fileId + " is not loaded in archive table " + tableName);
                }
            } else {
                //If don't skip archive, input fileId must not be pending, because must not be in the archive table.
                if (pendingFiles.contains(fileId)) {
                    // set loadArch to false?
                    throw new StorageManagerException("File " + fileId + " is not loaded in archive table");
                } else {
                    pendingFiles.add(fileId);
                }
            }

            //If there are some given pending files, load only those files, not all pending files
            List<Integer> givenPendingFiles = options.getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES);
            if (!givenPendingFiles.isEmpty()) {
                for (Integer pendingFile : givenPendingFiles) {
                    if (!pendingFiles.contains(pendingFile)) {
                        throw new StorageManagerException("File " + fileId + " is not pending to be loaded in variant table");
                    }
                }
            } else {
                options.put(HADOOP_LOAD_VARIANT_PENDING_FILES, pendingFiles);
            }
        }

        return input;
    }

    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
//        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        URI vcfMeta = URI.create(VariantReaderUtils.getMetaFromInputFile(input.toString()));

        int studyId = options.getInt(VariantStorageManager.Options.STUDY_ID.key());
        int fileId = options.getInt(VariantStorageManager.Options.FILE_ID.key());

        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jar = options.getString(OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES, null);
        if (jar == null) {
            throw new StorageManagerException("Missing option " + OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES);
        }


        boolean loadArch = options.getBoolean(HADOOP_LOAD_ARCHIVE);
        boolean loadVar = options.getBoolean(HADOOP_LOAD_VARIANT);

        if (loadArch) {
            Class execClass = ArchiveDriver.class;
            String executable = hadoopRoute + " jar " + jar + " " + execClass.getName();
            String args = ArchiveDriver.buildCommandLineArgs(input, vcfMeta, archiveTableCredentials.getHostAndPort(),
                    archiveTableCredentials.getTable(), studyId, fileId, options);

            long startTime = System.currentTimeMillis();
            logger.info("------------------------------------------------------");
            logger.info("Loading file {} into archive table '{}'", fileId, archiveTableCredentials.getTable());
            logger.debug(executable + " " + args);
            logger.info("------------------------------------------------------");
            int exitValue = mrExecutor.run(executable, args);
            logger.info("------------------------------------------------------");
            logger.info("Exit value: {}", exitValue);
            logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
            if (exitValue != 0) {
                throw new StorageManagerException("Error loading file " + input + " into archive table \""
                        + archiveTableCredentials.getTable() + "\"");
            }
        }

        if (loadVar) {
            List<Integer> pendingFiles = options.getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES);
            merge(studyId, pendingFiles);
        }

        return input; // TODO  change return value?
    }


    @Override
    public URI postLoad(URI input, URI output) throws StorageManagerException {
//        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        if (options.getBoolean(HADOOP_LOAD_VARIANT)) {
            // Current StudyConfiguration may be outdated. Remove it.
            options.remove(VariantStorageManager.Options.STUDY_CONFIGURATION.key());

//            HadoopCredentials dbCredentials = getDbCredentials();
//            VariantHadoopDBAdaptor dbAdaptor = getDBAdaptor(dbCredentials);
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

}
