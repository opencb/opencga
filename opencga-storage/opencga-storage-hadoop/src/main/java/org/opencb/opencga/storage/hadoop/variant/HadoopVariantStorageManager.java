package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveFileMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDeletionDriver;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by mh719 on 16/06/15.
 */
public class HadoopVariantStorageManager extends VariantStorageManager {
    public static final String STORAGE_ENGINE_ID = "hadoop";

    public static final String HADOOP_BIN = "hadoop.bin";
    public static final String HADOOP_ENV = "hadoop.env";
    public static final String OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES = "opencga.storage.hadoop.jar-with-dependencies";
    public static final String HADOOP_LOAD_ARCHIVE = "hadoop.load.archive";
    public static final String HADOOP_LOAD_VARIANT = "hadoop.load.variant";
    public static final String HADOOP_DELETE_FILE = "hadoop.delete.file";
    //Other files to be loaded from Archive to Variant
    public static final String HADOOP_LOAD_VARIANT_PENDING_FILES = "opencga.storage.hadoop.load.pending.files";
    public static final String OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY = "opencga.storage.hadoop.intermediate.hdfs.directory";

    protected Configuration conf = null;
    protected MRExecutor mrExecutor;
    private final HdfsVariantReaderUtils variantReaderUtils;

    public HadoopVariantStorageManager() {
        variantReaderUtils = new HdfsVariantReaderUtils(conf);
    }

    public void remove() throws StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        int studyId = options.getInt(VariantStorageManager.Options.STUDY_ID.key());
        Integer fileId = options.getInt(VariantStorageManager.Options.FILE_ID.key());

        String archiveTable = getTableName(studyId);
        HadoopCredentials variantsTable = getDbCredentials();
        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jar = options.getString(OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES, null);
        if (jar == null) {
            throw new StorageManagerException("Missing option " + OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES);
        }

        Class execClass = VariantTableDeletionDriver.class;
        String args = VariantTableDeletionDriver.buildCommandLineArgs(variantsTable.getHostAndPort(), archiveTable,
                variantsTable.getTable(), studyId, Collections.singletonList(fileId), options);
        String executable = hadoopRoute + " jar " + jar + ' ' + execClass.getName();

        long startTime = System.currentTimeMillis();
        logger.info("------------------------------------------------------");
        logger.info("Remove file IDs {} in analysis {} and archive table '{}'", fileId, archiveTable, variantsTable.getTable());
        logger.debug(executable + " " + args);
        logger.info("------------------------------------------------------");
        int exitValue = getMRExecutor(options).run(executable, args);
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
        if (exitValue != 0) {
            throw new StorageManagerException("Error removing fileId " + fileId + " from tables ");
        }
    }

    public MRExecutor getMRExecutor(ObjectMap options) {
        if (mrExecutor == null) {
            return new ExternalMRExecutor(options);
        } else {
            return mrExecutor;
        }
    }

    private HadoopCredentials getDbCredentials() throws StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        String dbName = options.getString(VariantStorageManager.Options.DB_NAME.key(), null);
        return buildCredentials(dbName);
    }

    @Override
    public VariantHadoopDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        return getDBAdaptor(buildCredentials(dbName));
    }

    public VariantHadoopDBAdaptor getDBAdaptor() throws StorageManagerException {
        return getDBAdaptor(getDbCredentials());
    }

    protected VariantHadoopDBAdaptor getDBAdaptor(HadoopCredentials credentials) throws StorageManagerException {
        try {
            return new VariantHadoopDBAdaptor(credentials, configuration.getStorageEngine(storageEngineId),
                    getHadoopConfiguration(configuration.getStorageEngine(storageEngineId).getOptions()));
        } catch (IOException e) {
            throw new StorageManagerException("Problems creating DB Adapter", e);
        }
    }

    public HadoopCredentials buildCredentials(String table) throws IllegalStateException {
        StorageEtlConfiguration vStore = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant();

        DatabaseCredentials db = vStore.getDatabase();
        String user = db.getUser();
        String pass = db.getPassword();
        List<String> hostList = db.getHosts();
        if (hostList.size() != 1) {
            throw new IllegalStateException("Expect only one server name");
        }
        String target = hostList.get(0);
        try {
            URI uri = new URI(target);
            String server = uri.getHost();
            Integer port = uri.getPort() > 0 ? uri.getPort() : 60000;
            //        String tablename = uri.getPath();
            //        tablename = tablename.startsWith("/") ? tablename.substring(1) : tablename; // Remove leading /
            //        String master = String.join(":", server, port.toString());
            HadoopCredentials credentials = new HadoopCredentials(server, table, user, pass, port);
            return credentials;
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public HadoopVariantStorageETL newStorageETL() throws StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        return new HadoopVariantStorageETL(configuration, storageEngineId, logger, getDBAdaptor(), getMRExecutor(options),
               getHadoopConfiguration(options), buildCredentials(getTableName(options.getInt(Options.STUDY_ID.key()))), variantReaderUtils);
    }

    @Override
    protected StudyConfigurationManager buildStudyConfigurationManager(ObjectMap options) throws StorageManagerException {
        try {
            HadoopCredentials dbCredentials = getDbCredentials();
            Configuration configuration = VariantHadoopDBAdaptor.getHbaseConfiguration(getHadoopConfiguration(options), dbCredentials);
            return new HBaseStudyConfigurationManager(dbCredentials, configuration, options);
        } catch (IOException e) {
            e.printStackTrace();
            return super.buildStudyConfigurationManager(options);
        }
    }

    private ArchiveFileMetadataManager buildArchiveFileMetaManager(String archiveTableName, ObjectMap options)
            throws StorageManagerException {
        return buildArchiveFileMetaManager(buildCredentials(archiveTableName), options);
    }

    private ArchiveFileMetadataManager buildArchiveFileMetaManager(HadoopCredentials archiveTableCredentials, ObjectMap options)
            throws StorageManagerException {
        try {
            Configuration configuration = VariantHadoopDBAdaptor.getHbaseConfiguration(getHadoopConfiguration(options),
                    archiveTableCredentials);
            return new ArchiveFileMetadataManager(archiveTableCredentials, configuration, options);
        } catch (IOException e) {
            throw new StorageManagerException("Unable to build ArchiveFileMetaManager", e);
        }
    }

    private Configuration getHadoopConfiguration(ObjectMap options) throws StorageManagerException {
        Configuration conf = this.conf == null ? new HdfsConfiguration() : this.conf;
        // This is the only key needed to connect to HDFS:
        //   CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY = fs.defaultFS
        //

        if (conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) == null) {
            throw new StorageManagerException("Missing configuration parameter \""
                    + CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY + "\"");
        }

        options.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> conf.set(entry.getKey(), options.getString(entry.getKey())));
        return conf;
    }

    /**
     * Get the archive table name given a StudyId.
     *
     * @param studyId Numerical study identifier
     * @return Table name
     */
    public static String getTableName(int studyId) {
        return HadoopVariantStorageETL.ARCHIVE_TABLE_PREFIX + Integer.toString(studyId);
    }

    public VariantSource readVariantSource(URI input) throws StorageManagerException {
        return variantReaderUtils.readVariantSource(input);
    }

    private static class HdfsVariantReaderUtils extends VariantReaderUtils {
        private final Configuration conf;

        public HdfsVariantReaderUtils(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public VariantSource readVariantSource(URI input) throws StorageManagerException {
            VariantSource source;

            if (input.getScheme() == null || input.getScheme().startsWith("file")) {
                return VariantReaderUtils.readVariantSource(Paths.get(input.getPath()), null);
            }

            Path metaPath = new Path(VariantReaderUtils.getMetaFromInputFile(input.toString()));
            try (
                    FileSystem fs = FileSystem.get(conf);
                    InputStream inputStream = new GZIPInputStream(fs.open(metaPath))
            ) {
                source = VariantReaderUtils.readVariantSource(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
                throw new StorageManagerException("Unable to read VariantSource", e);
            }
            return source;
        }
    }
}
