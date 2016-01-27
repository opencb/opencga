package org.opencb.opencga.storage.hadoop.variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.SQLException;
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
    public static final String OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY = "opencga.storage.hadoop.intermediate.hdfs.directory";

    protected static Logger logger = LoggerFactory.getLogger(HadoopVariantStorageManager.class);

    protected MRExecutor mrExecutor = null;
    protected Configuration conf = null;

    @Override
    public URI preLoad(URI input, URI output) throws StorageManagerException {
        super.preLoad(input, output);

        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        boolean loadArch = options.getBoolean(HADOOP_LOAD_ARCHIVE);
        boolean loadVar = options.getBoolean(HADOOP_LOAD_VARIANT);

        if (!loadArch && !loadVar) {
            loadArch = true;
            loadVar = true;
            options.put(HADOOP_LOAD_ARCHIVE, loadArch);
            options.put(HADOOP_LOAD_VARIANT, loadVar);
        }

        if (loadArch) {

            //TODO: CopyFromLocal input to HDFS
            if (!input.getScheme().equals("hdfs")) {
                if (!StringUtils.isEmpty(options.getString(OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY))) {
                    output = URI.create(options.getString(OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY));
                }
                if (output.getScheme() != null && !output.getScheme().equals("hdfs")) {
                    throw new StorageManagerException("Output must be in HDFS");
                }

                try {
                    Configuration conf = getHadoopConfiguration(options);
                    FileSystem fs = FileSystem.get(conf);
                    Path variantsOutputPath = new Path(output.resolve(Paths.get(input.getPath()).getFileName().toString()));
                    logger.info("Copy from {} to {}", new Path(input).toUri(), variantsOutputPath.toUri());
                    fs.copyFromLocalFile(false, new Path(input), variantsOutputPath);

                    URI fileInput = URI.create(getMetaFromInputFile(input.toString()));
                    Path fileOutputPath = new Path(output.resolve(Paths.get(fileInput.getPath()).getFileName().toString()));
                    logger.info("Copy from {} to {}", new Path(fileInput).toUri(), fileOutputPath.toUri());
                    fs.copyFromLocalFile(false, new Path(fileInput), fileOutputPath);

                    input = variantsOutputPath.toUri();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

//            throw new StorageManagerException("Input must be on hdfs. Automatically CopyFromLocal pending");
        }

        // TODO
//        if (loadVar) {
//        }

        return input;
    }

    public String getMetaFromInputFile(String input) {
        return input.replace("variants.", "file.").replace("file.avro", "file.json");
    }

    //TODO: Generalize this
    @Override
    protected VariantSource readVariantSource(URI input, ObjectMap options) throws StorageManagerException {
        VariantSource source;

        if (input.getScheme().startsWith("file")) {
            return readVariantSource(Paths.get(input.getPath()), null);
        }

        Configuration conf = getHadoopConfiguration(options);
        Path metaPath = new Path(getMetaFromInputFile(input.toString()));
        logger.debug("Loading meta file: {}", metaPath.toString());
        try (
                FileSystem fs = FileSystem.get(conf);
                InputStream inputStream = new GZIPInputStream(fs.open(metaPath))
        ) {
            source = new ObjectMapper().readValue(inputStream, VariantSource.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new StorageManagerException("Unable to read VariantSource", e);
        }
        return source;
    }

    @Override
    public URI preTransform(URI input) throws StorageManagerException, IOException, FileFormatException {
        logger.info("PreTransform: " + input);
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        options.put(Options.TRANSFORM_FORMAT.key(), "avro");
        return super.preTransform(input);
    }


    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        URI vcfMeta = URI.create(getMetaFromInputFile(input.toString()));

        HadoopCredentials archiveTable = buildCredentials(ArchiveHelper.getTableName(options.getInt(Options.STUDY_ID.key())));
        HadoopCredentials variantsTable = getDbCredentials();

        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jarOption = OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES;
        String jar = options.getString(jarOption, null);
        if (jar == null) {
            throw new StorageManagerException("Missing option " + jarOption);
        }


        boolean loadArch = options.getBoolean(HADOOP_LOAD_ARCHIVE);
        boolean loadVar = options.getBoolean(HADOOP_LOAD_VARIANT);

        if (loadArch) {
            // "Usage: %s [generic options] <avro> <avro-meta> <server> <output-table>
            Class execClass = ArchiveDriver.class;
            String executable = hadoopRoute + " jar " + jar + " " + execClass.getName();
            String args = ArchiveDriver.buildCommandLineArgs(input, vcfMeta, archiveTable.getHostAndPort(),
                    archiveTable.getTable(), options.getInt(Options.STUDY_ID.key()), options.getInt(Options.FILE_ID.key()));

            logger.debug("------------------------------------------------------");
            logger.debug(executable + " " + args);
            logger.debug("------------------------------------------------------");
            int exitValue = getMRExecutor(options).run(executable, args);
            logger.debug("------------------------------------------------------");
            logger.debug("Exit value: {}", exitValue);
            if (exitValue != 0) {
                throw new StorageManagerException("Error loading file " + input + " into archive table \""
                        + archiveTable.getTable() + "\"");
            }
        }

        if (loadVar) {
            // "Usage: %s [generic options] <server> <input-table> <output-table> <column>
            Class execClass = VariantTableDriver.class;
            String args = VariantTableDriver.buildCommandLineArgs(variantsTable.getHostAndPort(), archiveTable.getTable(),
                    variantsTable.getTable(), options.getInt(Options.STUDY_ID.key()), options.getInt(Options.FILE_ID.key()));
            String executable = hadoopRoute + " jar " + jar + ' ' + execClass.getName();

            logger.debug("------------------------------------------------------");
            logger.debug(executable + " " + args);
            logger.debug("------------------------------------------------------");
            int exitValue = getMRExecutor(options).run(executable, args);
            logger.debug("------------------------------------------------------");
            logger.debug("Exit value: {}", exitValue);
            if (exitValue != 0) {
                throw new StorageManagerException("Error loading file " + input + " into variant table \""
                        + variantsTable.getTable() + "\"");
            }
        }

        return input; // TODO  change return value?
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
        String dbName = options.getString(Options.DB_NAME.key(), null);
        HadoopCredentials cr = buildCredentials(dbName);
        return cr;
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
    public URI postLoad(URI input, URI output) throws IOException, StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        if (options.getBoolean(HADOOP_LOAD_VARIANT)) {
            HadoopCredentials dbCredentials = getDbCredentials();
            VariantHadoopDBAdaptor dbAdaptor = getDBAdaptor(dbCredentials);
            int studyId = options.getInt(Options.STUDY_ID.key());

            VariantPhoenixHelper phoenixHelper = new VariantPhoenixHelper(dbAdaptor.getGenomeHelper());
            try {
                phoenixHelper.registerNewStudy(dbAdaptor.getJdbcConnection(), dbCredentials.getTable(), studyId);
            } catch (SQLException e) {
                throw new StorageManagerException("Unable to register study in Phoenix", e);
            }


            return super.postLoad(input, output);
        } else {
            return input;
        }
    }

    @Override
    protected void checkLoadedVariants(URI input, int fileId, StudyConfiguration studyConfiguration, ObjectMap options) throws
            StorageManagerException {
        // TODO
    }

    @Override
    public URI postTransform(URI input) throws IOException, FileFormatException {
        return input; // TODO
    }

    @Override
    @Deprecated
    public VariantWriter getDBWriter(String dbName) throws StorageManagerException {
        return null;
    }

    public static Logger getLogger() {
        return logger;
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
}
