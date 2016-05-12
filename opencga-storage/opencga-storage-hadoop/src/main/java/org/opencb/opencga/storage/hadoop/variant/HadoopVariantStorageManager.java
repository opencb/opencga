package org.opencb.opencga.storage.hadoop.variant;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageETL;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveFileMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDeletionDriver;

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
    private HdfsVariantReaderUtils variantReaderUtils;

    public HadoopVariantStorageManager() {
//        variantReaderUtils = new HdfsVariantReaderUtils(conf);
    }

    @Override
    public List<StorageETLResult> index(List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean doTransform, boolean doLoad)
            throws StorageManagerException {

        if (inputFiles.size() == 1 || !doLoad) {
            return super.index(inputFiles, outdirUri, doExtract, doTransform, doLoad);
        }

        List<StorageETLResult> results = new ArrayList<>(inputFiles.size());

        // Check the database connection before we start
        if (doLoad) {
            testConnection();
        }
//        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        final int nThreads = 3;

        ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
        List<Future<StorageETLResult>> futures = new LinkedList<>();
        List<Integer> indexedFiles = Collections.synchronizedList(new ArrayList<>(nThreads));
        for (Iterator<URI> iterator = inputFiles.iterator(); iterator.hasNext();) {
            URI inputFile = iterator.next();
            //Provide a connected storageETL if load is required.
            ObjectMap extraOptions = new ObjectMap()
                    .append(HADOOP_LOAD_ARCHIVE, true)
                    .append(HADOOP_LOAD_VARIANT, false);
            VariantStorageETL storageETL = newStorageETL(doLoad, extraOptions);
            StorageETLResult storageETLResult = new StorageETLResult(inputFile);
            results.add(storageETLResult);
            futures.add(executorService.submit(() -> {
                URI nextUri = inputFile;
                boolean error = false;
                if (doTransform) {
                    try {
                        nextUri = transformFile(storageETL, storageETLResult, results, nextUri, outdirUri);
                    } catch (StorageETLException ignore) {
                        //Ignore here. Errors are stored in the ETLResult
                        error = true;
                    }
                }

                if (doLoad && !error) {
                    try {
                        loadFile(storageETL, storageETLResult, results, nextUri, outdirUri);
                        indexedFiles.add(storageETL.getOptions().getInt(Options.FILE_ID.key()));
                    } catch (StorageETLException ignore) {
                        //Ignore here. Errors are stored in the ETLResult
                    }
                }
                return storageETLResult;
            }));


            if (futures.size() % nThreads == 0 || !iterator.hasNext()) {
                try {
                    executorService.shutdown();
                    //FIXME: This is not a good idea
                    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
                    executorService = Executors.newFixedThreadPool(nThreads);
                } catch (InterruptedException e) {
                    throw new StorageETLException("Interrupted!", e, results);
                }
                int errors = 0;
                for (StorageETLResult result : results) {
                    if (result.getTransformError() != null) {
                        //TODO: Handle errors. Retry?
                        errors++;
                        result.getTransformError().printStackTrace();
                    } else if (result.getLoadError() != null) {
                        //TODO: Handle errors. Retry?
                        errors++;
                        result.getLoadError().printStackTrace();
                    }
                }
                if (errors > 0) {
                    throw new StorageETLException("Errors found", results);
                }
                int studyId = storageETL.getStudyConfiguration().getStudyId();

                storageETL.getOptions().put(HADOOP_LOAD_ARCHIVE, false);
                storageETL.getOptions().put(HADOOP_LOAD_VARIANT, true);
                //storageETL.merge(studyId, indexedFiles); // TODO enable again
                storageETL.postLoad(inputFile, outdirUri);

                indexedFiles.clear();
            }
        }
        return results;
    }

    @Override
    public VariantStorageETL newStorageETL(boolean connected) throws StorageManagerException {
        return newStorageETL(connected, null);
    }

    public VariantStorageETL newStorageETL(boolean connected, Map<? extends String, ?> extraOptions) throws StorageManagerException {
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        if (extraOptions != null) {
            options.putAll(extraOptions);
        }
        boolean directLoad = options.getBoolean("hadoop.load.direct", false);
        VariantHadoopDBAdaptor dbAdaptor = connected ? getDBAdaptor() : null;
        Configuration hadoopConfiguration = null == dbAdaptor ? null : dbAdaptor.getConfiguration();
        hadoopConfiguration = hadoopConfiguration == null ? getHadoopConfiguration(options) : hadoopConfiguration;
        hadoopConfiguration.setIfUnset(ArchiveDriver.CONFIG_ARCHIVE_TABLE_COMPRESSION, Algorithm.SNAPPY.getName());

        HBaseCredentials archiveCredentials = buildCredentials(getTableName(options.getInt(Options.STUDY_ID.key())));

        VariantStorageETL storageETL = null;
        if (directLoad) {
            storageETL = new HadoopDirectVariantStorageETL(configuration, storageEngineId, dbAdaptor, getMRExecutor(options),
                    hadoopConfiguration, archiveCredentials, getVariantReaderUtils(hadoopConfiguration), options);
        } else {
            storageETL = new HadoopVariantStorageETL(configuration, storageEngineId, dbAdaptor, getMRExecutor(options), hadoopConfiguration,
                    archiveCredentials, getVariantReaderUtils(hadoopConfiguration), options);
        }
        return storageETL;
    }

    private HdfsVariantReaderUtils getVariantReaderUtils(Configuration config) {
        if (null == variantReaderUtils) {
            variantReaderUtils = new HdfsVariantReaderUtils(config);
        } else if (this.variantReaderUtils.conf == null && config != null) {
            variantReaderUtils = new HdfsVariantReaderUtils(config);
        }
        return variantReaderUtils;
    }

    @Override
    public void dropFile(String study, int fileId) throws StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        VariantHadoopDBAdaptor dbAdaptor = getDBAdaptor();

        final int studyId;
        if (StringUtils.isNumeric(study)) {
            studyId = Integer.parseInt(study);
        } else {
            StudyConfiguration studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(study, null).first();
            studyId = studyConfiguration.getStudyId();
        }

        String archiveTable = getTableName(studyId);
        HBaseCredentials variantsTable = getDbCredentials();
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
        logger.info("Remove file ID {} in archive '{}' and analysis table '{}'", fileId, archiveTable, variantsTable.getTable());
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

    @Override
    public void dropStudy(String studyName) throws StorageManagerException {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public VariantHadoopDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        return getDBAdaptor(buildCredentials(dbName));
    }

    private HBaseCredentials getDbCredentials() throws StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        String dbName = options.getString(VariantStorageManager.Options.DB_NAME.key(), null);
        return buildCredentials(dbName);
    }


    public VariantHadoopDBAdaptor getDBAdaptor() throws StorageManagerException {
        return getDBAdaptor(getDbCredentials());
    }

    protected VariantHadoopDBAdaptor getDBAdaptor(HBaseCredentials credentials) throws StorageManagerException {
        try {
            return new VariantHadoopDBAdaptor(credentials, configuration.getStorageEngine(STORAGE_ENGINE_ID),
                    getHadoopConfiguration(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions()));
        } catch (IOException e) {
            throw new StorageManagerException("Problems creating DB Adapter", e);
        }
    }

    public HBaseCredentials buildCredentials(String table) throws IllegalStateException {
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
            HBaseCredentials credentials = new HBaseCredentials(server, table, user, pass, port);
            return credentials;
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }


    @Override
    protected StudyConfigurationManager buildStudyConfigurationManager(ObjectMap options) throws StorageManagerException {
        try {
            HBaseCredentials dbCredentials = getDbCredentials();
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

    private ArchiveFileMetadataManager buildArchiveFileMetaManager(HBaseCredentials archiveTableCredentials, ObjectMap options)
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
        Configuration conf = this.conf == null ? HBaseConfiguration.create() : this.conf;
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

    public MRExecutor getMRExecutor(ObjectMap options) {
        if (mrExecutor == null) {
            return new ExternalMRExecutor(options);
        } else {
            return mrExecutor;
        }
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
        return getVariantReaderUtils(null).readVariantSource(input);
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
