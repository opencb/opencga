package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageETL;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.executors.ExternalMRExecutor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDeletionDriver;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
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
    public static final String HADOOP_LOAD_VARIANT_RESUME = "hadoop.load.variant.resume";
    //Other files to be loaded from Archive to Variant
    public static final String HADOOP_LOAD_VARIANT_PENDING_FILES = "opencga.storage.hadoop.load.pending.files";
    public static final String OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY = "opencga.storage.hadoop.intermediate.hdfs.directory";
    public static final String OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE = "opencga.storage.hadoop.hbase.namespace";
    public static final String OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX = "opencga.storage.hadoop.hbase.archive.table.prefix";
    public static final String OPENCGA_STORAGE_HADOOP_MAPREDUCE_SCANNER_TIMEOUT = "opencga.storage.hadoop.mapreduce.scanner.timeout";

    public static final String HADOOP_LOAD_ARCHIVE_BATCH_SIZE = "hadoop.load.archive.batch.size";
    public static final String HADOOP_LOAD_VARIANT_BATCH_SIZE = "hadoop.load.variant.batch.size";
    public static final String HADOOP_LOAD_DIRECT = "hadoop.load.direct";
    public static final boolean HADOOP_LOAD_DIRECT_DEFAULT = true;

    public static final String EXTERNAL_MR_EXECUTOR = "opencga.external.mr.executor";
    public static final String ARCHIVE_TABLE_PREFIX = "opencga_study_";


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

        final boolean doArchive;
        final boolean doMerge;


        if (!getOptions().containsKey(HADOOP_LOAD_ARCHIVE) && !getOptions().containsKey(HADOOP_LOAD_VARIANT)) {
            doArchive = true;
            doMerge = true;
        } else {
            doArchive = getOptions().getBoolean(HADOOP_LOAD_ARCHIVE, false);
            doMerge = getOptions().getBoolean(HADOOP_LOAD_VARIANT, false);
        }

        if (!doArchive && !doMerge) {
            return Collections.emptyList();
        }

        final int nThreadArchive = getOptions().getInt(HADOOP_LOAD_ARCHIVE_BATCH_SIZE, 2);
        ObjectMap extraOptions = new ObjectMap()
                .append(HADOOP_LOAD_ARCHIVE, true)
                .append(HADOOP_LOAD_VARIANT, false);

        final List<StorageETLResult> concurrResult = new CopyOnWriteArrayList<>();
        List<VariantStorageETL> etlList = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(
                nThreadArchive,
                r -> {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    return t;
                }); // Set Daemon for quick shutdown !!!
        LinkedList<Future<StorageETLResult>> futures = new LinkedList<>();
        List<Integer> indexedFiles = new CopyOnWriteArrayList<>();
        for (URI inputFile : inputFiles) {
            //Provide a connected storageETL if load is required.

            VariantStorageETL storageETL = newStorageETL(doLoad, new ObjectMap(extraOptions));
            futures.add(executorService.submit(() -> {
                try {
                    Thread.currentThread().setName(Paths.get(inputFile).getFileName().toString());
                    StorageETLResult storageETLResult = new StorageETLResult(inputFile);
                    URI nextUri = inputFile;
                    boolean error = false;
                    if (doTransform) {
                        try {
                            nextUri = transformFile(storageETL, storageETLResult, concurrResult, nextUri, outdirUri);

                        } catch (StorageETLException ignore) {
                            //Ignore here. Errors are stored in the ETLResult
                            error = true;
                        }
                    }

                    if (doLoad && doArchive && !error) {
                        try {
                            loadFile(storageETL, storageETLResult, concurrResult, nextUri, outdirUri);
                        } catch (StorageETLException ignore) {
                            //Ignore here. Errors are stored in the ETLResult
                            error = true;
                        }
                    }
                    if (doLoad && !error) {
                        // Read the VariantSource to get the original fileName (it may be different from the
                        // nextUri.getFileName if this is the transformed file)
                        String fileName = storageETL.readVariantSource(nextUri, null).getFileName();
                        // Get latest study configuration from DB, might have been changed since
                        StudyConfiguration studyConfiguration = storageETL.getStudyConfiguration();
                        // Get file ID for the provided file name
                        Integer fileId = studyConfiguration.getFileIds().get(fileName);
                        indexedFiles.add(fileId);
                    }
                    return storageETLResult;
                } finally {
                    try {
                        storageETL.close();
                    } catch (StorageManagerException e) {
                        logger.error("Issue closing DB connection ", e);
                    }
                }
            }));
        }

        executorService.shutdown();

        int errors = 0;
        try {
            while (!futures.isEmpty()) {
                executorService.awaitTermination(1, TimeUnit.MINUTES);
                // Check values
                if (futures.peek().isDone() || futures.peek().isCancelled()) {
                    Future<StorageETLResult> first = futures.pop();
                    StorageETLResult result = first.get(1, TimeUnit.MINUTES);
                    if (result.getTransformError() != null) {
                        //TODO: Handle errors. Retry?
                        errors++;
                        result.getTransformError().printStackTrace();
                    } else if (result.getLoadError() != null) {
                        //TODO: Handle errors. Retry?
                        errors++;
                        result.getLoadError().printStackTrace();
                    }
                    concurrResult.add(result);
                }
            }
            if (errors > 0) {
                throw new StorageETLException("Errors found", concurrResult);
            }

            if (doLoad && doMerge) {
                int batchMergeSize = getOptions().getInt(HADOOP_LOAD_VARIANT_BATCH_SIZE, 10);
                // Overwrite default ID list with user provided IDs
                List<Integer> pendingFiles = indexedFiles;
                if (getOptions().containsKey(HADOOP_LOAD_VARIANT_PENDING_FILES)) {
                    List<Integer> idList = getOptions().getAsIntegerList(HADOOP_LOAD_VARIANT_PENDING_FILES);
                    if (!idList.isEmpty()) {
                        // only if the list is not empty
                        pendingFiles = idList;
                    }
                }

                List<Integer> filesToMerge = new ArrayList<>(batchMergeSize);
                int i = 0;
                for (Iterator<Integer> iterator = pendingFiles.iterator(); iterator.hasNext(); i++) {
                    Integer indexedFile = iterator.next();
                    filesToMerge.add(indexedFile);
                    if (filesToMerge.size() == batchMergeSize || !iterator.hasNext()) {
                        extraOptions = new ObjectMap()
                                .append(HADOOP_LOAD_ARCHIVE, false)
                                .append(HADOOP_LOAD_VARIANT, true)
                                .append(HADOOP_LOAD_VARIANT_PENDING_FILES, filesToMerge);

                        AbstractHadoopVariantStorageETL localEtl = newStorageETL(doLoad, extraOptions);

                        int studyId = getOptions().getInt(Options.STUDY_ID.key());
                        localEtl.preLoad(inputFiles.get(i), outdirUri);
                        localEtl.merge(studyId, filesToMerge);
                        localEtl.postLoad(inputFiles.get(i), outdirUri);
                        filesToMerge.clear();
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new StorageETLException("Interrupted!", e, concurrResult);
        } catch (ExecutionException e) {
            throw new StorageETLException("Execution exception!", e, concurrResult);
        } catch (TimeoutException e) {
            throw new StorageETLException("Timeout Exception", e, concurrResult);
        }  finally {
            if (!executorService.isShutdown()) {
                try {
                    executorService.shutdownNow();
                } catch (Exception e) {
                    logger.error("Problems shutting executer service down", e);
                }
            }
        }
        return concurrResult;
    }

    @Override
    public AbstractHadoopVariantStorageETL newStorageETL(boolean connected) throws StorageManagerException {
        return newStorageETL(connected, null);
    }

    public AbstractHadoopVariantStorageETL newStorageETL(boolean connected, Map<? extends String, ?> extraOptions)
            throws StorageManagerException {
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        if (extraOptions != null) {
            options.putAll(extraOptions);
        }
        boolean directLoad = options.getBoolean(HADOOP_LOAD_DIRECT, HADOOP_LOAD_DIRECT_DEFAULT);
        VariantHadoopDBAdaptor dbAdaptor = connected ? getDBAdaptor() : null;
        Configuration hadoopConfiguration = null == dbAdaptor ? null : dbAdaptor.getConfiguration();
        hadoopConfiguration = hadoopConfiguration == null ? getHadoopConfiguration(options) : hadoopConfiguration;
        hadoopConfiguration.setIfUnset(ArchiveDriver.CONFIG_ARCHIVE_TABLE_COMPRESSION, Algorithm.SNAPPY.getName());

        HBaseCredentials archiveCredentials = buildCredentials(getArchiveTableName(options.getInt(Options.STUDY_ID.key()), options));

        AbstractHadoopVariantStorageETL storageETL = null;
        if (directLoad) {
            storageETL = new HadoopDirectVariantStorageETL(configuration, storageEngineId, dbAdaptor, getMRExecutor(options),
                    hadoopConfiguration, archiveCredentials, getVariantReaderUtils(hadoopConfiguration), options);
        } else {
            storageETL = new HadoopVariantStorageETL(configuration, storageEngineId, dbAdaptor, getMRExecutor(options), hadoopConfiguration,
                    archiveCredentials, getVariantReaderUtils(hadoopConfiguration), options);
        }
        return storageETL;
    }

    public HdfsVariantReaderUtils getVariantReaderUtils() {
        return getVariantReaderUtils(conf);
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
        // Use ETL as helper class
        AbstractHadoopVariantStorageETL etl = newStorageETL(true);
        VariantDBAdaptor dbAdaptor = etl.getDBAdaptor();
        StudyConfiguration studyConfiguration;
        StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();
        List<Integer> fileList = Collections.singletonList(fileId);
        final int studyId;
        if (StringUtils.isNumeric(study)) {
            studyId = Integer.parseInt(study);
        } else {
            studyConfiguration = scm.getStudyConfiguration(study, null).first();
            studyId = studyConfiguration.getStudyId();
        }

        // Pre delete
        long lock = scm.lockStudy(studyId);
        try {
            studyConfiguration = scm.getStudyConfiguration(studyId, null).first();
            if (!studyConfiguration.getIndexedFiles().contains(fileId)) {
                throw StorageManagerException.unableToExecute("File not indexed.", fileId, studyConfiguration);
            }
            boolean resume = conf.getBoolean(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT_RESUME, false);
            BatchFileOperation operation =
                    etl.addBatchOperation(studyConfiguration, VariantTableDeletionDriver.JOB_OPERATION_NAME, fileList, resume);
            options.put(AbstractVariantTableDriver.TIMESTAMP, operation.getTimestamp());
            scm.updateStudyConfiguration(studyConfiguration, null);
        } finally {
            scm.unLockStudy(studyId, lock);
        }

        // Delete
        Thread hook = etl.newShutdownHook(VariantTableDeletionDriver.JOB_OPERATION_NAME, fileList);
        try {
            Runtime.getRuntime().addShutdownHook(hook);

            String archiveTable = getArchiveTableName(studyId, options);
            HBaseCredentials variantsTable = getDbCredentials();
            String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
            String jar = AbstractHadoopVariantStorageETL.getJarWithDependencies(options);

            Class execClass = VariantTableDeletionDriver.class;
            String args = VariantTableDeletionDriver.buildCommandLineArgs(variantsTable.getHostUri().toString(), archiveTable,
                    variantsTable.getTable(), studyId, fileList, options);
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

            // Post Delete
            // If everything went fine, remove file column from Archive table and from studyconfig
            lock = scm.lockStudy(studyId);
            try {
                studyConfiguration = scm.getStudyConfiguration(studyId, null).first();
                etl.secureSetStatus(studyConfiguration, BatchFileOperation.Status.READY,
                        VariantTableDeletionDriver.JOB_OPERATION_NAME, fileList);
                studyConfiguration.getIndexedFiles().remove(fileId);
                scm.updateStudyConfiguration(studyConfiguration, null);
            } finally {
                scm.unLockStudy(studyId, lock);
            }

        } catch (Exception e) {
            etl.setStatus(BatchFileOperation.Status.ERROR, VariantTableDeletionDriver.JOB_OPERATION_NAME, fileList);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    @Override
    public void dropStudy(String studyName) throws StorageManagerException {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public VariantHadoopDBAdaptor getDBAdaptor(String tableName) throws StorageManagerException {
        tableName = getVariantTableName(tableName);
        return getDBAdaptor(buildCredentials(tableName));
    }

    private HBaseCredentials getDbCredentials() throws StorageManagerException {
        String table = getVariantTableName();
        return buildCredentials(table);
    }

    @Override
    public VariantHadoopDBAdaptor getDBAdaptor() throws StorageManagerException {
        return getDBAdaptor(getDbCredentials());
    }

    protected VariantHadoopDBAdaptor getDBAdaptor(HBaseCredentials credentials) throws StorageManagerException {
        try {
            StorageEngineConfiguration storageEngine = this.configuration.getStorageEngine(STORAGE_ENGINE_ID);
            Configuration configuration = getHadoopConfiguration(storageEngine.getVariant().getOptions());
            configuration = VariantHadoopDBAdaptor.getHbaseConfiguration(configuration, credentials);

            return new VariantHadoopDBAdaptor(credentials, this.configuration, configuration);
        } catch (IOException e) {
            throw new StorageManagerException("Problems creating DB Adapter", e);
        }
    }

    public HBaseCredentials buildCredentials(String table) throws StorageManagerException {
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
            String server;
            Integer port;
            String zookeeperPath;
            if (target == null || target.isEmpty()) {
                Configuration conf = getHadoopConfiguration(getOptions());
                server = conf.get(HConstants.ZOOKEEPER_QUORUM);
                port = 60000;
                zookeeperPath = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            } else {
                URI uri = new URI(target);
                server = uri.getHost();
                port = uri.getPort() > 0 ? uri.getPort() : 60000;
                // If just an IP or host name is provided, the URI parser will return empty host, and the content as "path". Avoid that
                if (server == null) {
                    server = uri.getPath();
                    zookeeperPath = null;
                } else {
                    zookeeperPath = uri.getPath();
                }
            }
            HBaseCredentials credentials = new HBaseCredentials(server, table, user, pass, port);
            if (!StringUtils.isBlank(zookeeperPath)) {
                credentials = new HBaseCredentials(server, table, user, pass, port, zookeeperPath);
            }
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
            return new HBaseStudyConfigurationManager(dbCredentials.getTable(), configuration, options);
        } catch (IOException e) {
            e.printStackTrace();
            return super.buildStudyConfigurationManager(options);
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
        if (options.containsKey(EXTERNAL_MR_EXECUTOR)) {
            Class<? extends MRExecutor> aClass;
            if (options.get(EXTERNAL_MR_EXECUTOR) instanceof Class) {
                aClass = options.get(EXTERNAL_MR_EXECUTOR, Class.class).asSubclass(MRExecutor.class);
            } else {
                try {
                    aClass = Class.forName(options.getString(EXTERNAL_MR_EXECUTOR)).asSubclass(MRExecutor.class);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return aClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else if (mrExecutor == null) {
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
    public String getArchiveTableName(int studyId) {
        String prefix = getOptions().getString(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX);
        if (StringUtils.isEmpty(prefix)) {
            prefix = ARCHIVE_TABLE_PREFIX;
        }
        return buildTableName(getOptions().getString(OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, ""),
                prefix, studyId);
    }

    /**
     * Get the archive table name given a StudyId.
     *
     * @param studyId Numerical study identifier
     * @param conf Hadoop configuration with the OpenCGA values.
     * @return Table name
     */
    public static String getArchiveTableName(int studyId, Configuration conf) {
        String prefix = conf.get(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX);
        if (StringUtils.isEmpty(prefix)) {
            prefix = ARCHIVE_TABLE_PREFIX;
        }
        return buildTableName(conf.get(OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, ""),
                prefix, studyId);
    }

    /**
     * Get the archive table name given a StudyId.
     *
     * @param studyId Numerical study identifier
     * @param options Options
     * @return Table name
     */
    public static String getArchiveTableName(int studyId, ObjectMap options) {
        String prefix = options.getString(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX);
        if (StringUtils.isEmpty(prefix)) {
            prefix = ARCHIVE_TABLE_PREFIX;
        }
        return buildTableName(options.getString(OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, ""),
                prefix, studyId);
    }

    public String getVariantTableName() {
        return getVariantTableName(getOptions().getString(Options.DB_NAME.key()));
    }

    public String getVariantTableName(String table) {
        return getVariantTableName(table, getOptions());
    }

    public static String getVariantTableName(String table, ObjectMap options) {
        return buildTableName(options.getString(OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, ""), "", table);
    }

    public static String getVariantTableName(String table, Configuration conf) {
        return buildTableName(conf.get(OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, ""), "", table);
    }

    protected static String buildTableName(String namespace, String prefix, int studyId) {
        return buildTableName(namespace, prefix, String.valueOf(studyId));
    }

    protected static String buildTableName(String namespace, String prefix, String tableName) {
        StringBuilder sb = new StringBuilder();

        if (StringUtils.isNotEmpty(namespace)) {
            if (tableName.contains(":")) {
                if (!tableName.startsWith(namespace + ":")) {
                    throw new IllegalArgumentException("Wrong namespace : '" + tableName + "'."
                            + " Namespace mismatches with the read from configuration:" + namespace);
                } else {
                    tableName = tableName.substring(tableName.indexOf(':') + 1); // Remove '<namespace>:'
                }
            }
            sb.append(namespace).append(":");
        }
        if (StringUtils.isNotEmpty(prefix)) {
            sb.append(prefix);
            if (!prefix.endsWith("_")) {
                sb.append("_");
            }
        }
        sb.append(tableName);

        String fullyQualified = sb.toString();
        TableName.isLegalFullyQualifiedTableName(fullyQualified.getBytes());
        return fullyQualified;
    }

    public VariantSource readVariantSource(URI input) throws StorageManagerException {
        return getVariantReaderUtils(null).readVariantSource(input);
    }

    private static class HdfsVariantReaderUtils extends VariantReaderUtils {
        private final Configuration conf;

        HdfsVariantReaderUtils(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public VariantSource readVariantSource(URI input) throws StorageManagerException {
            VariantSource source;

            if (input.getScheme() == null || input.getScheme().startsWith("file")) {
                if (input.getPath().contains("variants.proto")) {
                    return VariantReaderUtils.readVariantSource(Paths.get(input.getPath().replace("variants.proto", "file.json")), null);
                } else {
                    return VariantReaderUtils.readVariantSource(Paths.get(input.getPath()), null);
                }
            }

            Path metaPath = new Path(VariantReaderUtils.getMetaFromTransformedFile(input.toString()));
            FileSystem fs = null;
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                throw new StorageManagerException("Unable to get FileSystem", e);
            }
            try (
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
