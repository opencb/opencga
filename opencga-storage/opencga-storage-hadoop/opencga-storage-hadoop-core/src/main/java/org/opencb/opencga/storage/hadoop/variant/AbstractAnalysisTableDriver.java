package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.util.SchemaUtil;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseStudyConfigurationDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.OPENCGA_STORAGE_HADOOP_MAPREDUCE_SCANNER_TIMEOUT;

/**
 * Created by mh719 on 21/11/2016.
 */
public abstract class AbstractAnalysisTableDriver extends Configured implements Tool {

    public static final String CONFIG_VARIANT_FILE_IDS             = "opencga.variant.input.file_ids";
    public static final String CONFIG_VARIANT_TABLE_NAME           = "opencga.variant.table.name";
    public static final String CONFIG_VARIANT_TABLE_COMPRESSION    = "opencga.variant.table.compression";
    public static final String CONFIG_VARIANT_TABLE_PRESPLIT_SIZE  = "opencga.variant.table.presplit.size";
    public static final String TIMESTAMP                           = "opencga.variant.table.timestamp";

    public static final String HBASE_KEYVALUE_SIZE_MAX = "hadoop.load.variant.hbase.client.keyvalue.maxsize";
    public static final String HBASE_SCAN_CACHING = "hadoop.load.variant.scan.caching";

    private final Logger logger = LoggerFactory.getLogger(AbstractAnalysisTableDriver.class);
    private VariantTableHelper variantTablehelper;
    private StudyConfigurationManager scm;

    public AbstractAnalysisTableDriver() {
        super(HBaseConfiguration.create());
    }

    public AbstractAnalysisTableDriver(Configuration conf) {
        super(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        configFromArgs(args);
        Configuration conf = getConf();
        String archiveTable = getArchiveTable();
        String variantTable = getAnalysisTable();
        Integer studyId = getStudyId();

        int maxKeyValueSize = conf.getInt(HBASE_KEYVALUE_SIZE_MAX, 10485760); // 10MB
        logger.info("HBASE: set " + ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY + " to " + maxKeyValueSize);
        conf.setInt(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, maxKeyValueSize); // always overwrite server default (usually 1MB)

        /* -------------------------------*/
        // Validate parameters CHECK
        if (StringUtils.isEmpty(archiveTable)) {
            throw new IllegalArgumentException("No archive hbase table basename specified!!!");
        }
        if (StringUtils.isEmpty(variantTable)) {
            throw new IllegalArgumentException("No variant hbase table specified!!!");
        }
        if (archiveTable.equals(variantTable)) {
            throw new IllegalArgumentException("archive and variant tables must be different");
        }
        if (studyId < 0) {
            throw new IllegalArgumentException("No Study id specified!!!");
        }

        VariantTableHelper helper = initVariantTableHelper(studyId, archiveTable, variantTable);

        // Other validations
        parseAndValidateParameters();

        /* -------------------------------*/
        // Validate input CHECK
        try (HBaseManager hBaseManager = new HBaseManager(conf)) {
            checkTablesExist(hBaseManager, archiveTable, variantTable);
        }

        // Check File(s) or Study is specified
        List<Integer> fileIds = getFiles();

        /* -------------------------------*/
        // JOB setup
        Job job = newJob(variantTable, fileIds);
        setupJob(job, archiveTable, variantTable, fileIds);

        preExecution(variantTable);

        boolean succeed = executeJob(job);
        if (!succeed) {
            logger.error("error with job!");
        }

        postExecution(succeed);
        getStudyConfigurationManager().close();
        return succeed ? 0 : 1;
    }

    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        // do nothing
    }

    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        // do nothing
    }

    protected abstract void parseAndValidateParameters();

    protected abstract Class<?> getMapperClass();

    protected abstract Job setupJob(Job job, String archiveTable, String variantTable, List<Integer> files) throws IOException;

    /**
     * Give the name of the action that the job is doing.
     *
     * Used to create the jobName and as {@link org.opencb.opencga.storage.core.metadata.BatchFileOperation#operationName}
     *
     * e.g. : "Delete", "Load", "Annotate", ...
     *
     * @return Job action
     */
    protected abstract String getJobOperationName();

    private boolean executeJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        Thread hook = new Thread(() -> {
            try {
                if (!job.isComplete()) {
                    job.killJob();
                }
//                onError();
            } catch (IOException e) {
                logger.error("Error", e);
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        boolean succeed = job.waitForCompletion(true);
        Runtime.getRuntime().removeShutdownHook(hook);
        return succeed;
    }


    protected final void initMapReduceJob(Job job, Class<? extends TableMapper> mapperClass, String inTable, Scan scan)
            throws IOException {
        boolean addDependencyJar = getConf().getBoolean(GenomeHelper.CONFIG_HBASE_ADD_DEPENDENCY_JARS, true);
        logger.info("Use table {} as input", inTable);
        TableMapReduceUtil.initTableMapperJob(
                inTable,      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapperClass,   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job,
                addDependencyJar);
    }

    protected final void initMapReduceJob(Job job, Class<? extends TableMapper> mapperClass, String inTable, String outTable, Scan scan)
            throws IOException {
        boolean addDependencyJar = getConf().getBoolean(GenomeHelper.CONFIG_HBASE_ADD_DEPENDENCY_JARS, true);
        initMapReduceJob(job, mapperClass, inTable, scan);
        logger.info("Use table {} as output", outTable);
        TableMapReduceUtil.initTableReducerJob(
                outTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
        job.setNumReduceTasks(0);
    }

    protected final Scan createArchiveTableScan(List<Integer> files) {
        Scan scan = new Scan();
        int caching = getConf().getInt(HBASE_SCAN_CACHING, 50);
        logger.info("Scan set Caching to " + caching);
        scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // https://hbase.apache.org/book.html#perf.hbase.client.seek
        int lookAhead = getConf().getInt("hadoop.load.variant.scan.lookahead", -1);
        if (lookAhead > 0) {
            logger.info("Scan set LOOKAHEAD to " + lookAhead);
            scan.setAttribute(Scan.HINT_LOOKAHEAD, Bytes.toBytes(lookAhead));
        }
        // specify return columns (file IDs)
        FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (Integer id : files) {
            filter.addFilter(new ColumnRangeFilter(Bytes.toBytes(ArchiveTableHelper.getColumnName(id)), true,
                    Bytes.toBytes(ArchiveTableHelper.getColumnName(id)), true));
        }
        filter.addFilter(new ColumnPrefixFilter(GenomeHelper.VARIANT_COLUMN_B_PREFIX));
        scan.setFilter(filter);
        return scan;
    }

    protected final Scan createVariantsTableScan() {
        Scan scan = new Scan();
//        int caching = getConf().getInt(AbstractAnalysisTableDriver.HBASE_SCAN_CACHING, 50);
//        getLog().info("Scan set Caching to " + caching);
//        scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily(getHelper().getColumnFamily()); // Ignore PHOENIX columns!!!
        return scan;
    }

    private Job newJob(String variantTable, List<Integer> files) throws IOException {
        Job job = Job.getInstance(getConf(), "opencga: " + getJobOperationName() + " files " + files
                + " from VariantTable '" + variantTable + '\'');
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.setJarByClass(getMapperClass());    // class that contains mapper

        // Increase the ScannerTimeoutPeriod to avoid ScannerTimeoutExceptions
        // See opencb/opencga#352 for more info.
        int scannerTimeout = getConf().getInt(OPENCGA_STORAGE_HADOOP_MAPREDUCE_SCANNER_TIMEOUT,
                getConf().getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
        logger.info("Set Scanner timeout to " + scannerTimeout + " ...");
        job.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, scannerTimeout);
        return job;
    }

    protected List<Integer> getFilesToUse() throws IOException {
        StudyConfiguration studyConfiguration = readStudyConfiguration();
        LinkedHashSet<Integer> indexedFiles = studyConfiguration.getIndexedFiles();
        List<Integer> files = getFiles();
        if (files.isEmpty()) { // no files specified - use all indexed files for study
            files = new ArrayList<>(indexedFiles);
        } else { // Validate that they exist
            List<Integer> notIndexed = files
                    .stream()
                    .filter(fid -> !indexedFiles.contains(fid))
                    .collect(Collectors.toList());
            if (!notIndexed.isEmpty()) {
                throw new IllegalStateException("Provided File ID(s) not indexed!!!" + notIndexed);
            }
        }
        if (files.isEmpty()) { // if still empty (no files provided and / or found in study
            throw new IllegalArgumentException("No files specified / available for study " + getHelper().getStudyId());
        }
        return files;
    }

    protected List<Integer> getFiles() {
        String[] fileArr = getConf().getStrings(CONFIG_VARIANT_FILE_IDS, new String[0]);
        return Arrays.stream(fileArr)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }

    protected int getStudyId() {
        return getConf().getInt(GenomeHelper.CONFIG_STUDY_ID, -1);
    }

    protected String getAnalysisTable() {
        return getConf().get(CONFIG_VARIANT_TABLE_NAME, StringUtils.EMPTY);
    }

    protected String getArchiveTable() {
        return getConf().get(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, StringUtils.EMPTY);
    }

    protected StudyConfiguration readStudyConfiguration() throws IOException {
        StudyConfigurationManager scm = getStudyConfigurationManager();
        int studyId = getHelper().getStudyId();
        QueryResult<StudyConfiguration> res = scm.getStudyConfiguration(studyId, new QueryOptions());
        if (res.getResult().size() != 1) {
            throw new IllegalStateException("StudyConfiguration " + studyId + " not found! " + res.getResult().size());
        }
        return res.first();
    }

    protected StudyConfigurationManager getStudyConfigurationManager() throws IOException {
        if (scm == null) {
            scm = new StudyConfigurationManager(new HBaseStudyConfigurationDBAdaptor(getAnalysisTable(), getConf(), null));
        }
        return scm;
    }

    private void checkTablesExist(HBaseManager hBaseManager, String... tables) {
        Arrays.stream(tables).forEach(table -> {
            try {
                if (!hBaseManager.tableExists(table)) {
                    throw new IllegalArgumentException("Table " + table + " does not exist!!!");
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private VariantTableHelper initVariantTableHelper(Integer studyId, String archiveTable, String analysisTable) {
        Configuration conf = getConf();
        VariantTableHelper.setStudyId(conf, studyId);
        VariantTableHelper.setAnalysisTable(conf, analysisTable);
        VariantTableHelper.setArchiveTable(conf, archiveTable);
        variantTablehelper = new VariantTableHelper(conf, archiveTable, analysisTable, null);
        return variantTablehelper;
    }

    protected VariantTableHelper getHelper() {
        return variantTablehelper;
    }

    private void configFromArgs(String[] args) {
        // TODO ?? Do we need this??
//        setConf(HBaseManager.addHBaseSettings(getConf(), args[0]));
        int fixedSizeArgs = 5;

        if (args.length < fixedSizeArgs || (args.length - fixedSizeArgs) % 2 != 0) {
            System.err.println("Usage: " + getClass().getSimpleName()
                    + " [generic options] <server> <input-table> <output-table> <studyId> <fileIds> [<key> <value>]*");
            System.err.println("Found " + Arrays.toString(args));
            ToolRunner.printGenericCommandUsage(System.err);
            throw new IllegalArgumentException("Wrong number of arguments!");
        }

        getConf().set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, args[1]);
        getConf().set(CONFIG_VARIANT_TABLE_NAME, args[2]);
        getConf().set(GenomeHelper.CONFIG_STUDY_ID, args[3]);
        getConf().setStrings(CONFIG_VARIANT_FILE_IDS, args[4].split(","));

        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            getConf().set(args[i], args[i + 1]);
        }
    }

    public int privateMain(String[] args) throws Exception {
        return privateMain(args, getConf());
    }

    public int privateMain(String[] args, Configuration conf) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        setConf(conf);
        return ToolRunner.run(this, args);
    }


    public static String buildCommandLineArgs(String server, String inputTable, String outputTable, int studyId,
                                              List<Integer> fileIds, Map<String, Object> other) {
        StringBuilder stringBuilder = new StringBuilder().append(server).append(' ').append(inputTable).append(' ')
                .append(outputTable).append(' ').append(studyId).append(' ');

        stringBuilder.append(fileIds.stream().map(Object::toString).collect(Collectors.joining(",")));
        addOtherParams(other, stringBuilder);
        return stringBuilder.toString();
    }

    public static void addOtherParams(Map<String, Object> other, StringBuilder stringBuilder) {
        for (Map.Entry<String, Object> entry : other.entrySet()) {
            Object value = entry.getValue();
            if (value != null && (value instanceof Number
                    || value instanceof Boolean
                    || value instanceof String && !((String) value).contains(" ") && !((String) value).isEmpty())) {
                stringBuilder.append(' ').append(entry.getKey()).append(' ').append(value);
            }
        }
    }

    public static boolean createVariantTableIfNeeded(GenomeHelper genomeHelper, String tableName) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(genomeHelper.getConf())) {
            return createVariantTableIfNeeded(genomeHelper, tableName, con);
        }
    }

    public static boolean createVariantTableIfNeeded(GenomeHelper genomeHelper, String tableName, Connection con)
            throws IOException {
        VariantPhoenixHelper variantPhoenixHelper = new VariantPhoenixHelper(genomeHelper);

        String namespace = SchemaUtil.getSchemaNameFromFullName(tableName);
        if (StringUtils.isNotEmpty(namespace)) {
//            HBaseManager.createNamespaceIfNeeded(con, namespace);
            try (java.sql.Connection jdbcConnection = variantPhoenixHelper.newJdbcConnection()) {
                variantPhoenixHelper.createSchemaIfNeeded(jdbcConnection, namespace);
                LoggerFactory.getLogger(AbstractAnalysisTableDriver.class).info("Phoenix connection is autoclosed ... " + jdbcConnection);
            } catch (ClassNotFoundException | SQLException e) {
                throw new IOException(e);
            }
        }

        int nsplits = genomeHelper.getConf().getInt(CONFIG_VARIANT_TABLE_PRESPLIT_SIZE, 100);
        List<byte[]> splitList = GenomeHelper.generateBootPreSplitsHuman(
                nsplits,
                (chr, pos) -> VariantPhoenixKeyFactory.generateVariantRowKey(chr, pos, "", ""));
        boolean newTable = HBaseManager.createTableIfNeeded(con, tableName, genomeHelper.getColumnFamily(),
                splitList, Compression.getCompressionAlgorithmByName(
                        genomeHelper.getConf().get(
                                CONFIG_VARIANT_TABLE_COMPRESSION,
                                Compression.Algorithm.SNAPPY.getName())));
        if (newTable) {
            try (java.sql.Connection jdbcConnection = variantPhoenixHelper.newJdbcConnection()) {
                variantPhoenixHelper.createTableIfNeeded(jdbcConnection, tableName);
                LoggerFactory.getLogger(AbstractAnalysisTableDriver.class).info("Phoenix connection is autoclosed ... " + jdbcConnection);
            } catch (ClassNotFoundException | SQLException e) {
                throw new IOException(e);
            }
        }
        return newTable;
    }
}
