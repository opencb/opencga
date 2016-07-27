package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.hpg.bigdata.tools.utils.HBaseUtils;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.exceptions.StorageHadoopException;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HBaseStudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public abstract class AbstractVariantTableDriver extends Configured implements Tool {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    public static final String CONFIG_VARIANT_FILE_IDS          = "opencga.variant.input.file_ids";
    public static final String CONFIG_VARIANT_TABLE_NAME        = "opencga.variant.table.name";
    public static final String CONFIG_VARIANT_TABLE_COMPRESSION = "opencga.variant.table.compression";

    public static final String TIMESTAMP                        = "opencga.variant.table.timestamp";

    private VariantTableHelper variantTablehelper;

    protected HBaseStudyConfigurationManager scm;
    protected StudyConfiguration studyConfiguration;

    public AbstractVariantTableDriver() { /* nothing */ }

    /**
     * @param conf Configuration.
     */
    public AbstractVariantTableDriver(Configuration conf) {
        super(conf);
    }

    @SuppressWarnings ("rawtypes")
    protected abstract Class<? extends TableMapper> getMapperClass();

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inTable = conf.get(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, StringUtils.EMPTY);
        String outTable = conf.get(CONFIG_VARIANT_TABLE_NAME, StringUtils.EMPTY);
        String[] fileArr = argFileArray();
        Integer studyId = conf.getInt(GenomeHelper.CONFIG_STUDY_ID, -1);

        /* -------------------------------*/
        // Validate parameters CHECK
        if (StringUtils.isEmpty(inTable)) {
            throw new IllegalArgumentException("No input hbase table basename specified!!!");
        }
        if (StringUtils.isEmpty(outTable)) {
            throw new IllegalArgumentException("No output hbase table specified!!!");
        }
        if (inTable.equals(outTable)) {
            throw new IllegalArgumentException("Input and Output tables must be different");
        }
        if (studyId < 0) {
            throw new IllegalArgumentException("No Study id specified!!!");
        }
        int fileCnt = fileArr.length;
        if (fileCnt == 0) {
            throw new IllegalArgumentException("No files specified");
        }

        List<Integer> fileIds = new ArrayList<>(fileArr.length);
        for (String fileIdStr : fileArr) {
            int id = Integer.parseInt(fileIdStr);
            fileIds.add(id);
        }

        getLog().info(String.format("Use table %s as input", inTable));

        GenomeHelper.setStudyId(conf, studyId);
        VariantTableHelper.setOutputTableName(conf, outTable);
        VariantTableHelper.setInputTableName(conf, inTable);

        VariantTableHelper gh = getHelper();

        /* -------------------------------*/
        // Validate input CHECK
        HBaseManager hBaseManager = gh.getHBaseManager();
        if (!hBaseManager.act(inTable, ((Table table, Admin admin) -> HBaseUtils.exist(table.getName(), admin)))) {
            throw new IllegalArgumentException(String.format("Input table %s does not exist!!!", inTable));
        }

        /* -------------------------------*/
        check(fileIds);

        /* -------------------------------*/
        // JOB setup
        Job job = createJob(outTable, fileArr);

        // QUERY design
        Scan scan = createScan(gh, fileArr);

        // set other scan attrs
        boolean addDependencyJar = conf.getBoolean(GenomeHelper.CONFIG_HBASE_ADD_DEPENDENCY_JARS, true);
        initMapReduceJob(inTable, outTable, job, scan, addDependencyJar);

        boolean succeed = executeJob(job);
        if (!succeed) {
            getLog().error("error with job!");
        }
        if (succeed) {
            onSuccess();
        } else {
            onError();
        }

        getStudyConfigurationManager().close();

        return succeed ? 0 : 1;
    }

    protected void check(List<Integer> fileIds) throws StorageManagerException, IOException {
        Configuration conf = getConf();
        HBaseStudyConfigurationManager scm = getStudyConfigurationManager();

        long lock = scm.lockStudy(getHelper().getStudyId());
        studyConfiguration = loadStudyConfiguration();

        List<BatchFileOperation> batches = studyConfiguration.getBatches();
        BatchFileOperation batchFileOperation;
        if (!batches.isEmpty()) {
            batchFileOperation = batches.get(batches.size() - 1);
            BatchFileOperation.Status currentStatus = batchFileOperation.currentStatus();
            if (currentStatus != null) {
                switch (currentStatus) {
                    case READY:
                        batchFileOperation = new BatchFileOperation(getJobOperationName(), fileIds, batchFileOperation.getTimestamp() + 1);
                        break;
                    case RUNNING:
                        if (!conf.getBoolean(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT_RESUME, false)) {
                            throw new StorageHadoopException("Unable to process a new batch. Ongoing batch operation: "
                                    + batchFileOperation);
                        }
                        // Do not break. Resuming last loading, go to error case.
                    case ERROR:
                        if (batchFileOperation.getFileIds().equals(fileIds)) {
                            LOG.info("Resuming Last batch loading due to error.");
                        } else {
                            throw new StorageHadoopException("Unable to resume last batch operation. "
                                    + "Must have the same files from the previous batch: " + batchFileOperation);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown Status " + currentStatus);
                }
            }
        } else {
            batchFileOperation = new BatchFileOperation(getJobOperationName(), fileIds, 1);
        }
        batchFileOperation.addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.RUNNING);
        batches.add(batchFileOperation);

        scm.updateStudyConfiguration(studyConfiguration, new QueryOptions());
        scm.unLockStudy(studyConfiguration.getStudyId(), lock);
        conf.setLong(TIMESTAMP, batchFileOperation.getTimestamp());
    }

    protected void onError() {
        try {
            HBaseStudyConfigurationManager scm = getStudyConfigurationManager();
            long lock = scm.lockStudy(getHelper().getStudyId());
            studyConfiguration = scm.getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
            BatchFileOperation batchFileOperation = studyConfiguration.getBatches().get(studyConfiguration.getBatches().size() - 1);
            batchFileOperation.addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.ERROR);
            scm.updateStudyConfiguration(studyConfiguration, new QueryOptions());
            scm.unLockStudy(studyConfiguration.getStudyId(), lock);
        } catch (IOException | StorageManagerException e) {
            e.printStackTrace();
        }
    }

    protected void onSuccess() {
        try {
            HBaseStudyConfigurationManager scm = getStudyConfigurationManager();
            long lock = scm.lockStudy(getHelper().getStudyId());
            studyConfiguration = scm.getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
            BatchFileOperation batchFileOperation = studyConfiguration.getBatches().get(studyConfiguration.getBatches().size() - 1);
            batchFileOperation.addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.READY);
            scm.updateStudyConfiguration(studyConfiguration, new QueryOptions());
            scm.unLockStudy(studyConfiguration.getStudyId(), lock);
        } catch (IOException | StorageManagerException e) {
            e.printStackTrace();
        }
    }

    /**
     * Give the name of the action that the job is doing.
     *
     * Used to create the jobName and as {@link BatchFileOperation#operationName}
     *
     * e.g. : "Delete", "Load", "Annotate", ...
     *
     * @return Job action
     */
    protected abstract String getJobOperationName();

    protected String[] argFileArray() {
        return getConf().getStrings(CONFIG_VARIANT_FILE_IDS, new String[0]);
    }

    protected VariantTableHelper getHelper() {
        if (null == variantTablehelper) {
            variantTablehelper = new VariantTableHelper(getConf());
        }
        return variantTablehelper;
    }

    protected void initMapReduceJob(String inTable, String outTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        TableMapReduceUtil.initTableMapperJob(
                inTable,      // input table
                scan,             // Scan instance to control CF and attribute selection
                getMapperClass(),   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job,
                addDependencyJar);
        TableMapReduceUtil.initTableReducerJob(
                outTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(MultiTableOutputFormat.class);
    }

    protected boolean executeJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        Thread hook = new Thread(() -> {
            try {
                if (!job.isComplete()) {
                    job.killJob();
                }
                onError();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        boolean succeed = job.waitForCompletion(true);
        Runtime.getRuntime().removeShutdownHook(hook);
        return succeed;
    }

    protected Logger getLog() {
        return LOG;
    }

    protected Job createJob(String outTable, String[] fileArr) throws IOException {
        Job job = Job.getInstance(getConf(), "opencga: " + getJobOperationName() + " file " + Arrays.toString(fileArr)
                + " on VariantTable '" + outTable + "'");
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.setJarByClass(getMapperClass());    // class that contains mapper
        return job;
    }

    protected Scan createScan(VariantTableHelper gh, String[] fileArr) {
        Scan scan = new Scan();
        scan.setCaching(100);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // specify return columns (file IDs)
        for (String fileIdStr : fileArr) {
            int id = Integer.parseInt(fileIdStr);
            scan.addColumn(gh.getColumnFamily(), Bytes.toBytes(ArchiveHelper.getColumnName(id)));
        }
        scan.addColumn(gh.getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
        return scan;
    }

    protected StudyConfiguration loadStudyConfiguration() throws IOException {
        HBaseStudyConfigurationManager scm = getStudyConfigurationManager();
        int studyId = getHelper().getStudyId();
        QueryResult<StudyConfiguration> res = scm.getStudyConfiguration(studyId, new QueryOptions());
        if (res.getResult().size() != 1) {
            throw new IllegalStateException("StudyConfiguration " + studyId + " not found! " + res.getResult().size());
        }
        return res.first();
    }

    protected HBaseStudyConfigurationManager getStudyConfigurationManager() throws IOException {
        if (scm == null) {
            byte[] outTable = getHelper().getOutputTable();
            scm = new HBaseStudyConfigurationManager(Bytes.toString(outTable), getConf(), null);
        }
        return scm;
    }

    public static String buildCommandLineArgs(String server, String inputTable, String outputTable, int studyId,
                                              List<Integer> fileIds, Map<String, Object> other) {
        StringBuilder stringBuilder = new StringBuilder().append(server).append(' ').append(inputTable).append(' ')
                .append(outputTable).append(' ').append(studyId).append(' ');

        stringBuilder.append(fileIds.stream().map(Object::toString).collect(Collectors.joining(",")));
        ArchiveDriver.addOtherParams(other, stringBuilder);
        return stringBuilder.toString();
    }

    public static boolean createVariantTableIfNeeded(GenomeHelper genomeHelper, String tableName) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(genomeHelper.getConf())) {
            return createVariantTableIfNeeded(genomeHelper, tableName, con);
        }
    }

    public static boolean createVariantTableIfNeeded(GenomeHelper genomeHelper, String tableName, Connection con) throws IOException {
        return genomeHelper.getHBaseManager().createTableIfNeeded(con, tableName, genomeHelper.getColumnFamily(),
                Compression.getCompressionAlgorithmByName(
                        genomeHelper.getConf().get(CONFIG_VARIANT_TABLE_COMPRESSION, Compression.Algorithm.SNAPPY.getName())));
    }

    public static String[] configure(String[] args, Configuration conf) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            throw new NullPointerException("Provided Configuration is null!!!");
        }
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);

        //get the args w/o generic hadoop args
        String[] toolArgs = parser.getRemainingArgs();

        int fixedSizeArgs = 5;
        if (toolArgs.length < fixedSizeArgs || (toolArgs.length - fixedSizeArgs) % 2 != 0) {
            System.err.printf("Usage: %s [generic options] <server> <input-table> <output-table> <studyId> <fileIds>"
                    + " [<key> <value>]*\n",
                    AbstractVariantTableDriver.class.getSimpleName());
            System.err.println("Found " + Arrays.toString(toolArgs));
            ToolRunner.printGenericCommandUsage(System.err);
            return null;
        }

        HBaseManager.addHBaseSettings(conf, toolArgs[0]);
        conf.set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, toolArgs[1]);
        conf.set(CONFIG_VARIANT_TABLE_NAME, toolArgs[2]);
        conf.set(GenomeHelper.CONFIG_STUDY_ID, toolArgs[3]);
        conf.setStrings(CONFIG_VARIANT_FILE_IDS, toolArgs[4].split(","));
        for (int i = fixedSizeArgs; i < toolArgs.length; i = i + 2) {
            conf.set(toolArgs[i], toolArgs[i + 1]);
        }
        return toolArgs;
    }
}
