/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.hpg.bigdata.tools.utils.HBaseUtils;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HBaseStudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.exceptions.StorageHadoopException;
import org.opencb.opencga.storage.hadoop.variant.metadata.BatchFileOperation;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStudyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableDriver extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(VariantTableDriver.class);

    public static final String CONFIG_VARIANT_FILE_IDS          = "opencga.variant.input.file_ids";
    public static final String CONFIG_VARIANT_TABLE_NAME        = "opencga.variant.table.name";
    public static final String CONFIG_VARIANT_TABLE_COMPRESSION = "opencga.variant.table.compression";
    private HBaseStudyConfigurationManager scm;

    public VariantTableDriver() { /* nothing */}

    public VariantTableDriver(Configuration conf) {
        super(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inTable = conf.get(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, StringUtils.EMPTY);
        String outTable = conf.get(CONFIG_VARIANT_TABLE_NAME, StringUtils.EMPTY);
        String[] fileArr = conf.getStrings(CONFIG_VARIANT_FILE_IDS, new String[0]);
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

        LOG.info(String.format("Use table %s as input", inTable));

        GenomeHelper.setStudyId(conf, studyId);
        VariantTableHelper.setOutputTableName(conf, outTable);
        VariantTableHelper.setInputTableName(conf, inTable);

        VariantTableHelper gh = new VariantTableHelper(conf);


        /* -------------------------------*/
        // Validate input CHECK
        if (!gh.getHBaseManager().act(inTable, ((Table table, Admin admin) -> HBaseUtils.exist(table.getName(), admin)))) {
            throw new IllegalArgumentException(String.format("Input table %s does not exist!!!", inTable));
        }

        /* -------------------------------*/
        // INIT META Data
        scm = new HBaseStudyConfigurationManager(Bytes.toString(gh.getOutputTable()), conf, null);
        HBaseVariantStudyConfiguration studyConfiguration = loadStudyConfiguration(studyId);

        List<BatchFileOperation> batches = studyConfiguration.getBatches();
        BatchFileOperation batchFileOperation;
        if (!batches.isEmpty()) {
            batchFileOperation = batches.get(batches.size() - 1);
            BatchFileOperation.Status currentStatus;
            try {
                currentStatus = batchFileOperation.currentStatus();
            } catch (Exception e) {
                throw e;
            }
            if (currentStatus != null) {
                switch (currentStatus) {
                    case RUNNING:
                        throw new StorageHadoopException("Unable to load a new batch. Already loading batch: "
                                + batchFileOperation);
                    case READY:
                        batchFileOperation = new BatchFileOperation(fileIds, batchFileOperation.getTimestamp() + 1);
                        break;
                    case ERROR:
                        if (batchFileOperation.getFileIds().equals(fileIds)) {
                            LOG.info("Resuming Last batch loading due to error.");
                        } else {
                            throw new StorageHadoopException("Unable to resume last batch loading. Must load the same "
                                    + "files from the previous batch: " + batchFileOperation);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown Status " + currentStatus);
                }
            }
        } else {
            batchFileOperation = new BatchFileOperation(fileIds, 1);
        }
        batchFileOperation.addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.RUNNING);
        batches.add(batchFileOperation);

        scm.updateStudyConfiguration(studyConfiguration, new QueryOptions());


        /* -------------------------------*/
        // JOB setup
        Job job = Job.getInstance(conf, "opencga: Load file " + Arrays.toString(fileArr) + " to VariantTable '" + outTable + "'");
        job.setJarByClass(VariantTableMapper.class);    // class that contains mapper
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");

        // QUERY design
        Scan scan = new Scan();
        scan.setCaching(100);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        // specify return columns (file IDs)
        for (Integer fileId : fileIds) {
            scan.addColumn(gh.getColumnFamily(), Bytes.toBytes(ArchiveHelper.getColumnName(fileId)));
        }
        scan.addColumn(gh.getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);

        // set other scan attrs
        TableMapReduceUtil.initTableMapperJob(
                inTable,      // input table
                scan,             // Scan instance to control CF and attribute selection
                VariantTableMapper.class,   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job,
                conf.getBoolean(GenomeHelper.CONFIG_HBASE_ADD_DEPENDENCY_JARS, true));
        TableMapReduceUtil.initTableReducerJob(
                outTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                conf.getBoolean(GenomeHelper.CONFIG_HBASE_ADD_DEPENDENCY_JARS, true));
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        Thread hook = new Thread(() -> {
            try {
                if (!job.isComplete()) {
                    job.killJob();
                }
                batches.get(batches.size() - 1).addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.READY);
                scm.updateStudyConfiguration(studyConfiguration, new QueryOptions());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        boolean succeed = job.waitForCompletion(true);
        Runtime.getRuntime().removeShutdownHook(hook);
        if (!succeed) {
            LOG.error("error with job!");
        }
        if (succeed) {
            batchFileOperation.addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.READY);
            scm.updateStudyConfiguration(studyConfiguration, new QueryOptions());
            return 0;
        } else {
            batchFileOperation.addStatus(Calendar.getInstance().getTime(), BatchFileOperation.Status.ERROR);
            scm.updateStudyConfiguration(studyConfiguration, new QueryOptions());
            return 1;
        }
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

    private HBaseVariantStudyConfiguration loadStudyConfiguration(int studyId) throws IOException {
        QueryResult<StudyConfiguration> res = scm.getStudyConfiguration(studyId, new QueryOptions());
        if (res.getResult().size() != 1) {
            throw new NotSupportedException();
        }
        return scm.toHBaseStudyConfiguration(res.first());
    }

    public static String buildCommandLineArgs(String server, String inputTable, String outputTable, int studyId,
                                              List<Integer> fileIds, Map<String, Object> other) {
        StringBuilder stringBuilder = new StringBuilder().append(server).append(' ').append(inputTable).append(' ')
                .append(outputTable).append(' ').append(studyId).append(' ');

        stringBuilder.append(fileIds.stream().map(Object::toString).collect(Collectors.joining(",")));
        for (Map.Entry<String, Object> entry : other.entrySet()) {
            Object value = entry.getValue();
            if (value != null && (value instanceof Number
                    || value instanceof Boolean
                    || value instanceof String && !((String) value).contains(" "))) {
                stringBuilder.append(' ').append(entry.getKey()).append(' ').append(value);
            }
        }
        return stringBuilder.toString();
    }

    public static void main(String[] args) throws Exception {
        System.exit(privateMain(args, null));
    }

    public static int privateMain(String[] args, Configuration conf) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            conf = new Configuration();
        }
        VariantTableDriver driver = new VariantTableDriver();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);

        //get the args w/o generic hadoop args
        String[] toolArgs = parser.getRemainingArgs();

        int fixedSizeArgs = 5;
        if (toolArgs.length < fixedSizeArgs || (toolArgs.length - fixedSizeArgs) % 2 != 0) {
            System.err.printf("Usage: %s [generic options] <server> <input-table> <output-table> <studyId> <fileIds>"
                    + " [<key> <value>]*\n",
                    VariantTableDriver.class.getSimpleName());
            System.err.println("Found " + Arrays.toString(toolArgs));
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        HBaseManager.addHBaseSettings(conf, toolArgs[0]);
        conf.set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, toolArgs[1]);
        conf.set(CONFIG_VARIANT_TABLE_NAME, toolArgs[2]);
        conf.set(GenomeHelper.CONFIG_STUDY_ID, toolArgs[3]);
        conf.setStrings(CONFIG_VARIANT_FILE_IDS, toolArgs[4].split(","));
        for (int i = fixedSizeArgs; i < toolArgs.length; i = i + 2) {
            conf.set(toolArgs[i], toolArgs[i + 1]);
        }

        //set the configuration back, so that Tool can configure itself
        driver.setConf(conf);

        /* Alternative to using tool runner */
//      int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);
        int exitCode = driver.run(toolArgs);

        return exitCode;
    }

}
