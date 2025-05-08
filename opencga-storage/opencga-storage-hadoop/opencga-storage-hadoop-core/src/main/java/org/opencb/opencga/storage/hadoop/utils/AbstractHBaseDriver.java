package org.opencb.opencga.storage.hadoop.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.ExceptionUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.variant.mr.AbstractHBaseVariantTableInputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.MR_EXECUTOR_SSH_PASSWORD;

/**
 * Created on 24/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractHBaseDriver extends Configured implements Tool {
    public static final String COUNTER_GROUP_NAME = "OPENCGA.HBASE";
    public static final String COLUMNS_TO_COUNT = "columns_to_count";
    public static final String MR_APPLICATION_ID = "MR_APPLICATION_ID";
    public static final String ERROR_MESSAGE = "ERROR_MESSAGE";
    public static final String OUTPUT_PARAM = "output";
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHBaseDriver.class);
    public static final String ARGS_FROM_STDIN = "STDIN";
    protected String table;

    public AbstractHBaseDriver() {
    }

    public AbstractHBaseDriver(Configuration conf) {
        super(conf);
    }

    protected abstract String getJobName();

    private Job newJob() throws IOException {
        LOGGER.info("Create MR Job");
        Job job = Job.getInstance(getConf(), getJobName());
        job.getConfiguration().set(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, "true");
        job.setJarByClass(AbstractHBaseDriver.class);    // class that contains mapper
        addJobConf(job, MRJobConfig.PRIORITY);
        addJobConf(job, MRJobConfig.QUEUE_NAME);
        addJobConf(job, MRJobConfig.MAP_MEMORY_MB);
        addJobConf(job, MRJobConfig.REDUCE_MEMORY_MB);
        addJobConf(job, MRJobConfig.MAP_CPU_VCORES);
        addJobConf(job, MRJobConfig.REDUCE_CPU_VCORES);
        addJobConf(job, MRJobConfig.JOB_RUNNING_MAP_LIMIT);
        addJobConf(job, MRJobConfig.JOB_RUNNING_REDUCE_LIMIT);
        addJobConf(job, MRJobConfig.TASK_TIMEOUT);
        VariantMapReduceUtil.configureTaskJavaHeap(((JobConf) job.getConfiguration()), getClass());
        return job;
    }

    private void addJobConf(Job job, String key) {
        String value = getParam(key);
        if (StringUtils.isNotEmpty(value)) {
            LOGGER.info("Configure MR job with {} = {}", key, value);
            job.getConfiguration().set(key, value);
        }
    }

    protected abstract void setupJob(Job job, String table) throws IOException;

    protected void parseAndValidateParameters() throws IOException {
        /* -------------------------------*/
        // Validate parameters CHECK
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("No table specified!");
        }
    }

    protected String getUsage() {
        return "Usage: " + getClass().getSimpleName() + " [generic options] <table> (<key> <value>)*";
    }

    protected String getParam(String key) {
        return getParam(key, null);
    }

    protected String getParam(String key, String defaultValue) {
        return VariantMapReduceUtil.getParam(getConf(), key, defaultValue, getClass());
    }

    protected int getFixedSizeArgs() {
        return 1;
    }

    protected void preExecution() throws IOException, StorageEngineException {

    }

    protected void postExecution(Job job) throws IOException, StorageEngineException {
        postExecution(job.isSuccessful());
    }

    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {

    }

    protected void close() throws IOException, StorageEngineException {
    }

    @Override
    public final int run(String[] args) throws Exception {

        Configuration conf = getConf();
        HBaseConfiguration.addHbaseResources(conf);
        getConf().setClassLoader(AbstractHBaseDriver.class.getClassLoader());
        if (args.length == 1 && args[0].equals(ARGS_FROM_STDIN)) {
            // Read args from STDIN
            List<String> argsFromStdin = new LinkedList<>();
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNextLine()) {
                String arg = scanner.nextLine();
                LOGGER.debug("Read arg from STDIN: " + arg);
                argsFromStdin.add(arg);
            }
            LOGGER.info("Read " + argsFromStdin.size() + " args from STDIN");
            scanner.close();
            args = argsFromStdin.toArray(new String[0]);
        }
        if (configFromArgs(args)) {
            return 1;
        }

        // Other user defined validations
        parseAndValidateParameters();

        /* -------------------------------*/
        // JOB setup
        Job job = newJob();
        setupJob(job, table);


        preExecution();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        LOGGER.info("=================================================");
        LOGGER.info("Execute '" + getJobName() + "'");
        LOGGER.info("   * InputFormat   : " + job.getInputFormatClass().getName());
        if (StringUtils.isNotEmpty(job.getConfiguration().get(TableInputFormat.INPUT_TABLE))) {
            LOGGER.info("     - InputTable  : " + job.getConfiguration().get(TableInputFormat.INPUT_TABLE));
            if (job.getConfiguration().getBoolean(AbstractHBaseVariantTableInputFormat.USE_SAMPLE_INDEX_TABLE_INPUT_FORMAT, false)) {
                String sampleIndexTable = job.getConfiguration().get(AbstractHBaseVariantTableInputFormat.SAMPLE_INDEX_TABLE);
                if (StringUtils.isNotEmpty(sampleIndexTable)) {
                    LOGGER.info("     - SecondarySampleIndexTable  : " + sampleIndexTable);
                } else {
                    LOGGER.info("     - SecondarySampleIndexTable  : (not set)");
                }
            }
        } else if (StringUtils.isNotEmpty(job.getConfiguration().get(PhoenixConfigurationUtil.INPUT_TABLE_NAME))) {
            LOGGER.info("     - InputPTable : " + job.getConfiguration().get(PhoenixConfigurationUtil.INPUT_TABLE_NAME));
        } else if (StringUtils.isNotEmpty(job.getConfiguration().get(FileInputFormat.INPUT_DIR))) {
            LOGGER.info("     - InputDir    : " + job.getConfiguration().get(FileInputFormat.INPUT_DIR));
        }
        int numMapLimit = job.getConfiguration().getInt(MRJobConfig.JOB_RUNNING_MAP_LIMIT, 0);
        if (numMapLimit > 0) {
            LOGGER.info("   * Mapper        : " + numMapLimit + "x " + job.getMapperClass().getName());
        } else {
            LOGGER.info("   * Mapper        : " + job.getMapperClass().getName());
        }
        JobConf jobConf = (JobConf) job.getConfiguration();
        LOGGER.info("     - memory required (MB) : " + jobConf.getMemoryRequired(TaskType.MAP));
        LOGGER.info("     - java-heap (MB) : " + JobConf.parseMaximumHeapSizeMB(jobConf.getTaskJavaOpts(TaskType.MAP)));
        LOGGER.info("     - java-opts : " + jobConf.getTaskJavaOpts(TaskType.MAP));
        if (job.getNumReduceTasks() > 0) {
            LOGGER.info("   * Reducer       : " + job.getNumReduceTasks() + "x " + job.getReducerClass().getName());
            LOGGER.info("     - memory required (MB) : " + jobConf.getMemoryRequired(TaskType.REDUCE));
            LOGGER.info("     - java-heap (MB) : " + JobConf.parseMaximumHeapSizeMB(jobConf.getTaskJavaOpts(TaskType.REDUCE)));
            LOGGER.info("     - java-opts : " + jobConf.getTaskJavaOpts(TaskType.REDUCE));
        } else {
            LOGGER.info("   * Reducer       : (no reducer)");
        }
        Class<? extends OutputFormat<?, ?>> outputFormatClass = job.getOutputFormatClass();
        LOGGER.info("   * OutputFormat  : " + outputFormatClass.getName());
        if (outputFormatClass.equals(LazyOutputFormat.class)) {
            outputFormatClass = (Class<? extends OutputFormat<?, ?>>) job.getConfiguration()
                    .getClass(LazyOutputFormat.OUTPUT_FORMAT, null, OutputFormat.class);
            LOGGER.info("     - BaseOutputFormat  : " + outputFormatClass.getName());
        }
        if (outputFormatClass.equals(TableOutputFormat.class)
                && StringUtils.isNotEmpty(job.getConfiguration().get(TableOutputFormat.OUTPUT_TABLE))) {
            LOGGER.info("     - OutputTable : " + job.getConfiguration().get(TableOutputFormat.OUTPUT_TABLE));
        } else if (StringUtils.isNotEmpty(job.getConfiguration().get(FileOutputFormat.OUTDIR))) {
            LOGGER.info("     - Outdir      : " + job.getConfiguration().get(FileOutputFormat.OUTDIR));

            if (TextOutputFormat.getCompressOutput(job)) {
                Class<? extends CompressionCodec> compressorClass = TextOutputFormat.getOutputCompressorClass(job, GzipCodec.class);
                LOGGER.info("     - Compress    : " + compressorClass.getName());
            }
        }
        LOGGER.info("=================================================");
        LOGGER.info("tmpjars=" + Arrays.toString(job.getConfiguration().getStrings("tmpjars")));
        LOGGER.info("tmpfiles=" + Arrays.toString(job.getConfiguration().getStrings("tmpfiles")));
        reportRunningJobs();
        boolean succeed = executeJob(job);
        if (!succeed) {
            LOGGER.error("error with job!");
            if (!"NA".equals(job.getStatus().getFailureInfo())) {
                String errorMessage = job.getStatus().getFailureInfo().replace("\n", "\\n");
                errorMessage += getExtendedTaskErrorMessage(job);
                LOGGER.error("Failure info: " + errorMessage.replace("\\n", "\n"));
                printKeyValue(ERROR_MESSAGE, errorMessage);
            }
        }
        LOGGER.info("=================================================");
        LOGGER.info("Finish job " + getJobName());
        LOGGER.info("Total time : " + TimeUtils.durationToString(stopWatch));
        LOGGER.info("=================================================");
        for (Counter counter : job.getCounters().getGroup(COUNTER_GROUP_NAME)) {
            printKeyValue(counter.getName(), counter.getValue());
        }
        postExecution(job);
        close();

        return succeed ? 0 : 1;
    }

    private static String getExtendedTaskErrorMessage(Job job) {
        try {
            StringBuilder sb = new StringBuilder();
            int eventCounter = 0;
            TaskCompletionEvent[] events;
            do {
                events = job.getTaskCompletionEvents(eventCounter, 10);
                eventCounter += events.length;
                for (TaskCompletionEvent event : events) {
                    if (event.getStatus() == TaskCompletionEvent.Status.FAILED) {
                        LOGGER.info(event.toString());
                        // Displaying the task diagnostic information
                        TaskAttemptID taskId = event.getTaskAttemptId();
                        String[] taskDiagnostics = job.getTaskDiagnostics(taskId);
                        if (taskDiagnostics != null) {
                            for (String diagnostics : taskDiagnostics) {
                                for (String diagnosticLine : diagnostics.split("\n")) {
                                    if (diagnosticLine.contains("Error:")
                                            || diagnosticLine.contains("Caused by:")
                                            || diagnosticLine.contains("Suppressed:")) {
                                        sb.append(diagnosticLine);
                                        sb.append("\\n");
                                    }
                                }
                            }
                        }
                    }
                }
            } while (events.length > 0);
            return sb.toString();
        } catch (Exception e) {
            // Ignore
            LOGGER.error("Error getting task diagnostics", e);
        }
        return "";
    }

    private void reportRunningJobs() {
        if (getConf().getBoolean("storage.hadoop.mr.skipReportRunningJobs", false)) {
            LOGGER.info("Skip report running jobs");
            return;
        }
        // Get the number of pending or running jobs in yarn
        try (YarnClient yarnClient = YarnClient.createYarnClient()) {
            yarnClient.init(getConf());
            yarnClient.start();

            List<ApplicationReport> applications = yarnClient.getApplications(EnumSet.of(
                    YarnApplicationState.NEW,
                    YarnApplicationState.NEW_SAVING,
                    YarnApplicationState.SUBMITTED,
                    YarnApplicationState.ACCEPTED,
                    YarnApplicationState.RUNNING));
            if (applications.isEmpty()) {
                LOGGER.info("No pending or running jobs in yarn");
            } else {
                LOGGER.info("Found " + applications.size() + " pending or running jobs in yarn");
                for (Map.Entry<YarnApplicationState, List<ApplicationReport>> entry : applications.stream()
                        .collect(Collectors.groupingBy(ApplicationReport::getYarnApplicationState)).entrySet()) {
                    LOGGER.info("   * " + entry.getKey() + " : " + entry.getValue().size());
                }
            }
        } catch (IOException | YarnException e) {
            LOGGER.error("Error getting list of pending jobs from YARN", e);
        }
    }

    private boolean configFromArgs(String[] args) {
        int fixedSizeArgs = getFixedSizeArgs();

        boolean help = ArrayUtils.contains(args, "-h") || ArrayUtils.contains(args, "--help");
        if (args.length < fixedSizeArgs || (args.length - fixedSizeArgs) % 2 != 0 || help) {
            System.err.println(getUsage());
            ToolRunner.printGenericCommandUsage(System.err);
            if (!help) {
//                System.err.println("Found " + Arrays.toString(args));
                throw new IllegalArgumentException("Wrong number of arguments!");
            }
            return true;
        }

        for (int i = 0; i < fixedSizeArgs; i++) {
            LOGGER.info("Fixed arg " + i + ": " + args[i]);
        }
        // Get first other args to avoid overwrite the fixed position args.
        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            String key = args[i];
            String value = args[i + 1];
            getConf().set(key, value);
            float maxLineLength = 300f;
            for (int batch = 0; batch < Math.ceil(value.length() / maxLineLength); batch++) {
                String prefix;
                if (batch == 0) {
                    prefix = "- " + key + ": ";
                } else {
                    prefix = StringUtils.repeat(' ', 6 + key.length());
                }
                int start = (int) (batch * maxLineLength);
                int end = (int) Math.min(value.length(), (batch + 1) * maxLineLength);
                LOGGER.info(prefix + value.substring(start, end));
            }
        }

        parseFixedParams(args);
        return false;
    }

    protected void parseFixedParams(String[] args) {
        table = args[0];
    }

    private boolean executeJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        Thread hook = new Thread(() -> {
            LOGGER.info("Shutdown hook called!");
            LOGGER.info("Gracefully stopping the job '" + job.getJobID() + "' ...");
            try {
                if (job.getJobState() == JobStatus.State.RUNNING) {
                    if (!job.isComplete()) {
                        job.killJob();
                    }
                    LOGGER.info("Job '" + job.getJobID() + "' stopped!");
                } else {
                    LOGGER.info("Job '" + job.getJobID() + "' is not running. Nothing to do.");
                }
//                onError();
            } catch (Exception e) {
                LOGGER.error("Error", e);
            }
        });
        try {
            job.submit();
            // Add shutdown hook after successfully submitting the job.
            Runtime.getRuntime().addShutdownHook(hook);
            JobID jobID = job.getJobID();
            String applicationId = jobID.appendTo(new StringBuilder(ApplicationId.appIdStrPrefix)).toString();
            printKeyValue(MR_APPLICATION_ID, applicationId);
            boolean completion = job.waitForCompletion(true);
            Runtime.getRuntime().removeShutdownHook(hook);
            return completion;
        } catch (Exception e) {
            // Do not use a finally block to remove shutdownHook, as finally blocks will be executed even if the JVM is killed,
            // and this would throw IllegalStateException("Shutdown in progress");
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (Exception e1) {
                e.addSuppressed(e1);
            }
            throw e;
        }
    }

    protected static void printKeyValue(String key, Object value) {
        // Print key value using System.err directly so it can be read by the MRExecutor
        // Do not use logger, as it may be redirected to a file or stdout
        // Do not use stdout, as it won't be read by the MRExecutor
        System.err.println(key + "=" + value);
    }

    protected void deleteTemporaryFile(Path outdir) throws IOException {
        LOGGER.info("Delete temporary file " + outdir.toUri());
        FileSystem fileSystem = outdir.getFileSystem(getConf());
        fileSystem.delete(outdir, true);
        fileSystem.cancelDeleteOnExit(outdir);
        LOGGER.info("Temporary file deleted!");
    }

    protected final int getServersSize(String table) throws IOException {
        int serversSize;
        try (HBaseManager hBaseManager = new HBaseManager(getConf())) {
            serversSize = hBaseManager.act(table, (t, admin) -> admin.getClusterStatus().getServersSize());
        }
        return serversSize;
    }

    public static String[] buildArgs(String table, ObjectMap options) {
        List<String> args = new ArrayList<>(1 + options.size() * 2);

        args.add(table);
        addMap(args, options, "");
        return args.toArray(new String[0]);
    }

    private static void addMap(List<String> args, ObjectMap options, String keyPrefix) {
        for (String key : options.keySet()) {
            if (options.get(key) instanceof Map) {
                ObjectMap map = new ObjectMap(options.getMap(key));
                addMap(args, map, keyPrefix + key + ".");
            } else {
                String value = options.getString(key);
                if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
                    String finalKey = keyPrefix + key;
                    if (finalKey.equals(MR_EXECUTOR_SSH_PASSWORD.key())) {
                        continue;
                    }
                    args.add(finalKey);
                    args.add(value);
                }
            }
        }
    }

    protected static void main(String[] args, Class<? extends Tool> aClass) {
        try {
            Tool tool = aClass.newInstance();
            int code = ToolRunner.run(tool, args);
            System.exit(code);
        } catch (Exception e) {
            LoggerFactory.getLogger(aClass).error("Error executing " + aClass, e);
            printKeyValue(ERROR_MESSAGE, ExceptionUtils.prettyExceptionMessage(e, false, true));
            System.exit(1);
        }
    }

    public int privateMain(String[] args) throws Exception {
        return privateMain(args, getConf());
    }

    public int privateMain(String[] args, Configuration conf) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf != null) {
            setConf(conf);
        }
        return ToolRunner.run(this, args);
    }
}
