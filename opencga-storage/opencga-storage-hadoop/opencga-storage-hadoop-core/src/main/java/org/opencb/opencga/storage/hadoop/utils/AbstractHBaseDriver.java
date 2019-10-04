package org.opencb.opencga.storage.hadoop.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created on 24/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractHBaseDriver extends Configured implements Tool {

    public static final String COUNTER_GROUP_NAME = VariantsTableMapReduceHelper.COUNTER_GROUP_NAME;
    public static final String COLUMNS_TO_COUNT = "columns_to_count";
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHBaseDriver.class);
    protected String table;

    public AbstractHBaseDriver() {
    }

    public AbstractHBaseDriver(Configuration conf) {
        super(conf);
    }

    protected abstract String getJobName();

    private Job newJob() throws IOException {
        Job job = Job.getInstance(getConf(), getJobName());
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.setJarByClass(AbstractHBaseDriver.class);    // class that contains mapper
        return job;
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
        return getConf().get(key, getConf().get("--" + key, defaultValue));
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
        LOGGER.info("Execute " + getJobName());
        LOGGER.info("=================================================");
        boolean succeed = executeJob(job);
        if (!succeed) {
            LOGGER.error("error with job!");
        }
        LOGGER.info("=================================================");
        LOGGER.info("Finish job " + getJobName());
        LOGGER.info("Total time : " + TimeUtils.durationToString(stopWatch));
        LOGGER.info("=================================================");
        postExecution(job);
        close();

        return succeed ? 0 : 1;
    }

    private boolean configFromArgs(String[] args) {
        int fixedSizeArgs = getFixedSizeArgs();

        LOGGER.info(Arrays.toString(args));
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

        // Get first other args to avoid overwrite the fixed position args.
        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            getConf().set(args[i], args[i + 1]);
        }

        parseFixedParams(args);
        return false;
    }

    protected void parseFixedParams(String[] args) {
        table = args[0];
    }

    private boolean executeJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        Thread hook = new Thread(() -> {
            try {
                if (!job.isComplete()) {
                    job.killJob();
                }
//                onError();
            } catch (IOException e) {
                LOGGER.error("Error", e);
            }
        });
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            return job.waitForCompletion(true);
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    /**
     * Concatenate all generated files from a MapReduce job into one single local file.
     * @param mrOutdir      MapReduce output directory
     * @param localOutput   Local file
     * @throws IOException  on IOException
     */
    protected void concatMrOutputToLocal(Path mrOutdir, Path localOutput) throws IOException {
        FileSystem fileSystem = mrOutdir.getFileSystem(getConf());
        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(mrOutdir, false);
        try (FSDataOutputStream os = localOutput.getFileSystem(getConf()).create(localOutput)) {
            while (it.hasNext()) {
                Path path = it.next().getPath();
                if (!path.getName().equals(FileOutputCommitter.SUCCEEDED_FILE_NAME)
                        && !path.getName().equals(FileOutputCommitter.PENDING_DIR_NAME)) {
                    try (FSDataInputStream is = fileSystem.open(path)) {
                        IOUtils.copyBytes(is, os, getConf(), false);
                    }
                }
            }
        }
    }

    public static String[] buildArgs(String table, ObjectMap options) {
        List<String> args = new ArrayList<>(1 + options.size() * 2);

        args.add(table);
        for (String key : options.keySet()) {
            String value = options.getString(key);
            if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
                args.add(key);
                args.add(value);
            }
        }
        return args.toArray(new String[args.size()]);
    }

    protected static void main(String[] args, Class<? extends Tool> aClass) {
        try {
            Tool tool = aClass.newInstance();
            int code = ToolRunner.run(tool, args);
            System.exit(code);
        } catch (Exception e) {
            LoggerFactory.getLogger(aClass).error("Error executing " + aClass, e);
            System.exit(1);
        }
    }
}
