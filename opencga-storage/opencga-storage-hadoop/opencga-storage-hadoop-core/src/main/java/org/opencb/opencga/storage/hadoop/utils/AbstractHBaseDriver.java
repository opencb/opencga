package org.opencb.opencga.storage.hadoop.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.commons.datastore.core.ObjectMap;
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

    public static final String COLUMNS_TO_COUNT = "columns_to_count";
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHBaseDriver.class);
    protected String table;

    protected abstract String getJobName();

    protected abstract void setupJob(Job job, String table) throws IOException;

    protected void parseAndValidateParameters() {
    }

    protected String getUsage() {
        return "Usage: " + getClass().getSimpleName()
                + " [generic options] <table> (<key> <value>)*";
    }

    protected int getFixedSizeArgs() {
        return 1;
    }

    protected void parseAndValidateArgs(String[] args) {
    }

    protected void preExecution() {

    }

    protected void postExecution(boolean succeed) {

    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        HBaseConfiguration.addHbaseResources(conf);
        getConf().setClassLoader(AbstractHBaseDriver.class.getClassLoader());
        configFromArgs(args);
        parseAndValidateParameters();

//        int maxKeyValueSize = conf.getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_KEYVALUE_SIZE_MAX, 10485760); // 10MB
//        logger.info("HBASE: set " + ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY + " to " + maxKeyValueSize);
//        conf.setInt(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, maxKeyValueSize); // always overwrite server default (usually 1MB)

        /* -------------------------------*/
        // JOB setup
        Job job = Job.getInstance(getConf(), getJobName());
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.setJarByClass(AbstractHBaseDriver.class);    // class that contains mapper

//        // Increase the ScannerTimeoutPeriod to avoid ScannerTimeoutExceptions
//        // See opencb/opencga#352 for more info.
//        int scannerTimeout = getConf().getInt(MAPREDUCE_HBASE_SCANNER_TIMEOUT,
//                getConf().getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
//        logger.info("Set Scanner timeout to " + scannerTimeout + " ...");
//        job.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, scannerTimeout);

        setupJob(job, table);


        preExecution();

        boolean succeed = executeJob(job);
        if (!succeed) {
            LOGGER.error("error with job!");
        }
        postExecution(succeed);

        return succeed ? 0 : 1;
    }

    protected final void configFromArgs(String[] args) {
        int fixedSizeArgs = getFixedSizeArgs();

        System.out.println(Arrays.toString(args));
        if (args.length < fixedSizeArgs || (args.length - fixedSizeArgs) % 2 != 0) {
            System.err.println(getUsage());
            System.err.println("Found " + Arrays.toString(args));
            ToolRunner.printGenericCommandUsage(System.err);
            throw new IllegalArgumentException("Wrong number of arguments!");
        }

        // Get first other args to avoid overwrite the fixed position args.
        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            getConf().set(args[i], args[i + 1]);
        }

        table = args[0];

        /* -------------------------------*/
        // Validate parameters CHECK
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("No table specified!");
        }

        parseAndValidateArgs(args);
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

}
