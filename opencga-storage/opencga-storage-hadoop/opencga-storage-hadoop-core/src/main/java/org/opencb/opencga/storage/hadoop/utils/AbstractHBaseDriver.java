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
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.io.HDFSIOConnector;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.MR_EXECUTOR_SSH_PASSWORD;

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
        addJobConf(job, MRJobConfig.TASK_TIMEOUT);
        return job;
    }

    private void addJobConf(Job job, String key) {
        String value = getConf().get(getClass().getName() + "." + key);
        if (StringUtils.isEmpty(value)) {
            value = getConf().get(getClass().getSimpleName() + "." + key);
        }
        if (StringUtils.isEmpty(value)) {
            value = getConf().get(key);
        }

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
        String value = getConf().get(key);
        if (StringUtils.isEmpty(value)) {
            value = getConf().get("--" + key);
        }
        if (StringUtils.isEmpty(value)) {
            value = getConf().get(getClass().getName() + "." + key);
        }
        if (StringUtils.isEmpty(value)) {
            value = getConf().get(getClass().getSimpleName() + "." + key);
        }
        if (StringUtils.isEmpty(value)) {
            value = defaultValue;
        }
        return value;
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

    protected boolean isLocal(Path path) {
        return HDFSIOConnector.isLocal(path.toUri(), getConf());
    }

    protected Path getTempOutdir(String prefix) {
        return getTempOutdir(prefix, "");
    }

    protected Path getTempOutdir(String prefix, String sufix) {
        return new Path(getConf().get("hadoop.tmp.dir"), prefix + "." + TimeUtils.getTime() + "." + sufix);
    }

    protected Path getLocalOutput(Path outdir) throws IOException {
        return getLocalOutput(outdir, () -> null);
    }

    protected Path getLocalOutput(Path outdir, Supplier<String> nameGenerator) throws IOException {
        if (!isLocal(outdir)) {
            throw new IllegalArgumentException("Outdir " + outdir + " is not in the local filesystem");
        }
        Path localOutput = outdir;
        FileSystem localFs = localOutput.getFileSystem(getConf());
        if (localFs.exists(localOutput)) {
            if (localFs.isDirectory(localOutput)) {
                String name = nameGenerator.get();
                if (StringUtils.isEmpty(name)) {
                    throw new IllegalArgumentException("Local output '" + localOutput + "' is a directory");
                }
                localOutput = new Path(localOutput, name);
            } else {
                throw new IllegalArgumentException("File '" + localOutput + "' already exists!");
            }
        } else {
            if (!localFs.exists(localOutput.getParent())) {
                Files.createDirectories(Paths.get(localOutput.getParent().toUri()));
//                throw new IOException("No such file or directory: " + localOutput);
            }
        }
        return localOutput;
    }

    protected void deleteTemporaryFile(Path outdir) throws IOException {
        LOGGER.info("Delete temporary file " + outdir.toUri());
        FileSystem fileSystem = outdir.getFileSystem(getConf());
        fileSystem.delete(outdir, true);
        fileSystem.cancelDeleteOnExit(outdir);
    }

    /**
     * Concatenate all generated files from a MapReduce job into one single local file.
     *
     * @param mrOutdir      MapReduce output directory
     * @param localOutput   Local file
     * @throws IOException  on IOException
     * @return              List of copied files from HDFS
     */
    protected List<Path> concatMrOutputToLocal(Path mrOutdir, Path localOutput) throws IOException {
        return concatMrOutputToLocal(mrOutdir, localOutput, true);
    }

    /**
     * Concatenate all generated files from a MapReduce job into one single local file.
     *
     * @param mrOutdir      MapReduce output directory
     * @param localOutput   Local file
     * @param removeExtraHeaders Remove header lines starting with "#" from all files but the first
     * @throws IOException  on IOException
     * @return              List of copied files from HDFS
     */
    protected List<Path> concatMrOutputToLocal(Path mrOutdir, Path localOutput, boolean removeExtraHeaders) throws IOException {
        // TODO: Allow copy output to any IOConnector
        FileSystem fileSystem = mrOutdir.getFileSystem(getConf());
        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(mrOutdir, false);
        List<Path> paths = new ArrayList<>();
        while (it.hasNext()) {
            LocatedFileStatus status = it.next();
            Path path = status.getPath();
            if (status.isFile()
                    && !path.getName().equals(FileOutputCommitter.SUCCEEDED_FILE_NAME)
                    && !path.getName().equals(FileOutputCommitter.PENDING_DIR_NAME)
                    && status.getLen() > 0) {
                paths.add(path);
            }
        }
        if (paths.size() == 0) {
            LOGGER.warn("The MapReduce job didn't produce any output. This may not be expected.");
        } else if (paths.size() == 1) {
            LOGGER.info("Copy to local file " + paths.get(0) + " to " + localOutput);
            fileSystem.copyToLocalFile(false, paths.get(0), localOutput);
        } else {
            LOGGER.info("Concat and copy to local " + paths + " files from " + mrOutdir + " to " + localOutput);
            try (FSDataOutputStream os = localOutput.getFileSystem(getConf()).create(localOutput)) {
                for (int i = 0; i < paths.size(); i++) {
                    Path path = paths.get(i);
                    try (FSDataInputStream fsIs = fileSystem.open(path)) {
                        DataInputStream is;
                        if (removeExtraHeaders && i != 0) {
                            is = new DataInputStream(new BufferedInputStream(fsIs));
                            String line;
                            do {
                                is.mark(10 * 1024 * 1024); //10MB
                                line = is.readLine();
                            } while (line != null && line.startsWith("#"));
                            is.reset();
                        } else {
                            is = fsIs;
                        }

                        IOUtils.copyBytes(is, os, getConf(), false);
                    }
                }
            }
        }
        return paths;
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
            System.exit(1);
        }
    }
}
