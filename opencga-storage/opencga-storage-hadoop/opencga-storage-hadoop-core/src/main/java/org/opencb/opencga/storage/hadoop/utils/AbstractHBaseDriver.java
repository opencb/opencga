package org.opencb.opencga.storage.hadoop.utils;

import org.apache.commons.io.input.ReaderInputStream;
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
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.io.HDFSIOConnector;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opencb.opencga.core.common.IOUtils.humanReadableByteCount;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.MR_EXECUTOR_SSH_PASSWORD;

/**
 * Created on 24/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractHBaseDriver extends Configured implements Tool {
    public static final String COUNTER_GROUP_NAME = "OPENCGA.HBASE";
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
        addJobConf(job, MRJobConfig.JOB_RUNNING_MAP_LIMIT);
        addJobConf(job, MRJobConfig.JOB_RUNNING_REDUCE_LIMIT);
        addJobConf(job, MRJobConfig.TASK_TIMEOUT);
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

    protected Path getTempOutdir(String prefix) throws IOException {
        return getTempOutdir(prefix, "");
    }

    protected Path getTempOutdir(String prefix, String suffix) throws IOException {
        return getTempOutdir(prefix, suffix, false);
    }

    protected Path getTempOutdir(String prefix, String suffix, boolean ensureHdfs) throws IOException {
        if (StringUtils.isEmpty(suffix)) {
            suffix = "";
        } else if (!suffix.startsWith(".")) {
            suffix = "." + suffix;
        }
        // Be aware that
        // > ABFS does not allow files or directories to end with a dot.
        String fileName = prefix + "." + TimeUtils.getTime() + suffix;

        Path tmpDir = new Path(getConf().get("hadoop.tmp.dir"));
        if (ensureHdfs) {
            FileSystem fileSystem = tmpDir.getFileSystem(getConf());
            if (!fileSystem.getScheme().equals("hdfs")) {
                LOGGER.info("Temporary directory is not in hdfs:// . Hdfs is required for this temporary file.");
                LOGGER.info("   Default file system : " + fileSystem.getUri());
                for (String nameServiceId : getConf().getTrimmedStringCollection("dfs.nameservices")) {
                    try {
                        Path hdfsTmpPath = new Path("hdfs", nameServiceId, "/tmp/");
                        FileSystem hdfsFileSystem = hdfsTmpPath.getFileSystem(getConf());
                        if (hdfsFileSystem != null) {
                            LOGGER.info("Change to file system : " + hdfsFileSystem.getUri());
                            tmpDir = hdfsTmpPath;
                            break;
                        }
                    } catch (Exception e) {
                        LOGGER.debug("This file system is not hdfs:// . Skip!", e);
                    }
                }
            }
        }
        LOGGER.info("Temporary directory: " + tmpDir.toUri());
        return new Path(tmpDir, fileName);
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

    public class MapReduceOutputFile {
        public static final String OUTPUT_PARAM = "output";

        private final Supplier<String> nameGenerator;
        private final String tempFilePrefix;
        protected Path localOutput;
        protected Path outdir;

        public MapReduceOutputFile(Supplier<String> nameGenerator, String tempFilePrefix) throws IOException {
            this.nameGenerator = nameGenerator;
            this.tempFilePrefix = tempFilePrefix;
            getOutputPath();
        }

        protected void getOutputPath() throws IOException {
            String outdirStr = getParam(OUTPUT_PARAM);
            if (StringUtils.isNotEmpty(outdirStr)) {
                outdir = new Path(outdirStr);

                if (isLocal(outdir)) {
                    localOutput = AbstractHBaseDriver.this.getLocalOutput(outdir, nameGenerator);
                    outdir = getTempOutdir(tempFilePrefix, localOutput.getName());
                    outdir.getFileSystem(getConf()).deleteOnExit(outdir);
                }
                if (localOutput != null) {
                    LOGGER.info(" * Outdir file: " + localOutput.toUri());
                    LOGGER.info(" * Temporary outdir file: " + outdir.toUri());
                } else {
                    LOGGER.info(" * Outdir file: " + outdir.toUri());
                }
            }
        }

        public void postExecute(boolean succeed) throws IOException {
            if (succeed) {
                if (localOutput != null) {
                    concatMrOutputToLocal(outdir, localOutput);
                }
            }
            if (localOutput != null) {
                deleteTemporaryFile(outdir);
            }
        }

        public Path getLocalOutput() {
            return localOutput;
        }

        public Path getOutdir() {
            return outdir;
        }
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
                    && !path.getName().equals(ParquetFileWriter.PARQUET_METADATA_FILE)
                    && !path.getName().equals(ParquetFileWriter.PARQUET_COMMON_METADATA_FILE)
                    && status.getLen() > 0) {
                paths.add(path);
            }
        }
        if (paths.size() == 0) {
            LOGGER.warn("The MapReduce job didn't produce any output. This may not be expected.");
        } else if (paths.size() == 1) {
            LOGGER.info("Copy to local file " + paths.get(0).toUri() + " to " + localOutput.toUri());
            fileSystem.copyToLocalFile(false, paths.get(0), localOutput);
            LOGGER.info("File size : " + humanReadableByteCount(Files.size(Paths.get(localOutput.toUri())), false));
        } else {
            LOGGER.info("Concat and copy to local " + paths.size());
            LOGGER.info(" Source : " + mrOutdir.toUri());
            LOGGER.info(" Target : " + localOutput.toUri());
            LOGGER.info(" ---- ");
            try (FSDataOutputStream os = localOutput.getFileSystem(getConf()).create(localOutput)) {
                for (int i = 0; i < paths.size(); i++) {
                    Path path = paths.get(i);
                    LOGGER.info("Concat file : '{}' {} ", path.toUri(),
                            humanReadableByteCount(fileSystem.getFileStatus(path).getLen(), false));
                    try (FSDataInputStream fsIs = fileSystem.open(path)) {
                        BufferedReader br;
                        br = new BufferedReader(new InputStreamReader(fsIs));
                        InputStream is;
                        if (removeExtraHeaders && i != 0) {
                            String line;
                            do {
                                br.mark(10 * 1024 * 1024); //10MB
                                line = br.readLine();
                            } while (line != null && line.startsWith("#"));
                            br.reset();
                            is = new ReaderInputStream(br, Charset.defaultCharset());
                        } else {
                            is = fsIs;
                        }

                        IOUtils.copyBytes(is, os, getConf(), false);
                    }
                }
            }
            LOGGER.info("File size : " + humanReadableByteCount(Files.size(Paths.get(localOutput.toUri())), false));
        }
        return paths;
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
