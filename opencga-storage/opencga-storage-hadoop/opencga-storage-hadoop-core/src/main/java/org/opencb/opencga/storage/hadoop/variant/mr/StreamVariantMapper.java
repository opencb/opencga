package org.opencb.opencga.storage.hadoop.variant.mr;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StopWatch;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.storage.hadoop.variant.io.VariantExporterDriver.VariantExporterDirectMultipleOutputsMapper.buildOutputKeyPrefix;
import static org.opencb.opencga.storage.hadoop.variant.mr.StreamVariantDriver.STDERR_NAMED_OUTPUT;
import static org.opencb.opencga.storage.hadoop.variant.mr.StreamVariantDriver.STDOUT_NAMED_OUTPUT;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper.COUNTER_GROUP_NAME;

public class StreamVariantMapper extends VariantMapper<VariantLocusKey, Text> {
    private static final Log LOG = LogFactory.getLog(StreamVariantMapper.class);

    private static final int BUFFER_SIZE = 128 * 1024;
    public static final String MAX_INPUT_BYTES_PER_PROCESS = "opencga.variant.stream.maxInputBytesPerProcess";
    public static final String VARIANT_FORMAT = "opencga.variant.stream.format";
    public static final String COMMANDLINE_BASE64 = "opencga.variant.stream.commandline_base64";
    public static final String ADDENVIRONMENT_PARAM = "opencga.variant.stream.addenvironment";
    public static final String HAS_REDUCE = "opencga.variant.stream.hasReduce";
    public static final String DOCKER_PRUNE_OPTS = "opencga.variant.stream.docker.prune.opts";

    private final boolean verboseStdout = false;
    private static final long REPORTER_OUT_DELAY = 10 * 1000L;
    private static final long REPORTER_ERR_DELAY = 10 * 1000L;

    // Configured at SETUP
    private String commandLine;
    private int maxInputBytesPerProcess;
    private VariantWriterFactory.VariantOutputFormat format;
    private Map<String, String> envs;
    private VariantStorageMetadataManager metadataManager;
    private VariantWriterFactory writerFactory;
    private Query query;
    private QueryOptions options;
    private String firstVariant;
    private boolean multipleOutputs;

    private int processCount = 0;

    ////////////
    // Configured for every new process
    ////////////
    private Process process;
    private DataOutputStream stdin;
    private DataInputStream stdout;
    private DataInputStream stderr;
    private MRErrorThread stderrThread;
    private MROutputThread stdoutThread;
    private DataWriter<Variant> variantDataWriter;
    protected final List<Throwable> throwables = Collections.synchronizedList(new ArrayList<>());
    private int processedBytes = 0;
    private long numRecordsRead = 0;
    private long numRecordsWritten = 0;
    private MultipleOutputs<VariantLocusKey, Text> mos;
    private String stdoutBaseOutputPath;
    private String stderrBaseOutputPath;
    // auto-incremental number for each produced record.
    // These are used with the VariantLocusKey to ensure a sorted output.
    private int stdoutKeyNum;
    private int stderrKeyNum;
    private String currentChromosome;
    private int currentPosition;

    private volatile boolean processProvidedStatus_ = false;
    private VariantMetadata metadata;

    public static void setCommandLine(Job job, String commandLine) {
        String commandLineBase64 = Base64.getEncoder().encodeToString(commandLine.getBytes());
        job.getConfiguration().set(COMMANDLINE_BASE64, commandLineBase64);
    }

    public static void setVariantFormat(Job job, VariantWriterFactory.VariantOutputFormat format) {
        job.getConfiguration().set(VARIANT_FORMAT, format.toString());
    }

    public static void setMaxInputBytesPerProcess(Job job, int maxInputBytesPerProcess) {
        job.getConfiguration().setInt(MAX_INPUT_BYTES_PER_PROCESS, maxInputBytesPerProcess);
    }

    public static void setHasReduce(Job job, boolean hasReduce) {
        job.getConfiguration().setBoolean(HAS_REDUCE, hasReduce);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        commandLine = new String(Base64.getDecoder().decode(conf.get(COMMANDLINE_BASE64)));
        maxInputBytesPerProcess = conf.getInt(MAX_INPUT_BYTES_PER_PROCESS, 1024 * 1024 * 1024);
        format = VariantWriterFactory.toOutputFormat(conf.get(VARIANT_FORMAT), "");
        if (!format.isPlain()) {
            format = format.inPlain();
        }
        if (conf.getBoolean(HAS_REDUCE, false)) {
            // If the job has a reduce step, the output will be written by the reducer
            // No need to write the output here
            multipleOutputs = false;
        } else {
            // If the job does not have a reduce step, the output will be written by the mapper
            multipleOutputs = true;
        }

        envs = new HashMap<>();
        addEnvironment(envs, conf);
        // add TMPDIR environment variable with the value of java.io.tmpdir
        envs.put("TMPDIR", System.getProperty("java.io.tmpdir"));

        VariantTableHelper helper = new VariantTableHelper(conf);
        metadataManager = new VariantStorageMetadataManager(new HBaseVariantStorageMetadataDBAdaptorFactory(helper));
        writerFactory = new VariantWriterFactory(metadataManager);
        query = VariantMapReduceUtil.getQueryFromConfig(conf);
        options = VariantMapReduceUtil.getQueryOptionsFromConfig(conf);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        if (context.nextKeyValue()) {
            Variant currentValue = null;
            try {
                setup(context);
                startProcess(context);
                // Do-while instead of "while", as we've already called context.nextKeyValue() once
                do {
                    currentValue = context.getCurrentValue();
                    // Restart the process if the input bytes exceed the limit
                    // or if the chromosome changes
                    if (processedBytes > maxInputBytesPerProcess) {
                        LOG.info("Processed bytes = " + processedBytes + " > " + maxInputBytesPerProcess + ". Restarting process.");
                        restartProcess(context, "bytes_limit", false);
                    } else if (!VariantLocusKey.naturalConsecutiveChromosomes(currentChromosome, currentValue.getChromosome())) {
                        LOG.info("Chromosome changed from " + currentChromosome + " to " + currentValue.getChromosome()
                                + ". Restarting process.");
                        restartProcess(context, "chr_change", true);
                    }
                    map(context.getCurrentKey(), currentValue, context);
                } while (!hasExceptions() && context.nextKeyValue());
            } catch (Throwable th) {
                Object currentKey = context.getCurrentKey();
                if (currentKey != null) {
                    String keyStr;
                    if (currentKey instanceof ImmutableBytesWritable) {
                        keyStr = Bytes.toStringBinary(((ImmutableBytesWritable) currentKey).get());
                    } else {
                        keyStr = currentKey.toString();
                    }
                    String message = "Exception in mapper for key: '" + keyStr + "'";
                    try {
                        if (currentValue != null) {
                            message += " value: '" + currentValue + "'";
                        }
                    } catch (Throwable t) {
                        th.addSuppressed(t);
                    }
                    addException(message, th);
                } else {
                    addException(th);
                }
            }
            try {
                // Always call cleanup, even if there was an exception
                cleanup(context);
            } catch (Throwable th) {
                addException(th);
            }
        } else {
            context.getCounter(COUNTER_GROUP_NAME, "empty_input_split").increment(1);
        }
        throwExceptionIfAny();
    }

    private void restartProcess(Mapper<Object, Variant, VariantLocusKey, Text>.Context context, String reason, boolean restartOutput)
            throws IOException, InterruptedException, StorageEngineException {
        context.getCounter(COUNTER_GROUP_NAME, "restarted_process_" + reason).increment(1);
        closeProcess(context, restartOutput);
        startProcess(context);
    }

    private boolean hasExceptions() {
        return !throwables.isEmpty();
    }

    private void addException(String message, Throwable th) {
        th.addSuppressed(new AnnotationException(message));
        addException(th);
    }

    public static class AnnotationException extends RuntimeException {
        public AnnotationException(String message) {
            super(message);
        }
    }

    private void addException(Throwable th) {
        throwables.add(th);
        LOG.warn("{}", th);
        if (th instanceof OutOfMemoryError) {
            try {
                // Print the current memory status in multiple lines
                Runtime rt = Runtime.getRuntime();
                LOG.warn("Catch OutOfMemoryError!");
                LOG.warn("Free memory: " + rt.freeMemory());
                LOG.warn("Total memory: " + rt.totalMemory());
                LOG.warn("Max memory: " + rt.maxMemory());

                double mb = 1024 * 1024;
                th.addSuppressed(new AnnotationException(String.format("Memory usage. MaxMemory: %.2f MiB"
                                + " TotalMemory: %.2f MiB"
                                + " FreeMemory: %.2f MiB"
                                + " UsedMemory: %.2f MiB",
                        rt.maxMemory() / mb,
                        rt.totalMemory() / mb,
                        rt.freeMemory() / mb,
                        (rt.totalMemory() - rt.freeMemory()) / mb)));
            } catch (Throwable t) {
                // Ignore any exception while printing the memory status
                LOG.warn("Error printing memory status", t);
            }
        }
    }

    private void throwExceptionIfAny() throws IOException {
        if (hasExceptions()) {
            String message = "StreamVariantMapper failed:";
            if (stderrThread != null) {
                String stderr = String.join("\n[STDERR] - ", stderrThread.stderrBuffer);
                message += "\n[STDERR] - " + stderr;
            }
            if (throwables.size() == 1) {
                Throwable cause = throwables.get(0);
                throwables.clear();
                throw new IOException(message, cause);
            } else {
                IOException exception = new IOException(message);
                for (int i = 1; i < throwables.size(); i++) {
                    exception.addSuppressed(throwables.get(i));
                }
                throwables.clear();
                throw exception;
            }
        }
    }

    @Override
    protected void cleanup(Mapper<Object, Variant, VariantLocusKey, Text>.Context context) throws IOException, InterruptedException {
        closeProcess(context, true);
        dockerPruneImages(context.getConfiguration());
        super.cleanup(context);
    }

    private void dockerPruneImages(Configuration conf) {
        try {
            LOG.info("Pruning docker images");
            int maxImages = 5;


            String dockerPruneOpts = conf.get(DOCKER_PRUNE_OPTS, "");

            Command command = new Command(new String[]{"bash", "-c", "[ $(docker image ls  --format json | wc -l) -gt " + maxImages + " ] "
                    + "&& echo 'Run docker image prune' && docker image prune -f --all " + dockerPruneOpts
                    + "|| echo 'Skipping docker image prune. Less than " + maxImages + " images.'"}, Collections.emptyMap());
            command.run();
            int ecode = command.getExitValue();

            // Throw exception if the process failed
            if (ecode != 0) {
                throw new IOException("Error executing 'docker image prune -f -a'. Exit code: " + ecode);
            }
            LOG.info("Docker images pruned");
        } catch (IOException e) {
            addException(e);
        }
    }

    @Override
    protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
        numRecordsRead++;
        variantDataWriter.write(value);
        stdin.flush();
        processedBytes = stdin.size();
    }

    private void closeProcess(Context context, boolean closeOutputs) throws IOException, InterruptedException {

        try {
            if (variantDataWriter != null) {
                variantDataWriter.post();
                variantDataWriter.close();
            }
        } catch (Throwable th) {
            addException(th);
        } finally {
            variantDataWriter = null;
        }

        try {
            // Close the stream to the process
            // This will cause the process to finish
            // (if the process is reading from stdin, it will receive EOF)
            // If the process has already finished, the stdin.close() will throw an exception
            if (stdin != null && process.isAlive()) {
                stdin.close();
            }
        } catch (Throwable th) {
            if (th instanceof IOException && "Stream closed".equals(th.getMessage())) {
                // Ignore "Stream closed" exception
            } else {
                addException(th);
            }
        } finally {
            // Clear stdin even if it fails to avoid closing it twice
            stdin = null;
        }

        try {
            if (process != null) {
                // Wait for the process to finish
                int exitVal = process.waitFor();

                if (exitVal != 0) {
                    LOG.error("Process exited with code " + exitVal);
                    throw new IOException("Process exited with code " + exitVal);
                }
                process = null;
            }
        } catch (Throwable th) {
            addException(th);
        }

        try {
            if (stdout != null) {
                stdoutThread.join();
                stdout.close();
            }
        } catch (Throwable th) {
            addException(th);
        } finally {
            // Clear stdout even if it fails to avoid closing it twice
            stdout = null;
        }

        try {
            if (stderr != null) {
                stderrThread.join();
                stderr.close();
            }
        } catch (Throwable th) {
            addException(th);
        } finally {
            // Clear stderr even if it fails to avoid closing it twice
            stderr = null;
        }

        try {
            // Close the MultipleOutputs if required
            if (mos != null && closeOutputs) {
                mos.close();
                mos = null;
            }
        } catch (Throwable th) {
            addException(th);
        }
//        drainStdout(context);
    }

    private void startProcess(Context context) throws IOException, StorageEngineException, InterruptedException {
        LOG.info("bash -ce '" + commandLine + "'");
        context.getCounter(COUNTER_GROUP_NAME, "start_process").increment(1);

        Variant variant = context.getCurrentValue();
        if (variant.getChromosome().equals(currentChromosome)) {
            if (currentPosition >= variant.getStart()) {
                // Multiple variants might point to the same locus
                // In that case, simply increment the position
                currentPosition++;
            } else {
                currentPosition = variant.getStart();
            }
        } else {
            currentChromosome = variant.getChromosome();
            currentPosition = variant.getStart();
        }
        if (firstVariant == null) {
            firstVariant = variant.getChromosome() + ":" + variant.getStart();
        }
        if (multipleOutputs && mos == null) {
            mos = new MultipleOutputs<>(context);
            stdoutBaseOutputPath = buildOutputKeyPrefix(STDOUT_NAMED_OUTPUT, currentChromosome, currentPosition);
            stderrBaseOutputPath = buildOutputKeyPrefix(STDERR_NAMED_OUTPUT, currentChromosome, currentPosition);
        }
        stdoutKeyNum = 0;
        stderrKeyNum = 0;

        // Start the process
        ProcessBuilder builder = new ProcessBuilder("bash", "-ce", commandLine);
//        System.getenv().forEach((k, v) -> LOG.info("SYSTEM ENV: " + k + "=" + v));
//        builder.environment().forEach((k, v) -> LOG.info("ProcessBuilder ENV: " + k + "=" + v));
//        envs.forEach((k, v) -> LOG.info("Config ENV: " + k + "=" + v));
        builder.environment().putAll(envs);
        process = builder.start();
        processCount++;

        stdin = new DataOutputStream(new BufferedOutputStream(
                process.getOutputStream(),
                BUFFER_SIZE));
        stdout = new DataInputStream(new BufferedInputStream(
                process.getInputStream(),
                BUFFER_SIZE));

        stderr = new DataInputStream(new BufferedInputStream(process.getErrorStream()));

        stderrThread = new MRErrorThread(context);
        stdoutThread = new MROutputThread(context);
        stderrThread.start();
        stdoutThread.start();

        variantDataWriter = writerFactory.newDataWriter(format, stdin, new Query(query), new QueryOptions(options));


        if (format.inPlain() == VariantWriterFactory.VariantOutputFormat.JSON) {
            if (metadata == null) {
                VariantMetadataFactory metadataFactory = new VariantMetadataFactory(metadataManager);
                metadata = metadataFactory.makeVariantMetadata(query, options);
            }
            ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
            objectMapper.writeValue((DataOutput) stdin, metadata);
            stdin.write('\n');
        }

        processedBytes = 0;
        numRecordsRead = 0;
        numRecordsWritten = 0;

        variantDataWriter.open();
        variantDataWriter.pre();
        stdin.flush();

    }

    public static void setEnvironment(Job job, Map<String, String> env) {
        if (env == null || env.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            if (entry.getKey().contains(" ") || entry.getValue().contains(" ")) {
                throw new IllegalArgumentException("Environment variables cannot contain spaces: "
                        + "'" + entry.getKey() + "' = '" + entry.getValue() + "'");
            }
            if (entry.getKey().contains("=") || entry.getValue().contains("=")) {
                throw new IllegalArgumentException("Environment variables cannot contain '=': "
                        + "'" + entry.getKey() + "' = '" + entry.getValue() + "'");
            }
            if (sb.length() > 0) {
                sb.append(" ");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
        }
        job.getConfiguration().set(ADDENVIRONMENT_PARAM, sb.toString());
    }

    public static void addEnvironment(Map<String, String> env, Configuration conf) {
        String nameVals = conf.get(ADDENVIRONMENT_PARAM);
        // encoding "a=b c=d" from StreamJob
        if (nameVals == null) {
            return;
        }
        String[] nv = nameVals.split(" ");
        for (int i = 0; i < nv.length; i++) {
            String[] pair = nv[i].split("=", 2);
            if (pair.length != 2) {
                throw new IllegalArgumentException("Invalid name=value: " + nv[i]);
            } else {
                env.put(pair[0], pair[1]);
            }
        }
    }


    private class MROutputThread extends Thread {

        private final Mapper<Object, Variant, VariantLocusKey, Text>.Context context;
        private long lastStdoutReport = 0;
        private int numRecords = 0;

        MROutputThread(Context context) {
            this.context = context;
            setDaemon(true);
        }

        public void run() {
            Text line = new Text();
            LineReader stdoutLineReader = new LineReader(stdout);
            try {
                while (stdoutLineReader.readLine(line) > 0) {
                    write(line);
                    if (verboseStdout) {
                        LOG.info("[STDOUT] - " + line);
                    }
                    numRecordsWritten++;
                    long now = System.currentTimeMillis();
                    if (now - lastStdoutReport > REPORTER_OUT_DELAY) {
                        lastStdoutReport = now;
                        String hline = "Records R/W=" + numRecordsRead + "/" + numRecordsWritten;
                        if (!processProvidedStatus_) {
                            context.setStatus(hline);
                        } else {
                            context.progress();
                        }
                        LOG.info(hline);
                    }
                }
            } catch (Throwable th) {
                addException(th);
            }
        }

        private void write(Text line) throws IOException, InterruptedException {
            numRecords++;
            VariantLocusKey locusKey = new VariantLocusKey(currentChromosome, currentPosition,
                    StreamVariantReducer.STDOUT_KEY + (stdoutKeyNum++));
            if (multipleOutputs) {
                mos.write(STDOUT_NAMED_OUTPUT, locusKey, line, stdoutBaseOutputPath);
            } else {
                context.write(locusKey, line);
            }
        }
    }

    private class MRErrorThread extends Thread {

        private final Configuration conf;
        private final Mapper<Object, Variant, VariantLocusKey, Text>.Context context;
        private long lastStderrReport = 0;
        private final String reporterPrefix;
        private final String counterPrefix;
        private final String statusPrefix;
        private final LinkedList<String> stderrBuffer = new LinkedList<>();
        private int stderrBufferSize = 0;
        private static final int STDERR_BUFFER_CAPACITY = 10 * 1024;

        MRErrorThread(Context context) {
            this.context = context;
            this.conf = context.getConfiguration();
            this.reporterPrefix = conf.get("stream.stderr.reporter.prefix", "reporter:");
            this.counterPrefix = reporterPrefix + "counter:";
            this.statusPrefix = reporterPrefix + "status:";
            setDaemon(true);
        }

        public void run() {
            Text line = new Text();
            LineReader stderrLineReader = new LineReader(stderr);
            try {
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                write("---------- " + context.getTaskAttemptID().toString() + " -----------");
                write("Start time : " + TimeUtils.getTimeMillis());
                write("Input split : " + firstVariant);
                write("Batch start : " + currentChromosome + ":" + currentPosition);
                write("sub-process #" + processCount);
                write("--- START STDERR ---");
                int numRecords = 0;
                while (stderrLineReader.readLine(line) > 0) {
                    String lineStr = line.toString();
                    if (matchesReporter(lineStr)) {
                        if (matchesCounter(lineStr)) {
                            incrCounter(lineStr);
                        } else if (matchesStatus(lineStr)) {
                            processProvidedStatus_ = true;
                            setStatus(lineStr);
                        } else {
                            LOG.warn("Cannot parse reporter line: " + lineStr);
                        }
                    } else {
                        // Store STDERR in a circular buffer (just the last 10KB), and include it in case of exception
                        stderrBuffer.add(lineStr);
                        stderrBufferSize += lineStr.length();
                        while (stderrBufferSize > STDERR_BUFFER_CAPACITY && stderrBuffer.size() > 3) {
                            stderrBufferSize -= stderrBuffer.remove().length();
                        }
                        write(line);
                        numRecords++;
                        LOG.info("[STDERR] - " + lineStr);
                    }
                    long now = System.currentTimeMillis();
                    if (now - lastStderrReport > REPORTER_ERR_DELAY) {
                        lastStderrReport = now;
                        context.progress();
                    }
                    line.clear();
                }
                write("--- END STDERR ---");
                write("Execution time : " + TimeUtils.durationToString(stopWatch.now(TimeUnit.MILLISECONDS)));
                write("STDOUT lines : " + stdoutThread.numRecords);
                write("STDERR lines : " + numRecords);
            } catch (Throwable th) {
                addException(th);
            }
        }

        private void write(String line) throws IOException, InterruptedException {
            write(new Text(line));
        }

        private void write(Text line) throws IOException, InterruptedException {
            VariantLocusKey locusKey = new VariantLocusKey(currentChromosome, currentPosition,
                    StreamVariantReducer.STDERR_KEY + (stderrKeyNum++));

            if (multipleOutputs) {
                mos.write(STDERR_NAMED_OUTPUT, locusKey, line, stderrBaseOutputPath);
            } else {
                context.write(locusKey, line);
            }
        }

        private boolean matchesReporter(String line) {
            return line.startsWith(reporterPrefix);
        }

        private boolean matchesCounter(String line) {
            return line.startsWith(counterPrefix);
        }

        private boolean matchesStatus(String line) {
            return line.startsWith(statusPrefix);
        }

        private void incrCounter(String line) {
            String trimmedLine = line.substring(counterPrefix.length()).trim();
            String[] columns = trimmedLine.split(",");
            if (columns.length == 2) {
                try {
                    context.getCounter(COUNTER_GROUP_NAME, columns[0]).increment(Long.parseLong(columns[1]));
                } catch (NumberFormatException e) {
                    LOG.warn("Cannot parse counter increment '" + columns[1] + "' from line: " + line);
                }
            } else {
                LOG.warn("Cannot parse counter line: " + line);
            }
        }

        private void setStatus(String line) {
            context.setStatus(line.substring(statusPrefix.length()).trim());
        }
    }

}
