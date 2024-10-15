package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;

import java.io.*;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper.COUNTER_GROUP_NAME;

public class StreamVariantMapper extends VariantMapper<ImmutableBytesWritable, Text> {
    private static final Log LOG = LogFactory.getLog(StreamVariantMapper.class);

    private static final int BUFFER_SIZE = 128 * 1024;
    public static final String MAX_INPUT_BYTES_PER_PROCESS = "stream.maxInputBytesPerProcess";
    public static final String VARIANT_FORMAT = "opencga.variant.stream.format";
    public static final String COMMANDLINE_BASE64 = "opencga.variant.commandline_base64";
    public static final String ADDENVIRONMENT_PARAM = "opencga.variant.addenvironment";

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
    // Keep an auto-incremental number for each produced record. This is used as the key for the output record,
    // and will ensure a sorted output.
    private int outputKeyNum;

    // Configured for every new process
    private Process process;
    private DataOutputStream stdin;
    private DataInputStream stdout;
    private DataInputStream stderr;
    private MRErrorThread stderrThread;
    private MROutputThread stdoutThread;
    private DataWriter<Variant> variantDataWriter;
    private int processedBytes = 0;
    private long numRecordsRead = 0;
    private long numRecordsWritten = 0;
    protected final List<Throwable> throwables = Collections.synchronizedList(new ArrayList<>());

    private volatile boolean processProvidedStatus_ = false;

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

        envs = new HashMap<>();
        addEnvironment(envs, conf);
        // add TMPDIR environment variable with the value of java.io.tmpdir
        envs.put("TMPDIR", System.getProperty("java.io.tmpdir"));

        VariantTableHelper helper = new VariantTableHelper(conf);
        metadataManager = new VariantStorageMetadataManager(new HBaseVariantStorageMetadataDBAdaptorFactory(helper));
        writerFactory = new VariantWriterFactory(metadataManager);
        query = VariantMapReduceUtil.getQueryFromConfig(conf);
        options = VariantMapReduceUtil.getQueryOptionsFromConfig(conf);
        outputKeyNum = context.getCurrentKey().hashCode();
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        if (context.nextKeyValue()) {
            try {
                setup(context);
                startProcess(context);
                // Do-while instead of "while", as we've already called context.nextKeyValue() once
                do {
                    if (processedBytes > maxInputBytesPerProcess) {
                        LOG.info("Processed bytes = " + processedBytes + " > " + maxInputBytesPerProcess + ". Restarting process.");
                        context.getCounter(COUNTER_GROUP_NAME, "RESTARTED_PROCESS").increment(1);
                        closeProcess(context);
                        startProcess(context);
                    }
                    map(context.getCurrentKey(), context.getCurrentValue(), context);
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
                    addException("Exception in mapper for key: " + keyStr, th);
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
            context.getCounter(COUNTER_GROUP_NAME, "EMPTY_INPUT_SPLIT").increment(1);
        }
        throwExceptionIfAny();
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
                String stderr = String.join("\n", stderrThread.stderrBuffer);
                message += "\nSTDERR: " + stderr;
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
    protected void cleanup(Mapper<Object, Variant, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
        closeProcess(context);
        super.cleanup(context);
    }

    @Override
    protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
        numRecordsRead++;
        variantDataWriter.write(value);
        stdin.flush();
        processedBytes = stdin.size();
    }

    private void closeProcess(Context context) throws IOException, InterruptedException {

        try {
            if (variantDataWriter != null) {
                variantDataWriter.post();
                variantDataWriter.close();
            }

            // Close stdin to the process. This will cause the process to finish.
            if (stdin != null) {
                stdin.close();
                stdin = null;
            }

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
                stdout = null;
            }
        } catch (Throwable th) {
            addException(th);
        }
        try {
            if (stderr != null) {
                stderrThread.join();
                stderr.close();
                stderr = null;
            }
        } catch (Throwable th) {
            addException(th);
        }
//        drainStdout(context);
    }

    private void startProcess(Context context) throws IOException {
        LOG.info("bash -ce '" + commandLine + "'");
        context.getCounter(COUNTER_GROUP_NAME, "START_PROCESS").increment(1);

        // Start the process
        ProcessBuilder builder = new ProcessBuilder("bash", "-ce", commandLine);
//        System.getenv().forEach((k, v) -> LOG.info("SYSTEM ENV: " + k + "=" + v));
//        builder.environment().forEach((k, v) -> LOG.info("ProcessBuilder ENV: " + k + "=" + v));
//        envs.forEach((k, v) -> LOG.info("Config ENV: " + k + "=" + v));
        builder.environment().putAll(envs);
        process = builder.start();

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

        private final Mapper<Object, Variant, ImmutableBytesWritable, Text>.Context context;
        private long lastStdoutReport = 0;

        MROutputThread(Context context) {
            this.context = context;
            setDaemon(true);
        }

        public void run() {
            Text line = new Text();
            LineReader stdoutLineReader = new LineReader(stdout);
            try {
                while (stdoutLineReader.readLine(line) > 0) {
                    context.write(new ImmutableBytesWritable(Bytes.toBytes(outputKeyNum++)), line);
//                    context.write(null, line);
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
    }

    private class MRErrorThread extends Thread {

        private final Configuration conf;
        private final Mapper<Object, Variant, ImmutableBytesWritable, Text>.Context context;
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
                        LOG.info("[STDERR] - " + lineStr);
                    }
                    long now = System.currentTimeMillis();
                    if (now - lastStderrReport > REPORTER_ERR_DELAY) {
                        lastStderrReport = now;
                        context.progress();
                    }
                    line.clear();
                }
            } catch (Throwable th) {
                addException(th);
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
