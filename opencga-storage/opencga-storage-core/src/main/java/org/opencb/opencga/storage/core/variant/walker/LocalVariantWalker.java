package org.opencb.opencga.storage.core.variant.walker;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.VariantSparseFilterTask;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * Local (non-MapReduce) implementation of the variant walker.
 *
 * <p>Streams variant data from a {@link VariantDBIterator} through a subprocess (bash command or docker container)
 * via stdin/stdout. Mirrors the logic of the Hadoop StreamVariantMapper/StreamVariantReducer but runs locally
 * in a single thread.</p>
 */
public class LocalVariantWalker {

    private static final Logger LOG = LoggerFactory.getLogger(LocalVariantWalker.class);
    private static final int BUFFER_SIZE = 128 * 1024;
    private static final int STDERR_BUFFER_CAPACITY = 10 * 1024;

    private final VariantStorageMetadataManager metadataManager;
    private final VariantWriterFactory writerFactory;
    private final IOConnectorProvider ioConnectorProvider;
    private final boolean nativeSparse;

    // Configured for each walk
    private String commandLine;
    private int maxInputBytesPerProcess;
    private VariantOutputFormat format;
    private Query query;
    private QueryOptions queryOptions;

    // Process state
    private Process process;
    private DataOutputStream stdin;
    private DataInputStream stdout;
    private DataInputStream stderr;
    private DataWriter<Variant> variantDataWriter;
    private OutputStream stdoutOutputStream;
    private OutputStream stderrOutputStream;
    private Thread stdoutReaderThread;
    private Thread stderrReaderThread;
    private int processedBytes;
    private String currentChromosome;
    private boolean headerWritten;
    private int processCount;
    private VariantMetadata metadata;
    private VariantSparseFilterTask sparseFilterTask;
    private final Map<String, Long> counters = new LinkedHashMap<>();
    private final List<Throwable> throwables = Collections.synchronizedList(new ArrayList<>());
    private final LinkedList<String> stderrBuffer = new LinkedList<>();
    private int stderrBufferSize;

    public LocalVariantWalker(VariantStorageMetadataManager metadataManager,
                              VariantWriterFactory writerFactory,
                              IOConnectorProvider ioConnectorProvider,
                              boolean nativeSparse) {
        this.metadataManager = metadataManager;
        this.writerFactory = writerFactory;
        this.ioConnectorProvider = ioConnectorProvider;
        this.nativeSparse = nativeSparse;
    }

    /**
     * Walk variants through a subprocess command.
     *
     * @param outputFile   Output file URI for stdout (gzipped)
     * @param format       Variant output format to serialize variants to stdin
     * @param query        Variant query
     * @param queryOptions Query options
     * @param iterator     Variant iterator to read from
     * @param commandLine  Bash command line to execute
     * @param options      Storage engine options
     * @return List of output URIs (stdout, stderr, counters)
     * @throws StorageEngineException if there is any error
     */
    public List<URI> walk(URI outputFile, VariantOutputFormat format, Query query, QueryOptions queryOptions,
                          VariantDBIterator iterator, String commandLine, ObjectMap options) throws StorageEngineException {
        this.commandLine = commandLine;
        this.query = query;
        this.queryOptions = queryOptions;
        this.format = format;
        if (!this.format.isPlain()) {
            this.format = this.format.inPlain();
        }
        this.maxInputBytesPerProcess = options.getInt(VariantStorageOptions.WALKER_DOCKER_MAX_BYTES_PER_MAP.key(),
                1024 * 1024 * 1024);
        this.headerWritten = false;
        this.processCount = 0;
        this.currentChromosome = null;
        this.sparseFilterTask = (this.format == VariantOutputFormat.JSON_SPARSE && !nativeSparse)
                ? new VariantSparseFilterTask() : null;
        this.counters.clear();
        this.throwables.clear();
        this.stderrBuffer.clear();
        this.stderrBufferSize = 0;

        String outputPath = outputFile.getPath();
        URI stderrFile = URI.create(outputFile.toString() + ".stderr.txt.gz");
        URI countersFile = URI.create(outputFile.toString() + ".counters.json");

        try {
            // Open output streams
            stdoutOutputStream = new GZIPOutputStream(ioConnectorProvider.newOutputStreamRaw(outputFile));
            stderrOutputStream = new GZIPOutputStream(ioConnectorProvider.newOutputStreamRaw(stderrFile));

            // Process variants
            boolean started = false;
            while (iterator.hasNext() && !hasExceptions()) {
                Variant variant = iterator.next();

                if (!started) {
                    currentChromosome = variant.getChromosome();
                    startProcess();
                    started = true;
                } else if (processedBytes > maxInputBytesPerProcess) {
                    LOG.info("Processed bytes = " + processedBytes + " > " + maxInputBytesPerProcess + ". Restarting process.");
                    incrementCounter("restarted_process_bytes_limit");
                    closeProcess();
                    currentChromosome = variant.getChromosome();
                    startProcess();
                } else if (!ChromosomeUtils.naturalConsecutiveChromosomes(currentChromosome, variant.getChromosome())) {
                    LOG.info("Chromosome changed from " + currentChromosome + " to " + variant.getChromosome()
                            + ". Restarting process.");
                    incrementCounter("restarted_process_chr_change");
                    closeProcess();
                    currentChromosome = variant.getChromosome();
                    startProcess();
                }

                currentChromosome = variant.getChromosome();
                if (sparseFilterTask != null) {
                    sparseFilterTask.apply(Collections.singletonList(variant));
                }
                variantDataWriter.write(variant);
                stdin.flush();
                processedBytes = stdin.size();
                incrementCounter("input_records");
            }

            if (started) {
                closeProcess();
            }
        } catch (Throwable th) {
            addException(th);
        } finally {
            // Ensure process is closed
            try {
                closeProcess();
            } catch (Throwable th) {
                addException(th);
            }
            // Close output streams
            closeQuietly(stdoutOutputStream);
            closeQuietly(stderrOutputStream);
            stdoutOutputStream = null;
            stderrOutputStream = null;
        }

        throwExceptionIfAny();

        // Write counters file
        try {
            writeCounters(countersFile);
        } catch (IOException e) {
            throw new StorageEngineException("Error writing counters file", e);
        }

        // Build result list
        List<URI> result = new ArrayList<>(3);
        result.add(outputFile);
        if (fileExists(stderrFile)) {
            result.add(stderrFile);
        }
        if (fileExists(countersFile)) {
            result.add(countersFile);
        }
        return result;
    }

    private void startProcess() throws IOException, StorageEngineException {
        LOG.info("bash -ce '" + commandLine + "'");
        incrementCounter("start_process");

        ProcessBuilder builder = new ProcessBuilder("bash", "-ce", commandLine);
        builder.environment().put("TMPDIR", System.getProperty("java.io.tmpdir"));
        process = builder.start();
        processCount++;

        stdin = new DataOutputStream(new BufferedOutputStream(process.getOutputStream(), BUFFER_SIZE));

        stdout = new DataInputStream(new BufferedInputStream(process.getInputStream(), BUFFER_SIZE));
        stderr = new DataInputStream(new BufferedInputStream(process.getErrorStream()));

        stdoutReaderThread = new StdoutReaderThread(stdout);
        stderrReaderThread = new StderrReaderThread(stderr);
        stdoutReaderThread.setDaemon(true);
        stderrReaderThread.setDaemon(true);
        stdoutReaderThread.start();
        stderrReaderThread.start();

        variantDataWriter = writerFactory.newDataWriter(format, stdin, new Query(query), new QueryOptions(queryOptions));

        if (format.inPlain() == VariantOutputFormat.JSON || format.inPlain() == VariantOutputFormat.JSON_SPARSE) {
            if (metadata == null) {
                VariantMetadataFactory metadataFactory = new VariantMetadataFactory(metadataManager);
                metadata = metadataFactory.makeVariantMetadata(query, queryOptions);
            }
            ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
            objectMapper.writeValue((DataOutput) stdin, metadata);
            stdin.write('\n');
        }

        processedBytes = 0;

        variantDataWriter.open();
        variantDataWriter.pre();
        stdin.flush();
    }

    private void closeProcess() throws IOException, InterruptedException {
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
            if (stdin != null && process != null && process.isAlive()) {
                stdin.close();
            }
        } catch (Throwable th) {
            if (th instanceof IOException && "Stream closed".equals(th.getMessage())) {
                // Ignore "Stream closed" exception
            } else {
                addException(th);
            }
        } finally {
            stdin = null;
        }

        try {
            if (process != null) {
                int exitVal = process.waitFor();
                if (exitVal != 0) {
                    LOG.error("Process exited with code " + exitVal);
                    throw new IOException("Process exited with code " + exitVal);
                }
            }
        } catch (Throwable th) {
            addException(th);
        } finally {
            process = null;
        }

        try {
            if (stdout != null) {
                stdoutReaderThread.join();
                stdout.close();
            }
        } catch (Throwable th) {
            addException(th);
        } finally {
            // Clear stdout even if it fails to avoid closing it twice
            stdout = null;
            stdoutReaderThread = null;
        }

        try {
            if (stderr != null) {
                stderrReaderThread.join();
                stderr.close();
            }
        } catch (Throwable th) {
            addException(th);
        } finally {
            // Clear stderr even if it fails to avoid closing it twice
            stderr = null;
            stderrReaderThread = null;
        }
    }

    private class StdoutReaderThread extends Thread {
        private final DataInputStream stdout;

        StdoutReaderThread(DataInputStream stdout) {
            super("StdoutReaderThread");
            this.stdout = stdout;
        }

        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stdout))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("#")) {
                        if (!headerWritten) {
                            writeLine(stdoutOutputStream, line);
                        }
                        // else skip duplicate header
                    } else {
                        // length < 3 to include lines with a small combination of \n \r \t and spaces.
                        if (line.length() < 3 && StringUtils.isBlank(line)) {
                            incrementCounter("stdout_records_empty");
                            // Do not interrupt header with empty records
                        } else {
                            headerWritten = true;
                        }
                        writeLine(stdoutOutputStream, line);
                    }
                    incrementCounter("stdout_records");
                }
            } catch (Throwable th) {
                addException(th);
            }
        }
    }

    private class StderrReaderThread extends Thread {
        private static final String REPORTER_PREFIX = "reporter:";
        private static final String COUNTER_PREFIX = REPORTER_PREFIX + "counter:";
        private static final String STATUS_PREFIX = REPORTER_PREFIX + "status:";

        private final DataInputStream stderr;

        StderrReaderThread(DataInputStream stderr) {
            super("StderrReaderThread");
            this.stderr = stderr;
        }

        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stderr))) {
                writeLine(stderrOutputStream, "sub-process #" + processCount);
                writeLine(stderrOutputStream, "Start time : " + TimeUtils.getTimeMillis());
                writeLine(stderrOutputStream, "--- START STDERR ---");
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith(COUNTER_PREFIX)) {
                        parseCounter(line.substring(COUNTER_PREFIX.length()).trim());
                    } else if (line.startsWith(STATUS_PREFIX)) {
                        LOG.info("[STATUS] - " + line.substring(STATUS_PREFIX.length()).trim());
                    } else if (line.startsWith(REPORTER_PREFIX)) {
                        LOG.warn("Cannot parse reporter line: " + line);
                    } else {
                        synchronized (stderrBuffer) {
                            stderrBuffer.add(line);
                            stderrBufferSize += line.length();
                            while (stderrBufferSize > STDERR_BUFFER_CAPACITY && stderrBuffer.size() > 3) {
                                stderrBufferSize -= stderrBuffer.remove().length();
                            }
                        }
                        writeLine(stderrOutputStream, line);
                        LOG.info("[STDERR] - " + line);
                    }
                    incrementCounter("stderr_records");
                }
                writeLine(stderrOutputStream, "--- END STDERR ---");
            } catch (Throwable th) {
                addException(th);
            }
        }

        private void parseCounter(String trimmedLine) {
            String[] columns = trimmedLine.split(",");
            if (columns.length == 2) {
                try {
                    long value = Long.parseLong(columns[1]);
                    incrementCounter(columns[0], value);
                } catch (NumberFormatException e) {
                    LOG.warn("Cannot parse counter increment '" + columns[1] + "' from line: " + trimmedLine);
                }
            } else {
                LOG.warn("Cannot parse counter line: " + trimmedLine);
            }
        }
    }

    private synchronized void writeLine(OutputStream out, String line) throws IOException {
        if (out != null) {
            out.write(line.getBytes());
            out.write('\n');
        }
    }

    private void incrementCounter(String name) {
        incrementCounter(name, 1);
    }

    private synchronized void incrementCounter(String name, long value) {
        counters.merge(name, value, Long::sum);
    }

    private boolean hasExceptions() {
        return !throwables.isEmpty();
    }

    private void addException(Throwable th) {
        throwables.add(th);
        LOG.warn("Exception in LocalVariantWalker", th);
        if (th instanceof OutOfMemoryError) {
            try {
                Runtime rt = Runtime.getRuntime();
                LOG.warn("Catch OutOfMemoryError!");
                double mb = 1024.0 * 1024;
                LOG.warn(String.format("Memory usage. MaxMemory: %.2f MiB TotalMemory: %.2f MiB"
                                + " FreeMemory: %.2f MiB UsedMemory: %.2f MiB",
                        rt.maxMemory() / mb, rt.totalMemory() / mb,
                        rt.freeMemory() / mb, (rt.totalMemory() - rt.freeMemory()) / mb));
            } catch (Throwable t) {
                LOG.warn("Error printing memory status", t);
            }
        }
    }

    private void throwExceptionIfAny() throws StorageEngineException {
        if (hasExceptions()) {
            String message = "LocalVariantWalker failed:";
            synchronized (stderrBuffer) {
                if (!stderrBuffer.isEmpty()) {
                    String stderr = String.join("\n[STDERR] - ", stderrBuffer);
                    message += "\n[STDERR] - " + stderr;
                }
            }
            if (throwables.size() == 1) {
                Throwable cause = throwables.get(0);
                throwables.clear();
                throw new StorageEngineException(message, cause);
            } else {
                Throwable cause = throwables.get(0);
                StorageEngineException exception = new StorageEngineException(message, cause);
                for (int i = 1; i < throwables.size(); i++) {
                    exception.addSuppressed(throwables.get(i));
                }
                throwables.clear();
                throw exception;
            }
        }
    }

    private void writeCounters(URI countersFile) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        try (OutputStream os = ioConnectorProvider.newOutputStreamRaw(countersFile)) {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(os, counters);
        }
    }

    private boolean fileExists(URI uri) {
        try {
            Path path = Paths.get(uri);
            return Files.exists(path) && Files.size(path) > 0;
        } catch (Exception e) {
            return false;
        }
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                LOG.warn("Error closing stream", e);
            }
        }
    }
}
