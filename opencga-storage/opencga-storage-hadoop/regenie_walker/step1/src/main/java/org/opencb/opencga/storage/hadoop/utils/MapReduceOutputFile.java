package org.opencb.opencga.storage.hadoop.utils;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opencb.opencga.core.common.IOUtils.humanReadableByteCount;

public class MapReduceOutputFile {

    public static final String EXTRA_OUTPUT_PREFIX = "EXTRA_OUTPUT_";
    public static final String NAMED_OUTPUT = "NAMED_OUTPUT";
    public static final String EXTRA_NAMED_OUTPUT_PREFIX = "EXTRA_NAMED_OUTPUT_";
    private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceOutputFile.class);

    private final Configuration conf;
    private final Supplier<String> nameGenerator;
    private final Map<String, String> extraFiles = new HashMap<>();
    private String namedOutput;
    protected Path localOutput;
    protected Path outdir;

    public MapReduceOutputFile(String outdirStr, String tempFilePrefix, Configuration conf) throws IOException {
        this(outdirStr, null, tempFilePrefix, conf);
    }

    public MapReduceOutputFile(String outdirStr, Supplier<String> nameGenerator, String tempFilePrefix,
                               Configuration conf) throws IOException {
        this(outdirStr, nameGenerator, tempFilePrefix, false, conf);
    }

    public MapReduceOutputFile(String outdirStr, Supplier<String> nameGenerator, String tempFilePrefix, boolean ensureHdfs,
                               Configuration conf) throws IOException {
        this.conf = conf;
        this.nameGenerator = nameGenerator == null ? () -> null : nameGenerator;
        namedOutput = null;

        outdir = new Path(outdirStr);

        if (isLocal(outdir)) {
            localOutput = getLocalOutput(outdir);
            outdir = getTempOutdir(tempFilePrefix, localOutput.getName(), ensureHdfs, conf);
            outdir.getFileSystem(conf).deleteOnExit(outdir);
        }
        if (hasTempOutput()) {
            LOGGER.info(" * Output file      : " + toUri(localOutput));
            LOGGER.info(" * MapReduce outdir : " + toUri(outdir));
        } else {
            LOGGER.info(" * MapReduce outdir : " + toUri(outdir));
        }
    }

    public static Path getTempOutdir(String prefix, String suffix, boolean ensureHdfs, Configuration conf) throws IOException {
        if (StringUtils.isEmpty(suffix)) {
            suffix = "";
        } else if (!suffix.startsWith(".")) {
            suffix = "." + suffix;
        }
        // Be aware that
        // > ABFS does not allow files or directories to end with a dot.
        String fileName = prefix + "." + TimeUtils.getTime() + suffix;

        Path tmpDir = new Path(conf.get("hadoop.tmp.dir"));
        if (ensureHdfs) {
            if (!isHdfs(tmpDir, conf)) {
                LOGGER.info("Temporary directory is not in hdfs:// . Hdfs is required for this temporary file.");
                LOGGER.info("   Default file system : " + FileSystem.getDefaultUri(conf));
                for (String nameServiceId : conf.getTrimmedStringCollection("dfs.nameservices")) {
                    try {
                        Path hdfsTmpPath = new Path("hdfs", nameServiceId, "/tmp/");
                        FileSystem hdfsFileSystem = hdfsTmpPath.getFileSystem(conf);
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
        return new Path(tmpDir, fileName);
    }

    /**
     * Check if a given Hadoop path is local.
     * If the scheme is null, it will check the default hadoop file system.
     * @param path Hadoop path
     * @return true if the path is local
     */
    protected boolean isLocal(Path path) {
        URI uri = path.toUri();
        String scheme = uri.getScheme();
        if (StringUtils.isEmpty(scheme)) {
            scheme = FileSystem.getDefaultUri(conf).getScheme();
        }
        return "file".equals(scheme);
    }

    /**
     * Check if a given URI is local.
     * If the scheme is null, it assumes it is local.
     * @param uri   URI
     * @return  true if the URI is local
     */
    public static boolean isLocal(URI uri) {
        String scheme = uri.getScheme();
        if (StringUtils.isEmpty(scheme)) {
            scheme = "file";
        }
        return "file".equals(scheme);
    }

    public static boolean isHdfs(Path dir, Configuration conf) {
        try {
            String scheme = dir.toUri().getScheme();
            if (StringUtils.isEmpty(scheme)) {
                scheme = FileSystem.getDefaultUri(conf).getScheme();
                return scheme.equals("hdfs");
            }
            FileSystem fileSystem = dir.getFileSystem(conf);
            return fileSystem.getScheme().equals("hdfs");
        } catch (IOException e) {
            LOGGER.error("Error checking if " + dir + " is HDFS : " + e.getMessage());
            return false;
        }
    }

    public void postExecute(ObjectMap result, boolean succeed) throws IOException {
        readKeyValues(result);
        postExecute(succeed);
    }

    public void postExecute(boolean succeed) throws IOException {
        printKeyValue();
        if (succeed) {
            if (hasTempOutput()) {
                getConcatMrOutputToLocal();
            }
        }
        if (hasTempOutput()) {
            deleteTemporaryFile(outdir);
        }
    }

    private void readKeyValues(ObjectMap result) {
        for (String key : result.keySet()) {
            if (key.equals(MapReduceOutputFile.NAMED_OUTPUT)) {
                setNamedOutput(result.getString(key));
            } else if (key.startsWith(MapReduceOutputFile.EXTRA_NAMED_OUTPUT_PREFIX)) {
                addExtraNamedOutput(key.substring(MapReduceOutputFile.EXTRA_NAMED_OUTPUT_PREFIX.length()), result.getString(key));
            }
        }
    }

    private void printKeyValue() {
        // Print keyValues only if this method is being called from an instance of AbstractHBaseDriver
        // Check the stacktrace
        boolean found = false;
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement stackTraceElement : stackTrace) {
            try {
                Class<?> aClass = Class.forName(stackTraceElement.getClassName());
                if (AbstractHBaseDriver.class.isAssignableFrom(aClass)) {
                    found = true;
                    break;
                }
            } catch (ClassNotFoundException e) {
                // This should never happen
                throw new RuntimeException(e);
            }
        }
        if (!found) {
            return;
        }

        if (namedOutput != null) {
            AbstractHBaseDriver.printKeyValue(NAMED_OUTPUT, namedOutput);
        }
        for (Map.Entry<String, String> entry : extraFiles.entrySet()) {
            String suffix = entry.getValue();
            String partFilePrefix = entry.getKey();
            if (hasTempOutput()) {
                Path extraOutput = localOutput.suffix(suffix);
                AbstractHBaseDriver.printKeyValue(EXTRA_OUTPUT_PREFIX + partFilePrefix, extraOutput);
            } else {
                AbstractHBaseDriver.printKeyValue(EXTRA_NAMED_OUTPUT_PREFIX + partFilePrefix, suffix);
            }
        }
    }

    public boolean hasTempOutput() {
        return localOutput != null;
    }

    public MapReduceOutputFile setNamedOutput(String partFilePrefix) {
        this.namedOutput = partFilePrefix;
        return this;
    }

    public void addExtraNamedOutput(String namedOutput, String localOutputPrefix) {
        extraFiles.put(namedOutput, localOutputPrefix);
    }

    protected void getConcatMrOutputToLocal() throws IOException {
        concatMrOutputToLocal(outdir, localOutput, true, namedOutput);

        for (Map.Entry<String, String> entry : extraFiles.entrySet()) {
            String partFilePrefix = entry.getKey();
            String suffix = entry.getValue();
            Path extraOutput = localOutput.suffix(suffix);
            concatMrOutputToLocal(outdir, extraOutput, true, partFilePrefix);
            AbstractHBaseDriver.printKeyValue(EXTRA_OUTPUT_PREFIX + partFilePrefix.toUpperCase(), extraOutput);
        }
    }

    /**
     * Get the local output file. Might be null if the destination is HDFS.
     * @return Local output file
     */
    public Path getLocalOutput() {
        return localOutput;
    }

    /**
     * Get the actual output directory for the MapReduce job.
     * @return Output directory
     */
    public Path getOutdir() {
        return outdir;
    }

    public Configuration getConf() {
        return conf;
    }

    private URI toUri(Path path) throws IOException {
        URI tmpUri = path.toUri();
        if (tmpUri.getScheme() == null) {
            // If the scheme is null, add the default scheme
            FileSystem fileSystem = path.getFileSystem(conf);
            tmpUri = fileSystem.getUri().resolve(tmpUri.getPath());
        }
        return tmpUri;
    }

    protected Path getLocalOutput(Path outdir) throws IOException {
        if (!isLocal(outdir)) {
            throw new IllegalArgumentException("Outdir " + outdir + " is not in the local filesystem");
        }
        Path localOutput = outdir;
        FileSystem localFs = localOutput.getFileSystem(conf);
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
        FileSystem fileSystem = outdir.getFileSystem(conf);
        fileSystem.delete(outdir, true);
        fileSystem.cancelDeleteOnExit(outdir);
        LOGGER.info("Temporary file deleted!");
    }

    /**
     * Concatenate all generated files from a MapReduce job into one single local file.
     *
     * @param mrOutdir    MapReduce output directory
     * @param localOutput Local file
     * @return List of copied files from HDFS
     * @throws IOException on IOException
     */
    protected List<Path> concatMrOutputToLocal(Path mrOutdir, Path localOutput) throws IOException {
        return concatMrOutputToLocal(mrOutdir, localOutput, true, null);
    }

    /**
     * Concatenate all generated files from a MapReduce job into one single local file.
     *
     * @param mrOutdir           MapReduce output directory
     * @param localOutput        Local file
     * @param removeExtraHeaders Remove header lines starting with "#" from all files but the first
     * @param partFilePrefix     Filter partial files with specific prefix. Otherwise, concat them all.
     * @return List of copied files from HDFS
     * @throws IOException on IOException
     */
    protected List<Path> concatMrOutputToLocal(Path mrOutdir, Path localOutput, boolean removeExtraHeaders, String partFilePrefix)
            throws IOException {
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
                if (partFilePrefix == null || path.getName().startsWith(partFilePrefix)) {
                    paths.add(path);
                }
            }
        }
        FileSystem localFileSystem = localOutput.getFileSystem(getConf());
        localFileSystem.setWriteChecksum(false);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (paths.isEmpty()) {
            LOGGER.warn("The MapReduce job didn't produce any output. This may not be expected.");
        } else if (paths.size() == 1) {
            LOGGER.info("Copy to local file");
            LOGGER.info(" Source : {} ({})",
                    paths.get(0).toUri(), humanReadableByteCount(fileSystem.getFileStatus(paths.get(0)).getLen(), false));
            LOGGER.info(" Target : {}", localOutput.toUri());
            fileSystem.copyToLocalFile(false, paths.get(0), localOutput);
        } else {
            LOGGER.info("Concat and copy to local : " + paths.size() + " partial files");
            LOGGER.info(" Source {}: {}", getCompression(paths.get(0).getName()), mrOutdir.toUri());
            LOGGER.info(" Target {}: {}", getCompression(localOutput.getName()), localOutput.toUri());
            LOGGER.info(" ---- ");

            try (OutputStream os = getOutputStreamPlain(localOutput.getName(), localFileSystem.create(localOutput))) {
                for (int i = 0; i < paths.size(); i++) {
                    Path partFile = paths.get(i);
                    long partFileSize = fileSystem.getFileStatus(partFile).getLen();
                    LOGGER.info("[{}] Concat {} file : '{}' ({}) ",
                            i,
                            getCompression(partFile.getName()),
                            partFile.toUri(),
                            humanReadableByteCount(partFileSize, false));
                    InputStream is = null;
                    Throwable e = null;
                    try {
                        is = getInputStream(partFile.getName(), fileSystem.open(partFile));
                        // Remove extra headers from all files but the first
                        if (removeExtraHeaders && i != 0) {
                            BufferedReader br = new BufferedReader(new InputStreamReader(is));
                            String line;
                            do {
                                br.mark(10 * 1024 * 1024); //10MB
                                line = br.readLine();
                                // Skip blank lines and
                            } while (line != null && (StringUtils.isBlank(line) || line.startsWith("#")));
                            br.reset();
                            is = new ReaderInputStream(br, Charset.defaultCharset());
                        }

                        if (partFileSize > 50 * 1024 * 1024) {
                            org.opencb.opencga.core.common.IOUtils.copyBytesParallel(is, os, getConf().getInt(
                                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT));
                        } else {
                            org.apache.hadoop.io.IOUtils.copyBytes(is, os, getConf(), false);
                        }
                    } catch (Throwable throwable) {
                        e = throwable;
                        throw throwable;
                    } finally {
                        if (is != null) {
                            try {
                                is.close();
                            } catch (IOException ex) {
                                if (e == null) {
                                    throw ex;
                                } else {
                                    e.addSuppressed(ex);
                                }
                            }
                        }
                    }
                }
            }
            LOGGER.info("File size : " + humanReadableByteCount(Files.size(Paths.get(localOutput.toUri())), false));
            LOGGER.info("Time to copy from HDFS and concat : " + TimeUtils.durationToString(stopWatch));
        }
        return paths;
    }

    private static String getCompression(String name) throws IOException {
        if (name.endsWith(".gz")) {
            return "gzip";
        } else if (name.endsWith(".snappy")) {
            return "snappy";
        } else if (name.endsWith(".lz4")) {
            return "lz4";
        } else if (name.endsWith(".zst")) {
            return "ztandard";
        } else {
            return "plain";
        }
    }

    private OutputStream getOutputStreamPlain(String name, OutputStream os) throws IOException {
        CompressionCodec codec = getCompressionCodec(name);
        if (codec == null) {
            return os;
        }
        try {
            return codec.createOutputStream(os);
        } catch (UnsatisfiedLinkError error) {
            if (codec instanceof SnappyCodec) {
                return new SnappyOutputStream(os);
            } else {
                throw error;
            }
        }
    }

    private CompressionCodec getCompressionCodec(String name) throws IOException {
        return getCompressionCodec(getCompression(name), getConf());
    }

    public static CompressionCodec getCompressionCodec(String codecName, Configuration conf) throws IOException {
        Class<? extends CompressionCodec> codecClass;
        switch (codecName) {
            case "deflate":
                codecClass = DeflateCodec.class;
                break;
            case "gz":
            case "gzip":
                codecClass = GzipCodec.class;
                break;
            case "snappy":
                codecClass = SnappyCodec.class;
                break;
            case "lz4":
                codecClass = Lz4Codec.class;
                break;
            case "ztandard":
                codecClass = ZStandardCodec.class;
                break;
            case "bz":
                codecClass = BZip2Codec.class;
                break;
            case "plain":
                return null;
            default:
                throw new IOException("Unknown compression codec " + codecName);
        }
        return ReflectionUtils.newInstance(codecClass, conf);
    }

    private InputStream getInputStream(String name, InputStream is) throws IOException {
        CompressionCodec codec = getCompressionCodec(name);
        if (codec == null) {
            return is;
        }
        try {
            return codec.createInputStream(is);
        } catch (UnsatisfiedLinkError error) {
            if (codec instanceof SnappyCodec) {
                return new SnappyInputStream(is);
            } else {
                throw error;
            }
        }
    }
}
