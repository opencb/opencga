package org.opencb.opencga.storage.core.io.managers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created on 01/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface IOManager {

    boolean supports(URI uri);

    InputStream newInputStreamRaw(URI uri) throws IOException;

    default InputStream newInputStream(URI uri) throws IOException {
        Logger logger = LoggerFactory.getLogger(IOManager.class);
        InputStream inputStream = newInputStreamRaw(uri);
        if (uri.getPath().endsWith(".gz")) {
            logger.debug("Gzip input compress");
            inputStream = new GZIPInputStream(inputStream);
        } else if (uri.getPath().endsWith(".snappy") || uri.getPath().endsWith(".snz")) {
            logger.debug("Snappy input compress");
            inputStream = new SnappyInputStream(inputStream);
        } else {
            logger.debug("Plain input");
        }
        return inputStream;
    }

    OutputStream newOutputStreamRaw(URI uri) throws IOException;

    default OutputStream newOutputStream(URI uri) throws IOException {
        Logger logger = LoggerFactory.getLogger(IOManager.class);
        OutputStream outputStream = newOutputStreamRaw(uri);
        if (uri.getPath().endsWith(".gz")) {
            logger.debug("Gzip output compress");
            outputStream = new GZIPOutputStream(outputStream);
        } else if (uri.getPath().endsWith(".snappy") || uri.getPath().endsWith(".snz")) {
            logger.debug("Snappy output compress");
            outputStream = new SnappyOutputStream(outputStream);
        } else {
            logger.debug("Plain output");
        }
        return outputStream;
    }

    boolean exists(URI uri) throws IOException;

    boolean isDirectory(URI uri) throws IOException;

    boolean canWrite(URI uri) throws IOException;

    default void checkWritable(URI uri) throws IOException {
        if (isDirectory(uri)) {
            throw new IOException(uri + ": Is a directory");
        } else {
            if (exists(uri)) {
                if (!canWrite(uri)) {
                    throw new IOException(uri + ": Permission denied");
                }
            } else {
                URI parentDir = uri.resolve(".");
                if (!canWrite(parentDir)) {
                    throw new IOException(parentDir + ": Permission denied");
                }
            }
        }
    }

    void copyFromLocal(Path localSourceFile, URI targetFile) throws IOException;

    void copyToLocal(URI sourceFile, Path localTargetFile) throws IOException;

    boolean delete(URI uri) throws IOException;

    long size(URI uri) throws IOException;

    String md5(URI uri) throws IOException;
}
