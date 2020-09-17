package org.opencb.opencga.storage.core.io.managers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.StringUtils;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created on 01/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class LocalIOConnector implements IOConnector {
    protected static Logger logger = LogManager.getLogger(LocalIOConnector.class);

    @Override
    public boolean isValid(URI uri) {
        return uri != null && (StringUtils.isEmpty(uri.getScheme()) || uri.getScheme().equals("file"));
    }

    @Override
    public InputStream newInputStreamRaw(URI uri) throws IOException {
        return Files.newInputStream(getPath(uri));
    }

    @Override
    public OutputStream newOutputStreamRaw(URI uri) throws IOException {
        return Files.newOutputStream(getPath(uri));
    }

    @Override
    public boolean exists(URI uri) throws IOException {
        return Files.exists(getPath(uri));
    }

    @Override
    public boolean isDirectory(URI uri) throws IOException {
        return getPath(uri).toFile().isDirectory();
    }

    @Override
    public boolean canWrite(URI uri) throws IOException {
        return getPath(uri).toFile().canWrite();
    }

    @Override
    public void copyFromLocal(Path localSourceFile, URI targetFile) throws IOException {
        Files.copy(localSourceFile, getPath(targetFile));
    }

    @Override
    public void copyToLocal(URI sourceFile, Path localTargetFile) throws IOException {
        Files.copy(getPath(sourceFile), localTargetFile);
    }

    public void move(URI source, URI target) throws IOException {
        Files.move(getPath(source), getPath(target));
    }

    public void createDirectory(URI directory) throws IOException {
        Files.createDirectory(getPath(directory));
    }

    @Override
    public boolean delete(URI uri) throws IOException {
        return Files.deleteIfExists(getPath(uri));
    }

    @Override
    public long size(URI uri) throws IOException {
        return Files.size(getPath(uri));
    }

    @Override
    public String md5(URI uri) throws IOException {
        String checksum;
        try {
            String[] command = {"md5sum", uri.getPath()};
            logger.debug("command = {} {}", command[0], command[1]);
            Process p = Runtime.getRuntime().exec(command);
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            checksum = br.readLine();

            if (p.waitFor() != 0) {
                //TODO: Handle error in checksum
                logger.info("checksum = " + checksum);
                br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                throw new IOException("md5sum failed with exit value : " + p.exitValue() + ". ERROR: " + br.readLine());
            }
        } catch (InterruptedException e) {
            //TODO: Handle error in checksum
            throw new IOException("Checksum error in file " + uri, e);
        }

        return checksum.split(" ")[0];
    }

    protected Path getPath(URI uri) {
        if (StringUtils.isEmpty(uri.getScheme())) {
            return Paths.get(uri.getPath());
        } else {
            return Paths.get(uri);
        }
    }
}
