package org.opencb.opencga.storage.core.io.managers;

import org.apache.solr.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class LocalIOManager implements IOManager {
    protected static Logger logger = LoggerFactory.getLogger(LocalIOManager.class);

    @Override
    public boolean supports(URI uri) {
        return StringUtils.isEmpty(uri.getScheme()) || uri.getScheme().equals("file");
    }

    @Override
    public InputStream newInputStreamRaw(URI uri) throws IOException {
        return Files.newInputStream(Paths.get(uri));
    }

    @Override
    public OutputStream newOutputStreamRaw(URI uri) throws IOException {
        return Files.newOutputStream(Paths.get(uri));
    }

    @Override
    public boolean exists(URI uri) throws IOException {
        return Files.exists(Paths.get(uri));
    }

    @Override
    public boolean isDirectory(URI uri) throws IOException {
        return Paths.get(uri).toFile().isDirectory();
    }

    @Override
    public boolean canWrite(URI uri) throws IOException {
        return Paths.get(uri).toFile().canWrite();
    }

    @Override
    public void copyFromLocal(Path localSourceFile, URI targetFile) throws IOException {
        Files.copy(localSourceFile, Paths.get(targetFile));
    }

    @Override
    public void copyToLocal(URI sourceFile, Path localTargetFile) throws IOException {
        Files.copy(Paths.get(sourceFile), localTargetFile);
    }

    public void move(URI source, URI target) throws IOException {
        Files.move(Paths.get(source), Paths.get(target));
    }

    public void createDirectory(URI directory) throws IOException {
        Files.createDirectory(Paths.get(directory));
    }

    @Override
    public boolean delete(URI uri) throws IOException {
        return Files.deleteIfExists(Paths.get(uri));
    }

    @Override
    public long size(URI uri) throws IOException {
        return Files.size(Paths.get(uri));
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
}
