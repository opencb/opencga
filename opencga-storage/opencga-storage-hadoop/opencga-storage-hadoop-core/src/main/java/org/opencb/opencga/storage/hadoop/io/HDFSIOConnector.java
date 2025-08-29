package org.opencb.opencga.storage.hadoop.io;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.io.managers.IOConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collection;

/**
 * Created on 03/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HDFSIOConnector extends Configured implements IOConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSIOConnector.class);

    public HDFSIOConnector(ObjectMap options) {
        Configuration conf = new Configuration();
        if (options != null) {
            for (String key : options.keySet()) {
                conf.set(key, options.getString(key));
            }
        }
    }

    public HDFSIOConnector(Configuration conf) {
        super(conf);
    }

    public static boolean isLocal(URI uri, Configuration conf) {
        String scheme = uri.getScheme();
        return "file".equals(scheme) || StringUtils.isEmpty(scheme) && conf.get(FileSystem.FS_DEFAULT_NAME_KEY).startsWith("file:");
    }

    public static org.apache.hadoop.fs.Path getHdfsRootPath(Configuration conf) throws IOException {
        Collection<String> nameservices = conf.getTrimmedStringCollection(HdfsClientConfigKeys.DFS_NAMESERVICES);
        if (nameservices.isEmpty()) {
            nameservices = new HdfsConfiguration().getTrimmedStringCollection(HdfsClientConfigKeys.DFS_NAMESERVICES);
        }
        for (String nameServiceId : nameservices) {
            try {
                org.apache.hadoop.fs.Path hdfsRootPath = new org.apache.hadoop.fs.Path("hdfs", nameServiceId, "/");
//                LOGGER.info("Checking if {} is a valid HDFS path", hdfsRootPath);
                FileSystem hdfsFileSystem = hdfsRootPath.getFileSystem(conf);
                if (hdfsFileSystem != null) {
                    // FileSystem is not null, so it is a valid HDFS path
                    return hdfsRootPath;
                }
            } catch (Exception e) {
                LOGGER.debug("This file system is not hdfs:// . Skip!", e);
            }
        }
        return null;
    }

    private FileSystem getFileSystem(URI uri) throws IOException {
        return FileSystem.get(uri, getConf());
    }

    @Override
    public boolean isValid(URI uri) {
        try {
            return getFileSystem(uri) != null && !isLocal(uri, getConf());
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public InputStream newInputStreamRaw(URI uri) throws IOException {
        return getFileSystem(uri).open(new org.apache.hadoop.fs.Path(uri));
    }

    @Override
    public OutputStream newOutputStreamRaw(URI uri) throws IOException {
        return getFileSystem(uri).create(new org.apache.hadoop.fs.Path(uri));
    }

    @Override
    public boolean exists(URI uri) throws IOException {
        return getFileSystem(uri).exists(new org.apache.hadoop.fs.Path(uri));
    }

    @Override
    public boolean isDirectory(URI uri) throws IOException {
        return getFileSystem(uri).isDirectory(new org.apache.hadoop.fs.Path(uri));
    }

    @Override
    public boolean canWrite(URI uri) throws IOException {
        return true;
    }

    @Override
    public void copyFromLocal(Path localSourceFile, URI targetFile) throws IOException {
        getFileSystem(targetFile).copyFromLocalFile(
                new org.apache.hadoop.fs.Path(localSourceFile.toUri()),
                new org.apache.hadoop.fs.Path(targetFile));
    }

    @Override
    public void copyToLocal(URI sourceFile, Path localTargetFile) throws IOException {
        getFileSystem(sourceFile).copyToLocalFile(
                new org.apache.hadoop.fs.Path(sourceFile),
                new org.apache.hadoop.fs.Path(localTargetFile.toUri()));

    }

    @Override
    public boolean delete(URI uri) throws IOException {
        return getFileSystem(uri).delete(new org.apache.hadoop.fs.Path(uri), true);
    }

    @Override
    public long size(URI uri) throws IOException {
        return getFileSystem(uri).getFileStatus(new org.apache.hadoop.fs.Path(uri)).getLen();
    }

    @Override
    public String md5(URI uri) throws IOException {
        byte[] bytes = getFileSystem(uri).getFileChecksum(new org.apache.hadoop.fs.Path(uri)).getBytes();
        if (bytes == null) {
            return null;
        } else {
            return String.format("%032x", new BigInteger(1, bytes));
        }
    }
}
