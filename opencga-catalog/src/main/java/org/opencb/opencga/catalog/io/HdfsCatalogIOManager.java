package org.opencb.opencga.catalog.io;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/**
 * Created by imedina on 03/10/14.
 */
public class HdfsCatalogIOManager extends CatalogIOManager {


    /**
     * This class implements all the operations for Hadoop HDFS. Useful links:
     *   http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html
     *   http://linuxjunkies.wordpress.com/2011/11/21/a-hdfsclient-for-hadoop-using-the-native-java-api-a-tutorial/
     */

    public void HdfsCatalogIOManager() throws IOException {
        /**
         * Hadoop XML need to be loaded:
         Configuration conf = new Configuration();
         conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
         conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
         conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

         FileSystem fileSystem = FileSystem.get(conf);
         */

    }


    public HdfsCatalogIOManager(String propertiesFile) throws IOException, CatalogIOManagerException {
        super(propertiesFile);
    }

    public HdfsCatalogIOManager(Properties properties) throws IOException, CatalogIOManagerException {
        super(properties);
    }

    @Override
    protected void setProperties(Properties properties) throws CatalogIOManagerException {

    }

    @Override
    protected void checkUri(URI param) throws CatalogIOManagerException {

    }

    @Override
    protected void checkDirectoryUri(URI param, boolean writable) throws CatalogIOManagerException {

    }

    @Override
    public boolean exists(URI uri) {
        return false;
    }

    @Override
    public URI createDirectory(URI uri, boolean parents) throws IOException {
        return null;
    }

    @Override
    public void deleteDirectory(URI uri) throws IOException {

    }

    @Override
    public void deleteFile(URI fileUri) throws IOException {

    }

    @Override
    public void rename(URI oldName, URI newName) throws CatalogIOManagerException, IOException {

    }

    @Override
    public boolean isDirectory(URI uri) {
        return false;
    }

    @Override
    public void copyFile(URI source, URI destination) { }

    @Override
    public void moveFile(URI source, URI target) throws IOException, CatalogIOManagerException {

    }

    @Override
    public URI getTmpUri() {
        return null;
    }

    @Override
    public void createFile(URI fileUri, InputStream inputStream) throws CatalogIOManagerException {

    }

    @Override
    public DataInputStream getFileObject(URI fileUri, int start, int limit) throws CatalogIOManagerException, IOException {
        return null;
    }

    @Override
    public DataInputStream getGrepFileObject(String userId, String projectId, String studyId, String objectId, String pattern, boolean ignoreCase, boolean multi) throws CatalogIOManagerException, IOException {
        return null;
    }

    @Override
    public String calculateChecksum(URI file) {
        return null;
    }

    @Override
    public List<URI> listFiles(URI directory) throws CatalogIOManagerException, IOException {
        return null;
    }

    @Override
    public long getFileSize(URI file) throws CatalogIOManagerException {
        return 0;
    }



}
