/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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


    public HdfsCatalogIOManager(String propertiesFile) throws CatalogIOManagerException {
        super(propertiesFile);
    }

    public HdfsCatalogIOManager(Properties properties) throws CatalogIOManagerException {
        super(properties);
    }

    @Override
    protected void setProperties(Properties properties) throws CatalogIOManagerException {

    }

    @Override
    protected void checkUriExists(URI param) throws CatalogIOManagerException {

    }

    @Override
    protected void checkUriScheme(URI param) throws CatalogIOManagerException {

    }

    @Override
    protected void checkDirectoryUri(URI param, boolean writable) throws CatalogIOManagerException {

    }

    @Override
    public boolean exists(URI uri) {
        return false;
    }

    @Override
    public URI createDirectory(URI uri, boolean parents) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public void deleteDirectory(URI uri) throws IOException {

    }

    @Override
    public void deleteFile(URI fileUri) throws IOException {

    }

    @Override
    public void rename(URI oldName, URI newName) throws CatalogIOManagerException {

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
    public DataInputStream getFileObject(URI fileUri, int start, int limit) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public DataInputStream getGrepFileObject(URI fileUri, String pattern, boolean ignoreCase, boolean multi) throws CatalogIOManagerException {
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
