package org.opencb.opencga.catalog.core.io;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;

/**
 * Created by imedina on 03/10/14.
 */
public class HdfsIOManager implements CatalogIOManager {


    /**
     * This class implements all the operations for Hadoop HDFS. Useful links:
     *   http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html
     *   http://linuxjunkies.wordpress.com/2011/11/21/a-hdfsclient-for-hadoop-using-the-native-java-api-a-tutorial/
     */

    @Override
    public void setup() throws IOException {
        /**
         * Hadoop XML need to be loaded:
         Configuration conf = new Configuration();
         conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
         conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
         conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

         FileSystem fileSystem = FileSystem.get(conf);
         */

    }

    @Override
    public Path getUserPath(String userId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path getAnonmousUserPath(String userId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path getProjectRootPath(String userId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path getProjectPath(String userId, String projectId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path getStudyPath(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public URI getStudyUri(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path getFilePath(String userId, String projectId, String studyId, String relativeFilePath, boolean check) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path getFilePath(String userId, String projectId, String studyId, String relativeFilePath) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path getTmpPath() {
        return null;
    }

    @Override
    public Path createUser(String userId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path deleteUser(String userId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path createAnonymousUser(String anonymousUserId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path deleteAnonymousUser(String anonymousUserId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path createProject(String userId, String projectId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path deleteProject(String userId, String projectId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public void renameProject(String userId, String oldProjectId, String newProjectId) throws CatalogIOManagerException {

    }

    @Override
    public boolean existProject(String userId, String projectId) throws CatalogIOManagerException {
        return false;
    }

    @Override
    public Path createStudy(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public Path deleteStudy(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public void renameStudy(String userId, String projectId, String oldStudyId, String newStudyId) throws CatalogIOManagerException {

    }

    @Override
    public Path createFolder(String userid, String projectId, String studyId, String fileId, boolean parent) throws CatalogIOManagerException {
        return null;
    }

    @Override
    public void createFile(String userId, String projectId, String studyId, String objectId, InputStream inputStream) throws CatalogIOManagerException {

    }

    @Override
    public void deleteFile(String userId, String projectId, String studyId, String objectId) throws CatalogIOManagerException {

    }

    @Override
    public DataInputStream getFileObject(String userid, String projectId, String studyId, String objectId, int start, int limit) throws CatalogIOManagerException, IOException {
        return null;
    }

    @Override
    public DataInputStream getGrepFileObject(String userId, String projectId, String studyId, String objectId, String pattern, boolean ignoreCase, boolean multi) throws CatalogIOManagerException, IOException {
        return null;
    }

    @Override
    public DataInputStream getFileFromJob(Path jobPath, String filename, String zip) throws CatalogIOManagerException, FileNotFoundException {
        return null;
    }

    @Override
    public DataInputStream getGrepFileFromJob(Path jobPath, String filename, String pattern, boolean ignoreCase, boolean multi) throws CatalogIOManagerException, IOException {
        return null;
    }

    @Override
    public InputStream getJobZipped(Path jobPath, String jobId) throws CatalogIOManagerException, IOException {
        return null;
    }
}
