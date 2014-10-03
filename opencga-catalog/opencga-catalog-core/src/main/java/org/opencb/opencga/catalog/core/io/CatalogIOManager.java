package org.opencb.opencga.catalog.core.io;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;

public interface CatalogIOManager {

    public void setup() throws IOException;

    public Path getUserPath(String userId) throws CatalogIOManagerException;

    public Path getAnonmousUserPath(String userId) throws CatalogIOManagerException;

    public Path getProjectRootPath(String userId) throws CatalogIOManagerException;

    public Path getProjectPath(String userId, String projectId) throws CatalogIOManagerException;

    public Path getStudyPath(String userId, String projectId, String studyId) throws CatalogIOManagerException;

    public URI getStudyUri(String userId, String projectId, String studyId) throws CatalogIOManagerException;

    public Path getFilePath(String userId, String projectId, String studyId, String relativeFilePath, boolean check) 
            throws CatalogIOManagerException;

    public Path getFilePath(String userId, String projectId, String studyId, String relativeFilePath) throws CatalogIOManagerException;

    public Path getTmpPath();

    public Path createUser(String userId) throws CatalogIOManagerException;

    public Path deleteUser(String userId) throws CatalogIOManagerException;

    public Path createAnonymousUser(String anonymousUserId) throws CatalogIOManagerException;

    public Path deleteAnonymousUser(String anonymousUserId) throws CatalogIOManagerException;

    public Path createProject(String userId, String projectId) throws CatalogIOManagerException;

    public Path deleteProject(String userId, String projectId) throws CatalogIOManagerException;

    public void renameProject(String userId, String oldProjectId, String newProjectId) throws CatalogIOManagerException;

    public boolean existProject(String userId, String projectId) throws CatalogIOManagerException;

    public Path createStudy(String userId, String projectId, String studyId) throws CatalogIOManagerException;

    public Path deleteStudy(String userId, String projectId, String studyId) throws CatalogIOManagerException;

    public void renameStudy(String userId, String projectId, String oldStudyId, String newStudyId) 
            throws CatalogIOManagerException;

    public Path createFolder(String userid, String projectId, String studyId, String fileId, boolean parent) 
            throws CatalogIOManagerException;

    public void createFile(String userId, String projectId, String studyId, String objectId, InputStream inputStream) 
            throws CatalogIOManagerException;

    public void deleteFile(String userId, String projectId, String studyId, String objectId) throws CatalogIOManagerException;

    public DataInputStream getFileObject(String userid, String projectId, String studyId, String objectId,int start, int limit)
            throws CatalogIOManagerException, IOException;
    public DataInputStream getGrepFileObject(String userId, String projectId, String studyId, String objectId, 
                                             String pattern, boolean ignoreCase, boolean multi) 
            throws CatalogIOManagerException, IOException;

    public DataInputStream getFileFromJob(Path jobPath, String filename, String zip) 
            throws CatalogIOManagerException,FileNotFoundException;

    public DataInputStream getGrepFileFromJob(Path jobPath, String filename, String pattern, boolean ignoreCase, 
                                              boolean multi) throws CatalogIOManagerException,IOException;

    public InputStream getJobZipped(Path jobPath, String jobId) throws CatalogIOManagerException, IOException;

}
