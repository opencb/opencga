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

import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

public abstract class CatalogIOManager {

    /**
     * OpenCGA folders are created in the ROOTDIR.
     * OPENCGA_USERS_FOLDER contains users workspaces organized by 'userId'
     * OPENCGA_ANONYMOUS_USERS_FOLDER contains anonymous users workspaces organized by 'randomStringId'
     * OPENCGA_BIN_FOLDER contains all packaged binaries delivered within OpenCGA
     */
    protected static final String OPENCGA_USERS_FOLDER = "users/";
    protected static final String OPENCGA_ANONYMOUS_USERS_FOLDER = "anonymous/";
    protected static final String OPENCGA_BIN_FOLDER = "bin/";
    /**
     * Users folders are created inside user workspace.
     * USER_PROJECTS_FOLDER this folder stores all the projects with the studies and files
     * USER_BIN_FOLDER contains user specific binaries
     */
    protected static final String USER_PROJECTS_FOLDER = "projects/";
    protected static final String USER_BIN_FOLDER = "bin/";
    protected static final String SHARED_DATA_FOLDER = "shared_data/";
    protected static final String DEFAULT_OPENCGA_JOBS_FOLDER = "jobs/";
    protected static Logger logger;
    //    private Path opencgaRootDirPath;
    protected URI rootDir;
    protected URI jobsDir;
    //    protected URI rootDir;
    protected URI tmp;
    @Deprecated
    protected Properties properties;
    protected CatalogConfiguration catalogConfiguration;

    private CatalogIOManager() {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public CatalogIOManager(String catalogConfigurationFile) throws CatalogIOException {
        this();
        this.catalogConfiguration = new CatalogConfiguration();
        //this.properties = new Properties();
        try {
            this.catalogConfiguration.load(new FileInputStream(catalogConfigurationFile));
            //this.properties.load(new FileInputStream(propertiesFile));
        } catch (IOException e) {
            throw new CatalogIOException("Error loading Catalog Configuration file", e);
        }
        setup();
    }

    @Deprecated
    public CatalogIOManager(Properties properties) throws CatalogIOException {
        this();
        this.properties = properties;
        setup();
    }

    public CatalogIOManager(CatalogConfiguration catalogConfiguration) throws CatalogIOException {
        this();
        this.catalogConfiguration = catalogConfiguration;
        setup();
    }

    protected abstract void setConfiguration(CatalogConfiguration catalogConfiguration) throws CatalogIOException;

    /**
     * This method creates the folders and workspace structure for storing the OpenCGA data. I
     *
     * @throws CatalogIOException CatalogIOException
     */
    public void setup() throws CatalogIOException {
        setConfiguration(catalogConfiguration);
        if (!exists(rootDir)) {
            logger.info("Initializing CatalogIOManager. Creating main folder '" + rootDir + "'");
            createDirectory(rootDir, true);
        }
        checkDirectoryUri(rootDir, true);

        if (!exists(jobsDir)) {
            logger.info("Initializing CatalogIOManager. Creating jobs folder '" + jobsDir + "'");
            createDirectory(jobsDir);
        }
        checkDirectoryUri(jobsDir, true);

        if (!exists(rootDir.resolve(OPENCGA_USERS_FOLDER))) {
            createDirectory(rootDir.resolve(OPENCGA_USERS_FOLDER));
        }

        if (!exists(rootDir.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER))) {
            createDirectory(rootDir.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER));
        }

        if (!exists(rootDir.resolve(OPENCGA_BIN_FOLDER))) {
            createDirectory(rootDir.resolve(OPENCGA_BIN_FOLDER));
        }

    }

    protected void checkParam(String param) throws CatalogIOException {
        if (param == null || param.equals("")) {
            throw new CatalogIOException("Parameter '" + param + "' not valid");
        }
    }

    protected abstract void checkUriExists(URI uri) throws CatalogIOException;

    protected abstract void checkUriScheme(URI uri) throws CatalogIOException;

    protected abstract void checkDirectoryUri(URI uri, boolean writable) throws CatalogIOException;

    public abstract boolean exists(URI uri);

    public abstract URI createDirectory(URI uri, boolean parents) throws CatalogIOException;

    public URI createDirectory(URI uri) throws CatalogIOException {
        return createDirectory(uri, false);
    }

    public abstract void deleteDirectory(URI uri) throws CatalogIOException;

    public abstract void deleteFile(URI fileUri) throws CatalogIOException;

    public abstract void rename(URI oldName, URI newName) throws CatalogIOException;

    public abstract boolean isDirectory(URI uri);

    public abstract void copyFile(URI source, URI target) throws IOException, CatalogIOException;

    public abstract void moveFile(URI source, URI target) throws IOException, CatalogIOException;


    public URI getUsersUri() throws CatalogIOException {
        return rootDir.resolve(OPENCGA_USERS_FOLDER);
    }

    public URI getAnonymousUsersUri() throws CatalogIOException {
        return rootDir.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER);
    }

    public URI getUserUri(String userId) throws CatalogIOException {
        checkParam(userId);
        try {
            return getUsersUri().resolve(new URI(null, userId.endsWith("/") ? userId : (userId + "/"), null));
        } catch (URISyntaxException e) {
            throw CatalogIOException.uriSyntaxException(userId, e);
        }
    }

    public URI getAnonymousUserUri(String userId) throws CatalogIOException { // FIXME: Should replicate to getAnonymousPojectUri,
        // ...Study..., etc ?
        checkParam(userId);
        try {
            return getAnonymousUsersUri().resolve(new URI(null, userId.endsWith("/") ? userId : (userId + "/"), null));
        } catch (URISyntaxException e) {
            throw CatalogIOException.uriSyntaxException(userId, e);
        }
    }

    public URI getProjectsUri(String userId) throws CatalogIOException {
        return getUserUri(userId).resolve(USER_PROJECTS_FOLDER);
    }

    public URI getProjectUri(String userId, String projectId) throws CatalogIOException {
        try {
            return getProjectsUri(userId).resolve(new URI(null, projectId.endsWith("/") ? projectId : (projectId + "/"), null));
        } catch (URISyntaxException e) {
            throw CatalogIOException.uriSyntaxException(userId, e);
        }
    }

    @Deprecated
    public URI getStudyUri(String userId, String projectId, String studyId) throws CatalogIOException {
        checkParam(studyId);
        try {
            return getProjectUri(userId, projectId).resolve(new URI(null, studyId.endsWith("/") ? studyId : (studyId + "/"), null));
        } catch (URISyntaxException e) {
            throw CatalogIOException.uriSyntaxException(studyId, e);
        }
    }

    public URI getFileUri(URI studyUri, String relativeFilePath)
            throws CatalogIOException {
        checkUriExists(studyUri);
        checkParam(relativeFilePath);
        try {
            return studyUri.resolve(new URI(null, relativeFilePath, null));
        } catch (URISyntaxException e) {
            throw CatalogIOException.uriSyntaxException(relativeFilePath, e);
        }
    }

    public URI getJobsUri(String userId) throws CatalogIOException {
        checkParam(userId);
        return jobsDir;
    }

    public abstract URI getTmpUri();    // FIXME Still used?


    public URI createUser(String userId) throws CatalogIOException {
        checkParam(userId);

        //    //just to show the previous version
        //        Path usersPath = Paths.get(opencgaRootDir, CatalogIOManager.OPENCGA_USERS_FOLDER);
        //        checkDirectoryUri(usersPath, true);
        //
        //        Path userPath = usersPath.resolve(userId);

        //        try {
        //            if(!Files.exists(userPath)) {
        //                Files.createDirectory(userPath);
        //                Files.createDirectory(Paths.get(userPath.toString(), CatalogIOManager.USER_PROJECTS_FOLDER));
        //                Files.createDirectory(Paths.get(userPath.toString(), CatalogIOManager.USER_BIN_FOLDER));
        //
        //                return userPath;
        //            }
        //        } catch (IOException e) {
        //            throw new CatalogIOManagerException("IOException" + e.toString());
        //        }
        //        return null;

//        URI opencgaPath = new URI(opencgaRootDir);
        URI usersUri = getUsersUri();
        checkDirectoryUri(usersUri, true);

        URI userPath = getUserUri(userId);
        try {
            if (!exists(userPath)) {
                createDirectory(userPath);
                createDirectory(userPath.resolve(CatalogIOManager.USER_PROJECTS_FOLDER));
                createDirectory(userPath.resolve(CatalogIOManager.USER_BIN_FOLDER));

                return userPath;
            }
        } catch (CatalogIOException e) {
            throw e;
        }
        return null;
    }

    public void deleteUser(String userId) throws CatalogIOException {
        URI userUri = getUserUri(userId);
        checkUriExists(userUri);
        deleteDirectory(userUri);
    }

    public URI createAnonymousUser(String anonymousUserId) throws CatalogIOException {
        checkParam(anonymousUserId);

        URI usersUri = getAnonymousUsersUri();
        checkDirectoryUri(usersUri, true);

        URI userUri = getAnonymousUserUri(anonymousUserId);
        try {
            if (!exists(userUri)) {
                createDirectory(userUri);
                createDirectory(userUri.resolve(USER_PROJECTS_FOLDER));
                createDirectory(userUri.resolve(USER_BIN_FOLDER));

                return userUri;
            }
        } catch (CatalogIOException e) {
            throw e;
        }
        return null;
    }

    public void deleteAnonymousUser(String anonymousUserId) throws CatalogIOException {
        URI anonymousUserUri = getAnonymousUserUri(anonymousUserId);
        checkUriExists(anonymousUserUri);

        deleteDirectory(anonymousUserUri);
//        return anonymousUserPath;
    }

    public URI createProject(String userId, String projectId) throws CatalogIOException {
        checkParam(projectId);

//        URI projectRootUri = getProjectsUri(userId);
//        checkDirectoryUri(projectRootUri, true);  //assuming catalogManager has checked it
//        URI projectUri = projectRootUri.resolve(projectId);
        URI projectUri = getProjectUri(userId, projectId);
        try {
            if (!exists(projectUri)) {
                projectUri = createDirectory(projectUri, true);
                //createDirectory(projectUri.resolve(SHARED_DATA_FOLDER));
            }
        } catch (CatalogIOException e) {
            throw new CatalogIOException("createProject(): could not create the project folder: " + e.toString());
        }

        return projectUri;
    }

    public void deleteProject(String userId, String projectId) throws CatalogIOException {
        URI projectUri = getProjectUri(userId, projectId);
        checkUriExists(projectUri);

        deleteDirectory(projectUri);
    }

    public void renameProject(String userId, String oldProjectId, String newProjectId)
            throws CatalogIOException {
        URI oldFolder = getProjectUri(userId, oldProjectId);
        URI newFolder = getProjectUri(userId, newProjectId);

        rename(oldFolder, newFolder);
    }

    public boolean existProject(String userId, String projectId) throws CatalogIOException {
        return exists(getProjectUri(userId, projectId));
    }

    public URI createStudy(String userId, String projectId, String studyId) throws CatalogIOException {
        checkParam(studyId);

        URI projectUri = getProjectUri(userId, projectId);
        checkDirectoryUri(projectUri, true);

        URI studyUri = getStudyUri(userId, projectId, studyId);

        return createStudy(studyUri);
    }

    public URI createStudy(URI studyUri) throws CatalogIOException {
        checkUriScheme(studyUri);
        try {
            if (!exists(studyUri)) {
                studyUri = createDirectory(studyUri);
            }
        } catch (CatalogIOException e) {
            throw new CatalogIOException("createStudy method: could not create the study folder: " + e.toString(), e);
        }

        return studyUri;
    }

    public void deleteStudy(URI studyUri) throws CatalogIOException {
        checkUriScheme(studyUri);
        checkUriExists(studyUri);

        deleteDirectory(studyUri);
    }

//    public void renameStudy(String userId, String projectId, String oldStudyId, String newStudyId)
//            throws CatalogIOManagerException {
//        URI oldFolder = getStudyUri(userId, projectId, oldStudyId);
//        URI newFolder = getStudyUri(userId, projectId, newStudyId);
//
//        try {
//            rename(oldFolder, newFolder);
//        } catch (IOException e) {
//            throw new CatalogIOManagerException("renameStudy(): could not rename the study folder: " + e.toString());
//        }
//    }

    public URI createJobOutDir(String userId, String folderName)
            throws CatalogIOException {
        checkParam(folderName);

        URI jobsFolderUri = getJobsUri(userId);
        checkDirectoryUri(jobsFolderUri, true);

        URI jobUri;
        try {
            jobUri = jobsFolderUri.resolve(new URI(null, folderName, null));
        } catch (URISyntaxException e) {
            throw CatalogIOException.uriSyntaxException(folderName, e);
        }
        if (!exists(jobUri)) {
            try {
                jobUri = createDirectory(jobUri, true);
            } catch (CatalogIOException e) {
                e.printStackTrace();
                throw new CatalogIOException("createStudy method: could not create the study folder: " + e.toString(), e);
            }
        } else {
            throw new CatalogIOException("createJobOutDir method: Job folder " + folderName + "already exists.");
        }
        return jobUri;
    }

    public URI createFolder(URI studyUri, String folderName, boolean parent)
            throws CatalogIOException {
        checkParam(folderName);
        if (!folderName.endsWith("/")) {
            folderName += "/";
        }
        checkDirectoryUri(studyUri, true);

//        Path fullFolderPath = getFileUri(userid, projectId, studyId, objectId);
        URI folderUri = null;
        try {
            folderUri = studyUri.resolve(new URI(null, folderName, null));
        } catch (URISyntaxException e) {
            throw CatalogIOException.uriSyntaxException(folderName, e);
        }
        try {
            if (!exists(folderUri)) {
                if (parent) {
                    createDirectory(folderUri, true);
                } else {
                    checkDirectoryUri(folderUri.resolve(".."), true);
                    createDirectory(folderUri);
                }
            }
        } catch (CatalogIOException e) {
            throw new CatalogIOException("createFolder(): could not create the directory " + e.toString());
        }

        return folderUri;
    }

    public void createFile(URI studyUri, String filePath, InputStream inputStream)
            throws CatalogIOException {
        URI fileUri = getFileUri(studyUri, filePath);
        createFile(fileUri, inputStream);
    }

    public abstract void createFile(URI fileUri, InputStream inputStream)
            throws CatalogIOException;

    public void deleteFile(URI studyUri, String filePath)
            throws CatalogIOException {
        URI fileUri = getFileUri(studyUri, filePath);
        checkUriExists(fileUri);

        logger.debug("Deleting {}", fileUri.toString());
        if (isDirectory(fileUri)) {
            deleteDirectory(fileUri);
        } else {
            deleteFile(fileUri);
        }
    }


    public DataInputStream getFileObject(URI studyUri, String objectId, int start, int limit)
            throws CatalogIOException {
        URI fileUri = getFileUri(studyUri, objectId);
        return getFileObject(fileUri, start, limit);
    }

    public abstract DataInputStream getFileObject(URI fileUri, int start, int limit)
            throws CatalogIOException;

    public DataInputStream getGrepFileObject(URI studyUri, String objectId, String pattern,
                                             boolean ignoreCase, boolean multi)
            throws CatalogIOException {
        URI fileUri = getFileUri(studyUri, objectId);
        return getGrepFileObject(fileUri, pattern, ignoreCase, multi);
    }

    public abstract DataInputStream getGrepFileObject(URI fileUri, String pattern, boolean ignoreCase, boolean multi)
            throws CatalogIOException;
//
//    public abstract DataInputStream getFileFromJob(Path jobPath, String filename, String zip)
//            throws CatalogIOManagerException,FileNotFoundException;
//
//    public abstract DataInputStream getGrepFileFromJob(Path jobPath, String filename, String pattern, boolean ignoreCase,
//                                                       boolean multi) throws CatalogIOManagerException,IOException;
//
//    public abstract InputStream getJobZipped(Path jobPath, String jobId) throws CatalogIOManagerException, IOException;

    public abstract DataOutputStream createOutputStream(URI fileUri, boolean overwrite) throws CatalogIOException;

    public abstract String calculateChecksum(URI file) throws CatalogIOException;

    public abstract List<URI> listFiles(URI directory) throws CatalogIOException;

    public Stream<URI> listFilesStream(URI directory) throws CatalogIOException {
        return listFiles(directory).stream();
    }

    public abstract long getFileSize(URI file) throws CatalogIOException;

    public abstract Date getCreationDate(URI file) throws CatalogIOException;

    public abstract Date getModificationDate(URI file) throws CatalogIOException;

//    public abstract String getCreationDate(URI file);
}
