/*
 * Copyright 2015-2017 OpenCB
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

import org.opencb.opencga.core.config.Configuration;
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
    protected URI rootDir;
    protected URI jobsDir;
    protected URI tmp;
    @Deprecated
    protected Properties properties;
    protected Configuration configuration;

    private CatalogIOManager() {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public CatalogIOManager(String catalogConfigurationFile) throws CatalogIOException {
        this();
        this.configuration = new Configuration();

        try {
            this.configuration.load(new FileInputStream(catalogConfigurationFile));
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

    public CatalogIOManager(Configuration configuration) throws CatalogIOException {
        this();
        this.configuration = configuration;
        setup();
    }

    protected abstract void setConfiguration(Configuration configuration) throws CatalogIOException;

    /**
     * This method creates the folders and workspace structure for storing the OpenCGA data. I
     *
     * @throws CatalogIOException CatalogIOException
     */
    public void setup() throws CatalogIOException {
        setConfiguration(configuration);
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

    public abstract void checkDirectoryUri(URI uri, boolean writable) throws CatalogIOException;

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

    public URI createProject(String userId, String projectId) throws CatalogIOException {
        checkParam(projectId);

        URI projectUri = getProjectUri(userId, projectId);
        try {
            if (!exists(projectUri)) {
                projectUri = createDirectory(projectUri, true);
            }
        } catch (CatalogIOException e) {
            throw new CatalogIOException("createProject(): could not create the project folder", e);
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
            throw new CatalogIOException("createFolder(): could not create the directory", e);
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

    public DataInputStream getFileObject(URI fileUri)
            throws CatalogIOException {
        return getFileObject(fileUri, -1, -1);
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

    public abstract DataOutputStream createOutputStream(URI fileUri, boolean overwrite) throws CatalogIOException;

    public abstract String calculateChecksum(URI file) throws CatalogIOException;

    public abstract List<URI> listFiles(URI directory) throws CatalogIOException;

    public Stream<URI> listFilesStream(URI directory) throws CatalogIOException {
        return listFiles(directory).stream();
    }

    public abstract long getFileSize(URI file) throws CatalogIOException;

    public abstract Date getCreationDate(URI file) throws CatalogIOException;

    public abstract Date getModificationDate(URI file) throws CatalogIOException;

}
