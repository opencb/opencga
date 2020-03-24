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

import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class CatalogIOManager {

    /**
     * OpenCGA folders are created in the ROOTDIR.
     * OPENCGA_USERS_FOLDER contains users workspaces organized by 'userId'
     * OPENCGA_ANONYMOUS_USERS_FOLDER contains anonymous users workspaces organized by 'randomStringId'
     * OPENCGA_BIN_FOLDER contains all packaged binaries delivered within OpenCGA
     */
    private static final String OPENCGA_USERS_FOLDER = "users/";
    private static final String OPENCGA_ANONYMOUS_USERS_FOLDER = "anonymous/";
    private static final String OPENCGA_BIN_FOLDER = "bin/";

    /**
     * Users folders are created inside user workspace.
     * USER_PROJECTS_FOLDER this folder stores all the projects with the studies and files
     * USER_BIN_FOLDER contains user specific binaries
     */
    protected static final String USER_PROJECTS_FOLDER = "projects/";
    protected static final String USER_BIN_FOLDER = "bin/";
    protected static Logger logger = LoggerFactory.getLogger(CatalogIOManager.class);
    protected URI rootDir;
    private URI jobDir;
    private IOManager ioManager;

    public CatalogIOManager(Configuration configuration) throws CatalogIOException {
        try {
            this.rootDir = UriUtils.createDirectoryUri(configuration.getWorkspace());
            this.jobDir = UriUtils.createDirectoryUri(Paths.get(configuration.getJobDir()).resolve("JOBS/").toAbsolutePath().toString());
        } catch (URISyntaxException e) {
            throw new CatalogIOException(e.getMessage(), e);
        }
        this.ioManager = new PosixIOManager();
    }

    public void createDefaultOpenCGAFolders() throws CatalogIOException {
        if (!ioManager.exists(rootDir)) {
            logger.info("Creating main folder '" + rootDir + "'");
            ioManager.createDirectory(rootDir, true);
        }
        ioManager.checkDirectoryUri(rootDir, true);

        if (!ioManager.exists(getUsersUri())) {
            ioManager.createDirectory(getUsersUri());
        }

        if (!ioManager.exists(rootDir.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER))) {
            ioManager.createDirectory(rootDir.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER));
        }

        if (!ioManager.exists(rootDir.resolve(OPENCGA_BIN_FOLDER))) {
            ioManager.createDirectory(rootDir.resolve(OPENCGA_BIN_FOLDER));
        }

        if (!ioManager.exists(jobDir)) {
            ioManager.createDirectory(jobDir, true);
        }
    }

    protected void checkParam(String param) throws CatalogIOException {
        if (param == null || param.equals("")) {
            throw new CatalogIOException("Parameter '" + param + "' not valid");
        }
    }

    public URI getUsersUri() {
        return Paths.get(rootDir).resolve(OPENCGA_USERS_FOLDER).toUri();
    }

    public URI getUserUri(String userId) throws CatalogIOException {
        checkParam(userId);
        return Paths.get(getUsersUri()).resolve(userId.endsWith("/") ? userId : (userId + "/")).toUri();
    }

    public URI getProjectsUri(String userId) throws CatalogIOException {
        return Paths.get(getUserUri(userId)).resolve(USER_PROJECTS_FOLDER).toUri();
    }

    public URI getProjectUri(String userId, String projectId) throws CatalogIOException {
        return Paths.get(getProjectsUri(userId)).resolve(projectId.endsWith("/") ? projectId : (projectId + "/")).toUri();
    }

    private URI getStudyUri(String userId, String projectId, String studyId) throws CatalogIOException {
        checkParam(studyId);
        return Paths.get(getProjectUri(userId, projectId)).resolve(studyId.endsWith("/") ? studyId : (studyId + "/")).toUri();
    }

    public URI createUser(String userId) throws CatalogIOException {
        checkParam(userId);

        URI usersUri = getUsersUri();
        ioManager.checkDirectoryUri(usersUri, true);

        URI userPath = getUserUri(userId);
        try {
            if (!ioManager.exists(userPath)) {
                ioManager.createDirectory(userPath);
                ioManager.createDirectory(Paths.get(userPath).resolve(CatalogIOManager.USER_PROJECTS_FOLDER).toUri());
                ioManager.createDirectory(Paths.get(userPath).resolve(CatalogIOManager.USER_BIN_FOLDER).toUri());

                return userPath;
            }
        } catch (CatalogIOException e) {
            throw e;
        }
        return null;
    }

    public void deleteUser(String userId) throws CatalogIOException {
        URI userUri = getUserUri(userId);
        ioManager.checkUriExists(userUri);
        ioManager.deleteDirectory(userUri);
    }

    public URI createProject(String userId, String projectId) throws CatalogIOException {
        checkParam(projectId);

        URI projectUri = getProjectUri(userId, projectId);
        try {
            if (!ioManager.exists(projectUri)) {
                projectUri = ioManager.createDirectory(projectUri, true);
            }
        } catch (CatalogIOException e) {
            throw new CatalogIOException("createProject(): could not create the project folder", e);
        }

        return projectUri;
    }

    public URI createStudy(String userId, String projectId, String studyId) throws CatalogIOException {
        URI studyUri = getStudyUri(userId, projectId, studyId);
        ioManager.checkUriScheme(studyUri);
        try {
            if (!ioManager.exists(studyUri)) {
                studyUri = ioManager.createDirectory(studyUri);
            }
        } catch (CatalogIOException e) {
            throw new CatalogIOException("createStudy method: could not create the study folder: " + e.toString(), e);
        }

        return studyUri;
    }

    public URI getJobsUri() {
        return jobDir;
    }

}
