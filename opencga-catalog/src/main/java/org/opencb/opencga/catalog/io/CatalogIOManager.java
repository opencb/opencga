/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CatalogIOManager {

    /*
     * OpenCGA folders are created in the ROOTDIR.
     * OPENCGA_ORGANIZATIONS_FOLDER contains organization workspaces organized by 'organizationId'
     * OPENCGA_ANONYMOUS_USERS_FOLDER contains anonymous users workspaces organized by 'randomStringId'
     * OPENCGA_BIN_FOLDER contains all packaged binaries delivered within OpenCGA
     */
    private static final String OPENCGA_ORGANIZATIONS_FOLDER = "orgs/";
    private static final String OPENCGA_ANONYMOUS_USERS_FOLDER = "anonymous/";
    private static final String OPENCGA_BIN_FOLDER = "bin/";

    /*
     * ORGANIZATION_PROJECTS_FOLDER this folder stores all the projects with the studies and files.
     * ORGANIZATION_BIN_FOLDER contains user specific binaries
     */
    protected static final String ORGANIZATION_PROJECTS_FOLDER = "projects/";
    protected static final String ORGANIZATION_BIN_FOLDER = "bin/";
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

        if (!ioManager.exists(getOrganizationsUri())) {
            ioManager.createDirectory(getOrganizationsUri());
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
        if (StringUtils.isEmpty(param)) {
            throw new CatalogIOException("Parameter '" + param + "' not valid");
        }
    }

    public URI getOrganizationsUri() {
        return Paths.get(rootDir).resolve(OPENCGA_ORGANIZATIONS_FOLDER).toUri();
    }

    public URI getOrganizationsUri(String organizationId) throws CatalogIOException {
        checkParam(organizationId);
        return Paths.get(getOrganizationsUri()).resolve(organizationId.endsWith("/") ? organizationId : (organizationId + "/")).toUri();
    }

    public URI getProjectsUri(String organizationId) throws CatalogIOException {
        return Paths.get(getOrganizationsUri(organizationId)).resolve(ORGANIZATION_PROJECTS_FOLDER).toUri();
    }

    public URI getProjectUri(String organizationId, String projectId) throws CatalogIOException {
        return Paths.get(getProjectsUri(organizationId)).resolve(projectId.endsWith("/") ? projectId : (projectId + "/")).toUri();
    }

    public URI getStudyUri(String organizationId, String projectId, String studyId) throws CatalogIOException {
        checkParam(studyId);
        return Paths.get(getProjectUri(organizationId, projectId)).resolve(studyId.endsWith("/") ? studyId : (studyId + "/")).toUri();
    }

    public URI createOrganization(String organizationId) throws CatalogIOException {
        checkParam(organizationId);

        URI organizationsUri = getOrganizationsUri();
        ioManager.checkDirectoryUri(organizationsUri, true);

        URI organizationUri = getOrganizationsUri(organizationId);
        Path organizationPath = Paths.get(organizationUri);
        try {
            if (!ioManager.exists(organizationUri)) {
                ioManager.createDirectory(organizationUri);
                ioManager.createDirectory(organizationPath.resolve(CatalogIOManager.ORGANIZATION_PROJECTS_FOLDER).toUri());
                ioManager.createDirectory(organizationPath.resolve(CatalogIOManager.ORGANIZATION_BIN_FOLDER).toUri());

                return organizationUri;
            }
        } catch (CatalogIOException e) {
            throw e;
        }
        return null;
    }

    public void deleteOrganization(String organization) throws CatalogIOException {
        URI organizationsUri = getOrganizationsUri(organization);
        ioManager.checkUriExists(organizationsUri);
        ioManager.deleteDirectory(organizationsUri);
    }

    public URI createProject(String organizationId, String projectId) throws CatalogIOException {
        checkParam(projectId);

        URI projectUri = getProjectUri(organizationId, projectId);
        try {
            if (!ioManager.exists(projectUri)) {
                projectUri = ioManager.createDirectory(projectUri, true);
            }
        } catch (CatalogIOException e) {
            throw new CatalogIOException("createProject(): could not create the project folder", e);
        }

        return projectUri;
    }

    public URI createStudy(String organizationId, String projectId, String studyId) throws CatalogIOException {
        URI studyUri = getStudyUri(organizationId, projectId, studyId);
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
