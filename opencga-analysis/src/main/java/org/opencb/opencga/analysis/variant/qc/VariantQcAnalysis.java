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

package org.opencb.opencga.analysis.variant.qc;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.WRITE_SAMPLES;

public class VariantQcAnalysis extends OpenCgaToolScopeStudy {

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        // Check permissions
        try {
            JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(study, jwtPayload);
            String organizationId = studyFqn.getOrganizationId();
            String userId = jwtPayload.getUserId(organizationId);

            long studyUid = catalogManager.getStudyManager().get(getStudy(), QueryOptions.empty(), token).first().getUid();
            catalogManager.getAuthorizationManager().checkStudyPermission(organizationId, studyUid, userId, WRITE_SAMPLES);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    @Override
    protected void run() throws Exception {
        // Nothing to do
    }

    protected static void checkStudy(String studyId, CatalogManager catalogManager, String token) throws ToolException {
        if (StringUtils.isEmpty(studyId)) {
            throw new ToolException("Missing study");
        }

        try {
            catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), token).first();
        } catch (CatalogException e) {
            throw new ToolException("Error accessing study ID '" + studyId + "'", e);
        }
    }

    protected static void checkPermissions(StudyPermissions.Permissions permissions, String studyId, CatalogManager catalogManager,
                                           String token) throws ToolException {
        checkStudy(studyId, catalogManager, token);

        try {
            JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, jwtPayload);
            String organizationId = studyFqn.getOrganizationId();
            String userId = jwtPayload.getUserId(organizationId);

            Study study = catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), token).first();
            catalogManager.getAuthorizationManager().checkStudyPermission(organizationId, study.getUid(), userId, permissions);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    protected static Path checkFileParameter(String fileId, String msg, String studyId, CatalogManager catalogManager, String token)
            throws ToolException {
        if (StringUtils.isEmpty(fileId)) {
            throw new ToolException(msg + " ID is empty");
        }
        File file;
        try {
            file = catalogManager.getFileManager().get(studyId, fileId, QueryOptions.empty(), token).first();
        } catch (CatalogException e) {
            throw new ToolExecutorException(msg + " ID '" + fileId + "' not found in OpenCGA catalog", e);
        }
        Path path = Paths.get(file.getUri());
        if (!Files.exists(path)) {
            throw new ToolExecutorException(msg + " '" + path + "' does not exist (file ID: " + fileId + ")");
        }
        return path;
    }

    protected Path getExternalFilePath(String analysisId, String resourceName) throws ToolException {
        URL url = null;
        try {
            url = new URL(ResourceUtils.URL + "analysis/" + analysisId + "/" + resourceName);
            ResourceUtils.downloadThirdParty(url, getOutDir());
        } catch (IOException e) {
            throw new ToolException("Something wrong happened downloading the resource '" + resourceName + "' from '" + url + "'", e);
        }

        if (!Files.exists(getOutDir().resolve(resourceName))) {
            throw new ToolException("After downloading the resource '" + resourceName + "', it does not exist at " + getOutDir());
        }
        return getOutDir().resolve(resourceName);
    }

    protected Path downloadExternalFileAtResources(String analysisId, String resourceName) throws ToolException {
        // Check if the resource has been downloaded previously
        Path resourcePath = getOpencgaHome().resolve("analysis/resources/" + analysisId);
        if (!Files.exists(resourcePath)) {
            // Create the resource path if it does not exist yet
            try {
                Files.createDirectories(resourcePath);
            } catch (IOException e) {
                throw new ToolException("It could not create the resource path '" + resourcePath + "'", e);
            }
        }
        if (!Files.exists(resourcePath.resolve(resourceName))) {
            // Otherwise, download it from the resource repository
            URL url = null;
            try {
                url = new URL(ResourceUtils.URL + "analysis/" + analysisId + "/" + resourceName);
                ResourceUtils.downloadThirdParty(url, resourcePath);
            } catch (IOException e) {
                throw new ToolException("Something wrong happened downloading the resource '" + resourceName + "' from '" + url + "'", e);
            }

            if (!Files.exists(resourcePath.resolve(resourceName))) {
                throw new ToolException("After downloading the resource '" + resourceName + "', it does not exist at " + resourcePath);
            }
        }
        return resourcePath.resolve(resourceName);
    }
}
