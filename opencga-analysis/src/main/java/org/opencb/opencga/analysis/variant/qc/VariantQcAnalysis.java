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
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.tools.ToolParams;

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
    }

    public static void checkParameters(ToolParams params, String study, CatalogManager catalogManager, String token) throws Exception {
        if (StringUtils.isEmpty(study)) {
            throw new ToolException("Missing study");
        }
    }

    protected static void checkPermissions(StudyPermissions.Permissions permissions, String studyId, CatalogManager catalogManager,
                                           String token) throws ToolException {
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
}
