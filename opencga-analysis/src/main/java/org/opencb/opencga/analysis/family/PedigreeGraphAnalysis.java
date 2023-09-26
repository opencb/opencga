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

package org.opencb.opencga.analysis.family;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.PedigreeGraphAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.family.PedigreeGraphAnalysisExecutor;

@Tool(id = PedigreeGraphAnalysis.ID, resource = Enums.Resource.FAMILY)
public class PedigreeGraphAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "pedigree-graph";
    public static final String DESCRIPTION = "Compute the family pedigree graph image.";

    @ToolParams
    private PedigreeGraphAnalysisParams pedigreeParams = new PedigreeGraphAnalysisParams();

    private Family family;

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        if (StringUtils.isEmpty(pedigreeParams.getFamilyId())) {
            throw new ToolException("Missing family ID. It is mandatory to compute the pedigree graph image");
        }

        // Check family
        study = catalogManager.getStudyManager().get(organizationId, study, QueryOptions.empty(), token).first().getFqn();
        OpenCGAResult<Family> familyResult = catalogManager.getFamilyManager().get(study, pedigreeParams.getFamilyId(),
                QueryOptions.empty(), token);
        if (familyResult.getNumResults() != 1) {
            throw new ToolException("Unable to compute the pedigree graph imae. Family '" + pedigreeParams.getFamilyId() +  "' not found");
        }
        family = familyResult.first();

    }

    @Override
    protected void run() throws ToolException {
        step(getId(), () -> {
            PedigreeGraphAnalysisExecutor toolExecutor = getToolExecutor(PedigreeGraphAnalysisExecutor.class);

            toolExecutor.setStudy(study)
                    .setFamily(family)
                    .execute();
        });
    }
}

