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

package org.opencb.opencga.analysis.family.qc;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyQualityControl;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.FamilyQcAnalysisExecutor;

import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.core.models.study.StudyAclEntry.StudyPermissions.WRITE_FAMILIES;
import static org.opencb.opencga.core.models.study.StudyAclEntry.StudyPermissions.WRITE_SAMPLES;

@Tool(id = FamilyQcAnalysis.ID, resource = Enums.Resource.FAMILY, description = FamilyQcAnalysis.DESCRIPTION)
public class FamilyQcAnalysis extends OpenCgaTool {

    public static final String ID = "family-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given family. It computes the relatedness scores among the"
    + " family members";

    public  static final String RELATEDNESS_STEP = "relatedness";

    private String studyId;
    private String familyId;
    private String relatednessMethod;
    private String relatednessMaf;

    private Family family;

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(studyId);

        if (StringUtils.isEmpty(studyId)) {
            throw new ToolException("Missing study ID.");
        }

        // Check permissions
        try {
            Study study = catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), token).first();
            String userId = catalogManager.getUserManager().getUserId(token);
            catalogManager.getAuthorizationManager().checkStudyPermission(study.getUid(), userId, WRITE_FAMILIES);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Sanity check
        if (StringUtils.isEmpty(familyId)) {
            throw new ToolException("Missing family ID.");
        }

        family = IndividualQcUtils.getFamilyById(studyId, familyId, catalogManager, token);
        if (family == null) {
            throw new ToolException("Family '" + familyId + "' not found.");
        }

        // As relatedness is the only QC to compute, it is mandatory
        if (StringUtils.isEmpty(relatednessMethod)) {
            relatednessMethod = "PLINK/IBD";
        }
        if (StringUtils.isEmpty(relatednessMaf)) {
            relatednessMaf = "cohort:ALL>0.05";
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(RELATEDNESS_STEP);
    }

    @Override
    protected void run() throws ToolException {

        // Get family quality control metrics to update
        FamilyQualityControl qualityControl = family.getQualityControl();
        if (qualityControl == null) {
            qualityControl = new FamilyQualityControl();
        }

        FamilyQcAnalysisExecutor executor = getToolExecutor(FamilyQcAnalysisExecutor.class);

        // Set up executor
        executor.setStudyId(studyId)
                .setFamily(family)
                .setRelatednessMethod(relatednessMethod)
                .setRelatednessMaf(relatednessMaf)
                .setQualityControl(qualityControl);

        // Step by step
        step(RELATEDNESS_STEP, () -> executor.setQcType(FamilyQcAnalysisExecutor.QcType.RELATEDNESS).execute());

        // Finally, update family quality control
        try {
            qualityControl = executor.getQualityControl();
            if (qualityControl != null) {
                catalogManager.getFamilyManager().update(getStudyId(), familyId, new FamilyUpdateParams().setQualityControl(qualityControl),
                        QueryOptions.empty(), token);
            }
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    public String getStudyId() {
        return studyId;
    }

    public FamilyQcAnalysis setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public FamilyQcAnalysis setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public FamilyQcAnalysis setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    public String getRelatednessMaf() {
        return relatednessMaf;
    }

    public FamilyQcAnalysis setRelatednessMaf(String relatednessMaf) {
        this.relatednessMaf = relatednessMaf;
        return this;
    }

    public Family getFamily() {
        return family;
    }

    public FamilyQcAnalysis setFamily(Family family) {
        this.family = family;
        return this;
    }
}
