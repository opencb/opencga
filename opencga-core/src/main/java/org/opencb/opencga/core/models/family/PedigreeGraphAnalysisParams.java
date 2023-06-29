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

package org.opencb.opencga.core.models.family;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class PedigreeGraphAnalysisParams extends ToolParams {

    public static final String DESCRIPTION = "Family analysis to compute the pedigree graph image.";

    @DataField(id = "familyId", description = FieldConstants.FAMILY_ID_DESCRIPTION)
    private String familyId;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public PedigreeGraphAnalysisParams() {
    }

    public PedigreeGraphAnalysisParams(String familyId, String outdir) {
        this.familyId = familyId;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PedigreeGraphAnalysisParams{");
        sb.append("familyId='").append(familyId).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFamilyId() {
        return familyId;
    }

    public PedigreeGraphAnalysisParams setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public PedigreeGraphAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}

