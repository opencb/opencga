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

package org.opencb.opencga.core.models.variant;

import org.opencb.biodata.models.clinical.qc.RelatednessReport;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.tools.ToolParams;

import static org.opencb.biodata.models.constants.FieldConstants.RELATEDNESS_REPORT_HAPLOID_CALL_MODE_DESCRIPTION;
import static org.opencb.biodata.models.constants.FieldConstants.RELATEDNESS_REPORT_MAF_DESCRIPTION;

public class FamilyQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Family QC analysis params. Family ID. Relatedness method based on the PLINK/IBD method."
        + " Minor allele frequency (MAF) is used to filter variants before computing relatedness, e.g.: " + ParamConstants.POP_FREQ_1000G
            + ":CEU>0.35 or cohort:ALL>0.05";

    @DataField(id = "family", description = FieldConstants.FAMILY_ID_DESCRIPTION)
    private String family;

    @DataField(id = "relatednessMaf", description = RELATEDNESS_REPORT_MAF_DESCRIPTION)
    private String relatednessMaf;

    @DataField(id = "haploidCallMode", description = RELATEDNESS_REPORT_HAPLOID_CALL_MODE_DESCRIPTION)
    private String haploidCallMode;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public FamilyQcAnalysisParams() {
    }

    public FamilyQcAnalysisParams(String family, String relatednessMaf, String haploidCallMode, String outdir) {
        this.family = family;
        this.relatednessMaf = relatednessMaf;
        this.haploidCallMode = haploidCallMode;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyQcAnalysisParams{");
        sb.append("family='").append(family).append('\'');
        sb.append(", relatednessMaf='").append(relatednessMaf).append('\'');
        sb.append(", haploidCallMode='").append(haploidCallMode).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFamily() {
        return family;
    }

    public FamilyQcAnalysisParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getRelatednessMaf() {
        return relatednessMaf;
    }

    public FamilyQcAnalysisParams setRelatednessMaf(String relatednessMaf) {
        this.relatednessMaf = relatednessMaf;
        return this;
    }

    public String getHaploidCallMode() {
        return haploidCallMode;
    }

    public FamilyQcAnalysisParams setHaploidCallMode(String haploidCallMode) {
        this.haploidCallMode = haploidCallMode;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public FamilyQcAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
