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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class FamilyQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Family QC analysis params.";

    @DataField(id = "families", description = FieldConstants.FAMILY_QC_FAMILY_ID_LIST_DESCRIPTION)
    private List<String> families;

    @Deprecated
    @DataField(id = "family", description = FieldConstants.FAMILY_QC_FAMILY_ID_DESCRIPTION, deprecated = true)
    private String family;

    @Deprecated
    private String relatednessMethod;

    @Deprecated
    @DataField(id = "relatednessMaf", description = FieldConstants.FAMILY_QC_RELATEDNESS_MAF_DESCRIPTION, deprecated = true)
    private String relatednessMaf;

    @DataField(id = "relatednessParams", description = FieldConstants.QC_RELATEDNESS_DESCRIPTION)
    private QcRelatednessAnalysisParams relatednessParams;

    @DataField(id = "skipIndex", description = FieldConstants.QC_SKIP_INDEX_DESCRIPTION)
    private Boolean skipIndex;

    @DataField(id = "overwrite", description = FieldConstants.QC_OVERWRITE_DESCRIPTION)
    private Boolean overwrite;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public FamilyQcAnalysisParams() {
    }

    public FamilyQcAnalysisParams(List<String> families, String family, String relatednessMethod, String relatednessMaf,
                                  QcRelatednessAnalysisParams relatednessParams, Boolean skipIndex, Boolean overwrite, String outdir) {
        this.families = families;
        this.family = family;
        this.relatednessMethod = relatednessMethod;
        this.relatednessMaf = relatednessMaf;
        this.relatednessParams = relatednessParams;
        this.skipIndex = skipIndex;
        this.overwrite = overwrite;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyQcAnalysisParams{");
        sb.append("families=").append(families);
        sb.append(", family='").append(family).append('\'');
        sb.append(", relatednessMethod='").append(relatednessMethod).append('\'');
        sb.append(", relatednessMaf='").append(relatednessMaf).append('\'');
        sb.append(", relatednessParams=").append(relatednessParams);
        sb.append(", skipIndex=").append(skipIndex);
        sb.append(", overwrite=").append(overwrite);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public List<String> getFamilies() {
        return families;
    }

    public FamilyQcAnalysisParams setFamilies(List<String> families) {
        this.families = families;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public FamilyQcAnalysisParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public FamilyQcAnalysisParams setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    public String getRelatednessMaf() {
        return relatednessMaf;
    }

    public FamilyQcAnalysisParams setRelatednessMaf(String relatednessMaf) {
        this.relatednessMaf = relatednessMaf;
        return this;
    }

    public QcRelatednessAnalysisParams getRelatednessParams() {
        return relatednessParams;
    }

    public FamilyQcAnalysisParams setRelatednessParams(QcRelatednessAnalysisParams relatednessParams) {
        this.relatednessParams = relatednessParams;
        return this;
    }

    public Boolean getSkipIndex() {
        return skipIndex;
    }

    public FamilyQcAnalysisParams setSkipIndex(Boolean skipIndex) {
        this.skipIndex = skipIndex;
        return this;
    }

    public Boolean getOverwrite() {
        return overwrite;
    }

    public FamilyQcAnalysisParams setOverwrite(Boolean overwrite) {
        this.overwrite = overwrite;
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
