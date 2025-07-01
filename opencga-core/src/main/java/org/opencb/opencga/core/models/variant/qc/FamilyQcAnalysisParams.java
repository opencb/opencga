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

package org.opencb.opencga.core.models.variant.qc;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class FamilyQcAnalysisParams extends ToolParams {

    @DataField(id = "family", description = FieldConstants.FAMILY_QC_FAMILY_ID_DESCRIPTION, deprecated = true)
    private String family;

    /**
     * @deprecated to be removed when latest changes take place
     */
    @Deprecated
    @DataField(id = "relatednessMethod", description = FieldConstants.RELATEDNESS_METHOD_DESCRIPTION, deprecated = true)
    private String relatednessMethod;

    /**
     * @deprecated to be removed when latest changes take place
     */
    @Deprecated
    @DataField(id = "relatednessMaf", description = FieldConstants.RELATEDNESS_MAF_DESCRIPTION, deprecated = true)
    private String relatednessMaf;

    @DataField(id = "skipIndex", description = FieldConstants.QC_SKIP_INDEX_DESCRIPTION)
    private Boolean skipIndex;

    @DataField(id = "overwrite", description = FieldConstants.QC_OVERWRITE_DESCRIPTION)
    private Boolean overwrite;

    @DataField(id = "resourcesDir", description = FieldConstants.RESOURCES_DIR_DESCRIPTION)
    private String resourcesDir;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public FamilyQcAnalysisParams() {
    }

    public FamilyQcAnalysisParams(String family, Boolean skipIndex, Boolean overwrite, String resourcesDir, String outdir) {
        this.family = family;
        this.skipIndex = skipIndex;
        this.overwrite = overwrite;
        this.resourcesDir = resourcesDir;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyQcAnalysisParams{");
        sb.append("family='").append(family).append('\'');
        sb.append(", skipIndex=").append(skipIndex);
        sb.append(", overwrite=").append(overwrite);
        sb.append(", resourcesDir='").append(resourcesDir).append('\'');
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

    @Deprecated
    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    @Deprecated
    public FamilyQcAnalysisParams setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    @Deprecated
    public String getRelatednessMaf() {
        return relatednessMaf;
    }

    @Deprecated
    public FamilyQcAnalysisParams setRelatednessMaf(String relatednessMaf) {
        this.relatednessMaf = relatednessMaf;
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

    public String getResourcesDir() {
        return resourcesDir;
    }

    public FamilyQcAnalysisParams setResourcesDir(String resourcesDir) {
        this.resourcesDir = resourcesDir;
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
