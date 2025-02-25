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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FamilyQcAnalysisParams extends ToolParams {

    @DataField(id = "families", description = FieldConstants.FAMILY_QC_FAMILY_ID_LIST_DESCRIPTION)
    private List<String> families;

    /**
     * @deprecated to be removed when latest changes take place
     */
    @Deprecated
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

    @DataField(id = "relatednessPruneInFreqsFile", description = FieldConstants.RELATEDNESS_PRUNE_IN_FREQS_FILE_DESCRIPTION)
    private String relatednessPruneInFreqsFile;

    @DataField(id = "relatednessPruneOutMarkersFile", description = FieldConstants.RELATEDNESS_PRUNE_OUT_MARKERS_FILE_DESCRIPTION)
    private String relatednessPruneOutMarkersFile;

    @DataField(id = "relatednessThresholdsFile", description = FieldConstants.RELATEDNESS_THRESHOLDS_FILE_DESCRIPTION)
    private String relatednessThresholdsFile;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public FamilyQcAnalysisParams() {
    }

    public FamilyQcAnalysisParams(List<String> families, Boolean skipIndex, Boolean overwrite, String relatednessPruneInFreqsFile,
                                  String relatednessPruneOutMarkersFile, String relatednessThresholdsFile, String outdir) {
        this.families = families;
        this.skipIndex = skipIndex;
        this.overwrite = overwrite;
        this.relatednessPruneInFreqsFile = relatednessPruneInFreqsFile;
        this.relatednessPruneOutMarkersFile = relatednessPruneOutMarkersFile;
        this.relatednessThresholdsFile = relatednessThresholdsFile;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyQcAnalysisParams{");
        sb.append("families=").append(families);
        sb.append(", family='").append(family).append('\'');
        sb.append(", relatednessMethod='").append(relatednessMethod).append('\'');
        sb.append(", relatednessMaf='").append(relatednessMaf).append('\'');
        sb.append(", skipIndex=").append(skipIndex);
        sb.append(", overwrite=").append(overwrite);
        sb.append(", relatednessPruneInFreqsFile='").append(relatednessPruneInFreqsFile).append('\'');
        sb.append(", relatednessPruneOutMarkersFile='").append(relatednessPruneOutMarkersFile).append('\'');
        sb.append(", relatednessThresholdsFile='").append(relatednessThresholdsFile).append('\'');
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

    @Deprecated
    public String getFamily() {
        return families.get(0);
    }

    @Deprecated
    public FamilyQcAnalysisParams setFamily(String family) {
        this.families = new ArrayList<>(Arrays.asList(family));
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

    public String getRelatednessPruneInFreqsFile() {
        return relatednessPruneInFreqsFile;
    }

    public FamilyQcAnalysisParams setRelatednessPruneInFreqsFile(String relatednessPruneInFreqsFile) {
        this.relatednessPruneInFreqsFile = relatednessPruneInFreqsFile;
        return this;
    }

    public String getRelatednessPruneOutMarkersFile() {
        return relatednessPruneOutMarkersFile;
    }

    public FamilyQcAnalysisParams setRelatednessPruneOutMarkersFile(String relatednessPruneOutMarkersFile) {
        this.relatednessPruneOutMarkersFile = relatednessPruneOutMarkersFile;
        return this;
    }

    public String getRelatednessThresholdsFile() {
        return relatednessThresholdsFile;
    }

    public FamilyQcAnalysisParams setRelatednessThresholdsFile(String relatednessThresholdsFile) {
        this.relatednessThresholdsFile = relatednessThresholdsFile;
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
