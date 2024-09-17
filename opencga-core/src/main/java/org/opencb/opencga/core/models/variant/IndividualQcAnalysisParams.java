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

public class IndividualQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Individual QC analysis params";

    @DataField(id = "individuals", description = FieldConstants.INDIVIDUAL_QC_INDIVIDUAL_ID_LIST_DESCRIPTION, deprecated = true)
    private List<String> individuals;

    @DataField(id = "individual", description = FieldConstants.INDIVIDUAL_QC_INDIVIDUAL_ID_DESCRIPTION, deprecated = true)
    private String individual;

    @DataField(id = "sample", description = FieldConstants.INDIVIDUAL_QC_SAMPLE_ID_DESCRIPTION, deprecated = true)
    private String sample;

    /**
     * @deprecated to be removed after latest changes take place
     */
    @Deprecated
    @DataField(id = "inferredSexMethod", description = FieldConstants.INFERRED_SEX_METHOD_DESCRIPTION, deprecated = true)
    private String inferredSexMethod;

    @DataField(id = "resourcesDir", description = FieldConstants.QC_RESOURCES_DIR_DESCRIPTION)
    private String resourcesDir;

    @DataField(id = "skip", description = FieldConstants.INDIVIDUAL_QC_SKIP_DESCRIPTION)
    private List<String> skip;

    @DataField(id = "skipIndex", description = FieldConstants.QC_SKIP_INDEX_DESCRIPTION)
    private Boolean skipIndex;

    @DataField(id = "overwrite", description = FieldConstants.QC_OVERWRITE_DESCRIPTION)
    private Boolean overwrite;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public IndividualQcAnalysisParams() {
    }

    public IndividualQcAnalysisParams(List<String> individuals, String individual, String sample, String inferredSexMethod,
                                      String resourcesDir, List<String> skip, Boolean skipIndex, Boolean overwrite, String outdir) {
        this.individuals = individuals;
        this.individual = individual;
        this.sample = sample;
        this.inferredSexMethod = inferredSexMethod;
        this.resourcesDir = resourcesDir;
        this.skip = skip;
        this.skipIndex = skipIndex;
        this.overwrite = overwrite;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQcAnalysisParams{");
        sb.append("individuals=").append(individuals);
        sb.append(", individual='").append(individual).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append(", inferredSexMethod='").append(inferredSexMethod).append('\'');
        sb.append(", resourcesDir='").append(resourcesDir).append('\'');
        sb.append(", skip=").append(skip);
        sb.append(", skipIndex=").append(skipIndex);
        sb.append(", overwrite=").append(overwrite);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public List<String> getIndividuals() {
        return individuals;
    }

    public IndividualQcAnalysisParams setIndividuals(List<String> individuals) {
        this.individuals = individuals;
        return this;
    }

    public String getIndividual() {
        return individual;
    }

    public IndividualQcAnalysisParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public IndividualQcAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getInferredSexMethod() {
        return inferredSexMethod;
    }

    public IndividualQcAnalysisParams setInferredSexMethod(String inferredSexMethod) {
        this.inferredSexMethod = inferredSexMethod;
        return this;
    }

    public String getResourcesDir() {
        return resourcesDir;
    }

    public IndividualQcAnalysisParams setResourcesDir(String resourcesDir) {
        this.resourcesDir = resourcesDir;
        return this;
    }

    public List<String> getSkip() {
        return skip;
    }

    public IndividualQcAnalysisParams setSkip(List<String> skip) {
        this.skip = skip;
        return this;
    }

    public Boolean getSkipIndex() {
        return skipIndex;
    }

    public IndividualQcAnalysisParams setSkipIndex(Boolean skipIndex) {
        this.skipIndex = skipIndex;
        return this;
    }

    public Boolean getOverwrite() {
        return overwrite;
    }

    public IndividualQcAnalysisParams setOverwrite(Boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public IndividualQcAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
