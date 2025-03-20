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

import java.util.Arrays;
import java.util.List;

public class IndividualQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Individual QC analysis params";

    @DataField(id = "individuals", description = FieldConstants.INDIVIDUAL_QC_INDIVIDUAL_ID_LIST_DESCRIPTION, deprecated = true)
    private List<String> individuals;

    @Deprecated
    @DataField(id = "individual", description = FieldConstants.INDIVIDUAL_QC_INDIVIDUAL_ID_DESCRIPTION, deprecated = true)
    private String individual;

    @Deprecated
    @DataField(id = "sample", description = FieldConstants.INDIVIDUAL_QC_SAMPLE_ID_DESCRIPTION, deprecated = true)
    private String sample;

    /**
     * @deprecated to be removed after latest changes take place
     */
    @Deprecated
    @DataField(id = "inferredSexMethod", description = FieldConstants.INFERRED_SEX_METHOD_DESCRIPTION, deprecated = true)
    private String inferredSexMethod;

    @Deprecated
    @DataField(id = "skipAnalysis", description = FieldConstants.INDIVIDUAL_QC_SKIP_ANALYSIS_DESCRIPTION)
    private List<String> skipAnalysis;

    @DataField(id = "skipIndex", description = FieldConstants.QC_SKIP_INDEX_DESCRIPTION)
    private Boolean skipIndex;

    @DataField(id = "overwrite", description = FieldConstants.QC_OVERWRITE_DESCRIPTION)
    private Boolean overwrite;

    @DataField(id = "resourcesDir", description = FieldConstants.RESOURCES_DIR_DESCRIPTION)
    private String resourcesDir;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public IndividualQcAnalysisParams() {
    }

    @Deprecated
    public IndividualQcAnalysisParams(List<String> individuals, String individual, String sample, String inferredSexMethod,
                                      String resourcesDir, List<String> skipAnalysis, Boolean skipIndex, Boolean overwrite, String outdir) {
        this.individuals = individuals;
        this.individual = individual;
        this.sample = sample;
        this.inferredSexMethod = inferredSexMethod;
        this.resourcesDir = resourcesDir;
        this.skipAnalysis = skipAnalysis;
        this.skipIndex = skipIndex;
        this.overwrite = overwrite;
        this.outdir = outdir;
    }

    public IndividualQcAnalysisParams(List<String> individuals, Boolean skipIndex, Boolean overwrite, String resourcesDir, String outdir) {
        this.individuals = individuals;
        this.skipIndex = skipIndex;
        this.overwrite = overwrite;
        this.resourcesDir = resourcesDir;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQcAnalysisParams{");
        sb.append("individuals=").append(individuals);
        sb.append(", skipIndex=").append(skipIndex);
        sb.append(", overwrite=").append(overwrite);
        sb.append(", resourcesDir='").append(resourcesDir).append('\'');
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

    @Deprecated
    public String getIndividual() {
        return individuals.get(0);
    }

    @Deprecated
    public IndividualQcAnalysisParams setIndividual(String individual) {
        this.individuals = Arrays.asList(individual);
        return this;
    }

    @Deprecated
    public String getSample() {
        return sample;
    }

    @Deprecated
    public IndividualQcAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    @Deprecated
    public String getInferredSexMethod() {
        return inferredSexMethod;
    }

    @Deprecated
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

    @Deprecated
    public List<String> getSkipAnalysis() {
        return skipAnalysis;
    }

    @Deprecated
    public IndividualQcAnalysisParams setSkipAnalysis(List<String> skipAnalysis) {
        this.skipAnalysis = skipAnalysis;
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
