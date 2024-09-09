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

public class FamilyQcRelatednessAnalysisParams {

    @DataField(id = "populationFrequencyFile", description = FieldConstants.FAMILY_QC_RELATEDNESS_POP_FREQ_FILE_DESCRIPTION)
    private String populationFrequencyFile;

    @DataField(id = "populationExcludeVariantsFile", description = FieldConstants.FAMILY_QC_RELATEDNESS_POP_EXCLUDE_VAR_FILE_DESCRIPTION)
    private String populationExcludeVariantsFile;

    @DataField(id = "thresholdsFile", description = FieldConstants.FAMILY_QC_RELATEDNESS_THRESHOLD_FILE_DESCRIPTION)
    private String thresholdsFile;

    public FamilyQcRelatednessAnalysisParams() {
    }

    public FamilyQcRelatednessAnalysisParams(String populationFrequencyFile, String populationExcludeVariantsFile, String thresholdsFile) {
        this.populationFrequencyFile = populationFrequencyFile;
        this.populationExcludeVariantsFile = populationExcludeVariantsFile;
        this.thresholdsFile = thresholdsFile;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyQcRelatednessAnalysisParams{");
        sb.append("populationFrequencyFile='").append(populationFrequencyFile).append('\'');
        sb.append(", populationExcludeVariantsFile='").append(populationExcludeVariantsFile).append('\'');
        sb.append(", thresholdsFile='").append(thresholdsFile).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getPopulationFrequencyFile() {
        return populationFrequencyFile;
    }

    public FamilyQcRelatednessAnalysisParams setPopulationFrequencyFile(String populationFrequencyFile) {
        this.populationFrequencyFile = populationFrequencyFile;
        return this;
    }

    public String getPopulationExcludeVariantsFile() {
        return populationExcludeVariantsFile;
    }

    public FamilyQcRelatednessAnalysisParams setPopulationExcludeVariantsFile(String populationExcludeVariantsFile) {
        this.populationExcludeVariantsFile = populationExcludeVariantsFile;
        return this;
    }

    public String getThresholdsFile() {
        return thresholdsFile;
    }

    public FamilyQcRelatednessAnalysisParams setThresholdsFile(String thresholdsFile) {
        this.thresholdsFile = thresholdsFile;
        return this;
    }
}
