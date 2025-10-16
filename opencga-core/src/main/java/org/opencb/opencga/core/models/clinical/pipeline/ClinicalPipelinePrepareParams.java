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

package org.opencb.opencga.core.models.clinical.pipeline;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.ArrayList;
import java.util.List;

public class ClinicalPipelinePrepareParams {

    @DataField(id = "referenceGenome", description = FieldConstants.CLINICAL_PIPELINE_REF_GENOME_DESCRIPTION)
    private String referenceGenome;

    @DataField(id = "alignerIndexes", description = FieldConstants.CLINICAL_PIPELINE_ALIGNER_INDEXES_DESCRIPTION)
    private List<String> alignerIndexes;

    public ClinicalPipelinePrepareParams() {
        this.alignerIndexes = new ArrayList<>();
    }

    public ClinicalPipelinePrepareParams(String referenceGenome, List<String> alignerIndexes) {
        this.referenceGenome = referenceGenome;
        this.alignerIndexes = alignerIndexes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalPipelinePrepareWrapperParams{");
        sb.append("referenceGenome='").append(referenceGenome).append('\'');
        sb.append(", alignerIndexes=").append(alignerIndexes);
        sb.append('}');
        return sb.toString();
    }

    public String getReferenceGenome() {
        return referenceGenome;
    }

    public ClinicalPipelinePrepareParams setReferenceGenome(String referenceGenome) {
        this.referenceGenome = referenceGenome;
        return this;
    }

    public List<String> getAlignerIndexes() {
        return alignerIndexes;
    }

    public ClinicalPipelinePrepareParams setAlignerIndexes(List<String> alignerIndexes) {
        this.alignerIndexes = alignerIndexes;
        return this;
    }
}


