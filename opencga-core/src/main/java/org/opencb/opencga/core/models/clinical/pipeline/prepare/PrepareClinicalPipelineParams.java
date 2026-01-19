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

package org.opencb.opencga.core.models.clinical.pipeline.prepare;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class PrepareClinicalPipelineParams extends ToolParams {

    @DataField(id = "referenceGenome", description = "Reference genome to be used in the clinical pipeline.")
    private String referenceGenome;

    @DataField(id = "indexes", description = "List of indexes to be prepared for the clinical pipeline, e.g., bwa, bowtie, affy,...")
    private List<String> indexes;

    public PrepareClinicalPipelineParams() {
    }

    public PrepareClinicalPipelineParams(String referenceGenome, List<String> indexes) {
        this.referenceGenome = referenceGenome;
        this.indexes = indexes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PrepareClinicalPipelineParams{");
        sb.append("referenceGenome='").append(referenceGenome).append('\'');
        sb.append(", indexes=").append(indexes);
        sb.append('}');
        return sb.toString();
    }

    public String getReferenceGenome() {
        return referenceGenome;
    }

    public PrepareClinicalPipelineParams setReferenceGenome(String referenceGenome) {
        this.referenceGenome = referenceGenome;
        return this;
    }

    public List<String> getIndexes() {
        return indexes;
    }

    public PrepareClinicalPipelineParams setIndexes(List<String> indexes) {
        this.indexes = indexes;
        return this;
    }
}


