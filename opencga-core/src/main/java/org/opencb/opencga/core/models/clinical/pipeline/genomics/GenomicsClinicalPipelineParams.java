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

package org.opencb.opencga.core.models.clinical.pipeline.genomics;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.clinical.pipeline.ClinicalPipelineParams;
import org.opencb.opencga.core.models.operations.variant.VariantIndexParams;

import java.util.List;

public class GenomicsClinicalPipelineParams extends ClinicalPipelineParams<GenomicsPipelineConfig> {

    @DataField(id = "samples", description = "List of samples following the format: sample_id:file_id1;file_id2[:rol[:somatic]]; 'rol'"
            + " can be 'mother', 'father' or 'child'. If the sample is somatic, then add ':somatic' at")
    protected List<String> samples;

    public GenomicsClinicalPipelineParams() {
        super();
    }

    public GenomicsClinicalPipelineParams(List<String> samples) {
        this.samples = samples;
    }

    public GenomicsClinicalPipelineParams(String indexDir, List<String> steps, VariantIndexParams variantIndexParams, String pipelineFile,
                                          GenomicsPipelineConfig pipeline, List<String> samples) {
        super(indexDir, steps, variantIndexParams, pipelineFile, pipeline);
        this.samples = samples;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenomicsClinicalPipelineParams{");
        sb.append("samples=").append(samples);
        sb.append(", indexDir='").append(indexDir).append('\'');
        sb.append(", steps=").append(steps);
        sb.append(", variantIndexParams=").append(variantIndexParams);
        sb.append(", pipelineFile='").append(pipelineFile).append('\'');
        sb.append(", pipeline=").append(pipeline);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getSamples() {
        return samples;
    }

    public GenomicsClinicalPipelineParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }
}


