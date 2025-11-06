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

import org.opencb.opencga.core.models.clinical.pipeline.ClinicalPipelineParams;

public class GenomicsClinicalPipelineParams extends ClinicalPipelineParams<GenomicsPipelineConfig> {

    public GenomicsClinicalPipelineParams() {
        super();
        pipeline = new GenomicsPipelineConfig();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenomicsClinicalPipelineParams{");
        sb.append("samples=").append(samples);
        sb.append(", dataDir='").append(dataDir).append('\'');
        sb.append(", indexDir='").append(indexDir).append('\'');
        sb.append(", steps=").append(steps);
        sb.append(", variantIndexParams=").append(variantIndexParams);
        sb.append(", pipelineFile='").append(pipelineFile).append('\'');
        sb.append(", pipeline=").append(pipeline);
        sb.append('}');
        return sb.toString();
    }
}


