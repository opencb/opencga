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

public class GenomicsPipelineConfig extends PipelineConfig {

    @DataField(id = "steps", description = "Pipeline steps (quality control, alignment, variant calling)")
    private GenomicsPipelineSteps steps;

    public GenomicsPipelineConfig() {
    }

    public GenomicsPipelineConfig(String name, String version, String type, String description, PipelineInput input, GenomicsPipelineSteps steps) {
        super(name, version, type, description, input);
        this.steps = steps;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenomicsPipelineConfig{");
        sb.append("steps=").append(steps);
        sb.append(", name='").append(name).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", input=").append(input);
        sb.append('}');
        return sb.toString();
    }

    public GenomicsPipelineSteps getSteps() {
        return steps;
    }

    public GenomicsPipelineConfig setSteps(GenomicsPipelineSteps steps) {
        this.steps = steps;
        return this;
    }
}