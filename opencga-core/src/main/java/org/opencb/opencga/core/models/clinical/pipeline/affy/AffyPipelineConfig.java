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

package org.opencb.opencga.core.models.clinical.pipeline.affy;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineConfig;

public class AffyPipelineConfig extends PipelineConfig {

    @DataField(id = "input", description = "Affy pipeline input configuration")
    private AffyPipelineInput input;

    @DataField(id = "steps", description = "Affy pipeline steps (quality control and genotype)")
    private AffyPipelineSteps steps;

    public AffyPipelineConfig() {
        super();
    }

    public AffyPipelineConfig(String name, String version, String type, String description, AffyPipelineInput input,
                              AffyPipelineSteps steps) {
        super(name, version, type, description);
        this.input = input;
        this.steps = steps;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AffyPipelineConfig{");
        sb.append("input=").append(input);
        sb.append(", steps=").append(steps);
        sb.append(", name='").append(name).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public AffyPipelineInput getInput() {
        return input;
    }

    public AffyPipelineConfig setInput(AffyPipelineInput input) {
        this.input = input;
        return this;
    }

    public AffyPipelineSteps getSteps() {
        return steps;
    }

    public AffyPipelineConfig setSteps(AffyPipelineSteps steps) {
        this.steps = steps;
        return this;
    }
}