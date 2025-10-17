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

public class PipelineConfig {

    @DataField(id = "name", description = "Pipeline name")
    private String name;

    @DataField(id = "version", description = "Pipeline version")
    private String version;

    @DataField(id = "description", description = "Pipeline description")
    private String description;

    @DataField(id = "input", description = "Pipeline input configuration")
    private PipelineInput input;

    @DataField(id = "steps", description = "Pipeline steps (quality control, alignment, variant calling)")
    private PipelineSteps steps;

    public PipelineConfig() {
    }

    public PipelineConfig(String name, String version, String description, PipelineInput input, PipelineSteps steps) {
        this.name = name;
        this.version = version;
        this.description = description;
        this.input = input;
        this.steps = steps;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineConfig{");
        sb.append("name='").append(name).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", input=").append(input);
        sb.append(", steps=").append(steps);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public PipelineConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public PipelineConfig setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public PipelineConfig setDescription(String description) {
        this.description = description;
        return this;
    }

    public PipelineInput getInput() {
        return input;
    }

    public PipelineConfig setInput(PipelineInput input) {
        this.input = input;
        return this;
    }

    public PipelineSteps getSteps() {
        return steps;
    }

    public PipelineConfig setSteps(PipelineSteps steps) {
        this.steps = steps;
        return this;
    }
}