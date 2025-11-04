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
    protected String name;

    @DataField(id = "version", description = "Pipeline version")
    protected String version;

    @DataField(id = "type", description = "Pipeline type")
    protected String type;

    @DataField(id = "description", description = "Pipeline description")
    protected String description;

    @DataField(id = "input", description = "Pipeline input configuration")
    protected PipelineInput input;

    public PipelineConfig() {
        this.input = new PipelineInput();
    }

    public PipelineConfig(String name, String version, String type, String description, PipelineInput input) {
        this.name = name;
        this.version = version;
        this.type = type;
        this.description = description;
        this.input = input;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineConfig{");
        sb.append("name='").append(name).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", input=").append(input);
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

    public String getType() {
        return type;
    }

    public PipelineConfig setType(String type) {
        this.type = type;
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
}