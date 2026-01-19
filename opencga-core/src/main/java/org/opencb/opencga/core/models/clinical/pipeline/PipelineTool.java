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

import java.util.Map;

public class PipelineTool {

    @DataField(id = "id", description = "Tool identifier")
    protected String id;

    @DataField(id = "parameters", description = "Tool command line parameters")
    protected Map<String, Object> parameters;

    @DataField(id = "options", description = "Tool-specific options")
    protected Map<String, Object> options;

    public PipelineTool() {
    }

    public PipelineTool(String id, Map<String, Object> parameters, Map<String, Object> options) {
        this.id = id;
        this.parameters = parameters;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineTool{");
        sb.append("id='").append(id).append('\'');
        sb.append(", parameters=").append(parameters);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public PipelineTool setId(String id) {
        this.id = id;
        return this;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public PipelineTool setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
        return this;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public PipelineTool setOptions(Map<String, Object> options) {
        this.options = options;
        return this;
    }
}