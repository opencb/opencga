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
    private String id;

    @DataField(id = "index", description = "Tool index path")
    private String index;

    @DataField(id = "reference", description = "Reference genome path")
    private String reference;

    @DataField(id = "parameters", description = "Tool command line parameters")
    private Map<String, Object> parameters;

    @DataField(id = "options", description = "Tool-specific options")
    private Map<String, Object> options;

    public PipelineTool() {
    }

    public PipelineTool(String id, String index, String reference, Map<String, Object> parameters, Map<String, Object> options) {
        this.id = id;
        this.index = index;
        this.reference = reference;
        this.parameters = parameters;
        this.options = options;
    }

    public String getId() { return id; }
    public PipelineTool setId(String id) { this.id = id; return this; }

    public String getIndex() { return index; }
    public PipelineTool setIndex(String index) { this.index = index; return this; }

    public String getReference() { return reference; }
    public PipelineTool setReference(String reference) { this.reference = reference; return this; }

    public Map<String, Object> getParameters() { return parameters; }
    public PipelineTool setParameters(Map<String, Object> parameters) { this.parameters = parameters; return this; }

    public Map<String, Object> getOptions() { return options; }
    public PipelineTool setOptions(Map<String, Object> options) { this.options = options; return this; }

    @Override
    public String toString() {
        return "PipelineTool{" +
                "id='" + id + '\'' +
                ", index='" + index + '\'' +
                ", reference='" + reference + '\'' +
                ", parameters=" + parameters +
                ", options=" + options +
                '}';
    }
}