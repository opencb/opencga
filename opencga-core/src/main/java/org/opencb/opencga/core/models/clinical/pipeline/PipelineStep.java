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

import java.util.List;
import java.util.Map;

public class PipelineStep {

    @DataField(id = "id", description = "Step identifier")
    private String id;

    @DataField(id = "options", description = "Step-specific options")
    private Map<String, Object> options;

    @DataField(id = "tool", description = "Single tool configuration")
    private PipelineTool tool;

    @DataField(id = "tools", description = "Multiple tools configuration")
    private List<PipelineTool> tools;

    public PipelineStep() {
    }

    public PipelineStep(String id, Map<String, Object> options, PipelineTool tool, List<PipelineTool> tools) {
        this.id = id;
        this.options = options;
        this.tool = tool;
        this.tools = tools;
    }

    public String getId() { return id; }
    public PipelineStep setId(String id) { this.id = id; return this; }

    public Map<String, Object> getOptions() { return options; }
    public PipelineStep setOptions(Map<String, Object> options) { this.options = options; return this; }

    public PipelineTool getTool() { return tool; }
    public PipelineStep setTool(PipelineTool tool) { this.tool = tool; return this; }

    public List<PipelineTool> getTools() { return tools; }
    public PipelineStep setTools(List<PipelineTool> tools) { this.tools = tools; return this; }

    @Override
    public String toString() {
        return "PipelineStep{" +
                "id='" + id + '\'' +
                ", options=" + options +
                ", tool=" + tool +
                ", tools=" + tools +
                '}';
    }
}