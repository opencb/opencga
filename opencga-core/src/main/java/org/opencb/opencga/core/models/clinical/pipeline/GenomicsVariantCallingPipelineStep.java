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

public class GenomicsVariantCallingPipelineStep extends PipelineStep {

    @DataField(id = "tools", description = "Variant calling tools")
    private List<GenomicsVariantCallingPipelineTool> tools;

    public GenomicsVariantCallingPipelineStep() {
    }

    public GenomicsVariantCallingPipelineStep(List<GenomicsVariantCallingPipelineTool> tools) {
        this.tools = tools;
    }

    public GenomicsVariantCallingPipelineStep(Map<String, Object> options, List<GenomicsVariantCallingPipelineTool> tools) {
        super(options);
        this.tools = tools;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineVariantCallingStep{");
        sb.append("tools=").append(tools);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public List<GenomicsVariantCallingPipelineTool> getTools() {
        return tools;
    }

    public GenomicsVariantCallingPipelineStep setTools(List<GenomicsVariantCallingPipelineTool> tools) {
        this.tools = tools;
        return this;
    }
}