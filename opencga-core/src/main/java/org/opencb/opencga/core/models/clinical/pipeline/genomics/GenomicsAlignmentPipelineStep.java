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
import org.opencb.opencga.core.models.clinical.pipeline.PipelineStep;

import java.util.Map;

public class GenomicsAlignmentPipelineStep extends PipelineStep {

    @DataField(id = "tool", description = "Alignment tool")
    private GenomicsAlignmentPipelineTool tool;

    public GenomicsAlignmentPipelineStep() {
    }

    public GenomicsAlignmentPipelineStep(GenomicsAlignmentPipelineTool tool) {
        this.tool = tool;
    }

    public GenomicsAlignmentPipelineStep(Map<String, Object> options, GenomicsAlignmentPipelineTool tool) {
        super(options);
        this.tool = tool;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineAlignmentStep{");
        sb.append("tool=").append(tool);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public GenomicsAlignmentPipelineTool getTool() {
        return tool;
    }

    public GenomicsAlignmentPipelineStep setTool(GenomicsAlignmentPipelineTool tool) {
        this.tool = tool;
        return this;
    }
}