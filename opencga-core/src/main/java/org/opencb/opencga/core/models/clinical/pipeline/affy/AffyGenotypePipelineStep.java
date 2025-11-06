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
import org.opencb.opencga.core.models.clinical.pipeline.PipelineStep;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineTool;

import java.util.Map;

public class AffyGenotypePipelineStep extends PipelineStep {

    @DataField(id = "tool", description = "Affy genotype tool")
    private PipelineTool tool;

    public AffyGenotypePipelineStep() {
        super();
    }

    public AffyGenotypePipelineStep(PipelineTool tool) {
        this.tool = tool;
    }

    public AffyGenotypePipelineStep(Map<String, Object> options, PipelineTool tool) {
        super(options);
        this.tool = tool;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineQualityControlStep{");
        sb.append("tool=").append(tool);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public PipelineTool getTool() {
        return tool;
    }

    public AffyGenotypePipelineStep setTool(PipelineTool tool) {
        this.tool = tool;
        return this;
    }
}