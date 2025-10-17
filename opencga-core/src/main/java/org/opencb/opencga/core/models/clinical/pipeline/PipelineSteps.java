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

public class PipelineSteps {

    @DataField(id = "qualityControl", description = "Quality control step")
    private PipelineQualityControlStep qualityControl;

    @DataField(id = "alignment", description = "Alignment step")
    private PipelineAlignmentStep alignment;

    @DataField(id = "variantCalling", description = "Variant calling step")
    private PipelineVariantCallingStep variantCalling;

    public PipelineSteps() {
    }

    public PipelineSteps(PipelineQualityControlStep qualityControl, PipelineAlignmentStep alignment,
                         PipelineVariantCallingStep variantCalling) {
        this.qualityControl = qualityControl;
        this.alignment = alignment;
        this.variantCalling = variantCalling;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineSteps{");
        sb.append("qualityControl=").append(qualityControl);
        sb.append(", alignment=").append(alignment);
        sb.append(", variantCalling=").append(variantCalling);
        sb.append('}');
        return sb.toString();
    }

    public PipelineQualityControlStep getQualityControl() {
        return qualityControl;
    }

    public PipelineSteps setQualityControl(PipelineQualityControlStep qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public PipelineAlignmentStep getAlignment() {
        return alignment;
    }

    public PipelineSteps setAlignment(PipelineAlignmentStep alignment) {
        this.alignment = alignment;
        return this;
    }

    public PipelineVariantCallingStep getVariantCalling() {
        return variantCalling;
    }

    public PipelineSteps setVariantCalling(PipelineVariantCallingStep variantCalling) {
        this.variantCalling = variantCalling;
        return this;
    }
}