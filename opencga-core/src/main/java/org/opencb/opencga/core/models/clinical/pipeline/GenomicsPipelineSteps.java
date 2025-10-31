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

public class GenomicsPipelineSteps {

    @DataField(id = "qualityControl", description = "Quality control step")
    private QualityControlPipelineStep qualityControl;

    @DataField(id = "alignment", description = "Alignment step")
    private GenomicsAlignmentPipelineStep alignment;

    @DataField(id = "variantCalling", description = "Variant calling step")
    private GenomicsVariantCallingPipelineStep variantCalling;

    public GenomicsPipelineSteps() {
    }

    public GenomicsPipelineSteps(QualityControlPipelineStep qualityControl, GenomicsAlignmentPipelineStep alignment,
                                 GenomicsVariantCallingPipelineStep variantCalling) {
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

    public QualityControlPipelineStep getQualityControl() {
        return qualityControl;
    }

    public GenomicsPipelineSteps setQualityControl(QualityControlPipelineStep qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public GenomicsAlignmentPipelineStep getAlignment() {
        return alignment;
    }

    public GenomicsPipelineSteps setAlignment(GenomicsAlignmentPipelineStep alignment) {
        this.alignment = alignment;
        return this;
    }

    public GenomicsVariantCallingPipelineStep getVariantCalling() {
        return variantCalling;
    }

    public GenomicsPipelineSteps setVariantCalling(GenomicsVariantCallingPipelineStep variantCalling) {
        this.variantCalling = variantCalling;
        return this;
    }
}