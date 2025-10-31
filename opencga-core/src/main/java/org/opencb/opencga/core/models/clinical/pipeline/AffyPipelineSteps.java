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

public class AffyPipelineSteps {

    @DataField(id = "qualityControl", description = "Quality control step")
    private QualityControlPipelineStep qualityControl;

    @DataField(id = "variantCalling", description = "Variant calling step")
    private GenomicsVariantCallingPipelineStep variantCalling;

    public AffyPipelineSteps() {
    }

    public AffyPipelineSteps(QualityControlPipelineStep qualityControl, GenomicsVariantCallingPipelineStep variantCalling) {
        this.qualityControl = qualityControl;
        this.variantCalling = variantCalling;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineSteps{");
        sb.append("qualityControl=").append(qualityControl);
        sb.append(", variantCalling=").append(variantCalling);
        sb.append('}');
        return sb.toString();
    }

    public QualityControlPipelineStep getQualityControl() {
        return qualityControl;
    }

    public AffyPipelineSteps setQualityControl(QualityControlPipelineStep qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public GenomicsVariantCallingPipelineStep getVariantCalling() {
        return variantCalling;
    }

    public AffyPipelineSteps setVariantCalling(GenomicsVariantCallingPipelineStep variantCalling) {
        this.variantCalling = variantCalling;
        return this;
    }
}