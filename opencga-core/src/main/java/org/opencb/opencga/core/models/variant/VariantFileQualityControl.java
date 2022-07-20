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

package org.opencb.opencga.core.models.variant;

import org.opencb.biodata.formats.sequence.ascat.AscatMetrics;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantFileQualityControl {

    @DataField(description = ParamConstants.VARIANT_FILE_QUALITY_CONTROL_VARIANT_SET_METRICS_DESCRIPTION)
    private VariantSetStats variantSetMetrics;
    @DataField(description = ParamConstants.VARIANT_FILE_QUALITY_CONTROL_ASCAT_METRICS_DESCRIPTION)
    private AscatMetrics ascatMetrics;

    public VariantFileQualityControl() {
    }

    public VariantFileQualityControl(VariantSetStats variantSetMetrics, AscatMetrics ascatMetrics) {
        this.variantSetMetrics = variantSetMetrics;
        this.ascatMetrics = ascatMetrics;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantFileQualityControl{");
        sb.append("variantSetMetrics=").append(variantSetMetrics);
        sb.append(", ascatMetrics=").append(ascatMetrics);
        sb.append('}');
        return sb.toString();
    }

    public AscatMetrics getAscatMetrics() {
        return ascatMetrics;
    }

    public VariantFileQualityControl setAscatMetrics(AscatMetrics ascatMetrics) {
        this.ascatMetrics = ascatMetrics;
        return this;
    }

    public VariantSetStats getVariantSetMetrics() {
        return variantSetMetrics;
    }

    public VariantFileQualityControl setVariantSetMetrics(VariantSetStats variantSetMetrics) {
        this.variantSetMetrics = variantSetMetrics;
        return this;
    }
}
