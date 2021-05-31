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

import org.opencb.biodata.models.variant.metadata.VariantSetStats;

public class VariantFileQualityControl {

    private VariantSetStats variantSetMetrics;

    public VariantFileQualityControl() {
    }

    public VariantFileQualityControl(VariantSetStats variantSetStats) {
        this.variantSetMetrics = variantSetStats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantFileQualityControl{");
        sb.append("variantSetMetrics=").append(variantSetMetrics);
        sb.append('}');
        return sb.toString();
    }

    public VariantSetStats getVariantSetMetrics() {
        return variantSetMetrics;
    }

    public VariantFileQualityControl setVariantSetMetrics(VariantSetStats variantSetMetrics) {
        this.variantSetMetrics = variantSetMetrics;
        return this;
    }
}
