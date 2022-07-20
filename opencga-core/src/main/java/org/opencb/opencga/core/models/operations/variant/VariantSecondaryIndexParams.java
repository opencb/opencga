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

package org.opencb.opencga.core.models.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantSecondaryIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Variant secondary index params.";

    public VariantSecondaryIndexParams() {
    }
    public VariantSecondaryIndexParams(String region, List<String> sample, boolean overwrite) {
        this.region = region;
        this.sample = sample;
        this.overwrite = overwrite;
    }
    @DataField(description = ParamConstants.VARIANT_SECONDARY_INDEX_PARAMS_REGION_DESCRIPTION)
    private String region;
    @DataField(description = ParamConstants.VARIANT_SECONDARY_INDEX_PARAMS_SAMPLE_DESCRIPTION)
    private List<String> sample;
    @DataField(description = ParamConstants.VARIANT_SECONDARY_INDEX_PARAMS_OVERWRITE_DESCRIPTION)
    private boolean overwrite;

    public String getRegion() {
        return region;
    }

    public VariantSecondaryIndexParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public List<String> getSample() {
        return sample;
    }

    public VariantSecondaryIndexParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public VariantSecondaryIndexParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }
}
