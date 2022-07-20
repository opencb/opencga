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

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantSampleDeleteParams extends ToolParams {

    public static final String DESCRIPTION = "Variant delete sample params";

    public VariantSampleDeleteParams() {
    }

    public VariantSampleDeleteParams(List<String> sample, boolean force, boolean resume) {
        this.sample = sample;
        this.force = force;
        this.resume = resume;
    }

    @DataField(description = ParamConstants.VARIANT_SAMPLE_DELETE_PARAMS_SAMPLE_DESCRIPTION)
    private List<String> sample;
    @DataField(description = ParamConstants.VARIANT_SAMPLE_DELETE_PARAMS_FORCE_DESCRIPTION)
    private boolean force;
    @DataField(description = ParamConstants.VARIANT_SAMPLE_DELETE_PARAMS_RESUME_DESCRIPTION)
    private boolean resume;

    public List<String> getSample() {
        return sample;
    }

    public VariantSampleDeleteParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public boolean isForce() {
        return force;
    }

    public VariantSampleDeleteParams setForce(boolean force) {
        this.force = force;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantSampleDeleteParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }
}
