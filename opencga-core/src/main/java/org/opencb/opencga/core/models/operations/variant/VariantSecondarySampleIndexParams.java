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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantSecondarySampleIndexParams extends ToolParams {

    public static final String DESCRIPTION = "Variant sample index params";
    @DataField(description = ParamConstants.VARIANT_SAMPLE_INDEX_PARAMS_SAMPLE_DESCRIPTION)
    private List<String> sample;
    @DataField(description = ParamConstants.VARIANT_SAMPLE_INDEX_PARAMS_BUILD_INDEX_DESCRIPTION)
    private boolean buildIndex;
    @DataField(description = ParamConstants.VARIANT_SAMPLE_INDEX_PARAMS_ANNOTATE_DESCRIPTION)
    private boolean annotate;
    @DataField(description = ParamConstants.VARIANT_SAMPLE_INDEX_PARAMS_FAMILY_INDEX_DESCRIPTION)
    private boolean familyIndex;
    @DataField(description = ParamConstants.VARIANT_SAMPLE_INDEX_PARAMS_OVERWRITE_DESCRIPTION)
    private boolean overwrite;

    public VariantSecondarySampleIndexParams() {
    }

    public VariantSecondarySampleIndexParams(List<String> sample, boolean buildIndex, boolean annotate, boolean familyIndex, boolean overwrite) {
        this.sample = sample;
        this.buildIndex = buildIndex;
        this.annotate = annotate;
        this.familyIndex = familyIndex;
        this.overwrite = overwrite;
    }

    public List<String> getSample() {
        return sample;
    }

    public VariantSecondarySampleIndexParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public boolean isBuildIndex() {
        return buildIndex;
    }

    public VariantSecondarySampleIndexParams setBuildIndex(boolean buildIndex) {
        this.buildIndex = buildIndex;
        return this;
    }

    public boolean isAnnotate() {
        return annotate;
    }

    public VariantSecondarySampleIndexParams setAnnotate(boolean annotate) {
        this.annotate = annotate;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public VariantSecondarySampleIndexParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public boolean isFamilyIndex() {
        return familyIndex;
    }

    public VariantSecondarySampleIndexParams setFamilyIndex(boolean familyIndex) {
        this.familyIndex = familyIndex;
        return this;
    }
}
