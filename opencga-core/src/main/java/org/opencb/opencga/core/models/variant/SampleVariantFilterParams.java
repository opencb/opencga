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

import java.util.Arrays;
import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class SampleVariantFilterParams extends AnnotationVariantQueryParams {

    public static final String DESCRIPTION = "Sample variant filter params";
    private List<String> genotypes = Arrays.asList("0/1", "1/1");
    @DataField(description = ParamConstants.SAMPLE_VARIANT_FILTER_PARAMS_SAMPLE_DESCRIPTION)
    private List<String> sample;
    private boolean samplesInAllVariants = false;
    private int maxVariants = 50;

    public List<String> getGenotypes() {
        return genotypes;
    }

    public SampleVariantFilterParams setGenotypes(List<String> genotypes) {
        this.genotypes = genotypes;
        return this;
    }

    public List<String> getSample() {
        return sample;
    }

    public SampleVariantFilterParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public boolean isSamplesInAllVariants() {
        return samplesInAllVariants;
    }

    public SampleVariantFilterParams setSamplesInAllVariants(boolean samplesInAllVariants) {
        this.samplesInAllVariants = samplesInAllVariants;
        return this;
    }

    public int getMaxVariants() {
        return maxVariants;
    }

    public SampleVariantFilterParams setMaxVariants(int maxVariants) {
        this.maxVariants = maxVariants;
        return this;
    }
}
