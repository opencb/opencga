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

public class VariantSampleIndexParams extends ToolParams {

    public static final String DESCRIPTION = "Variant sample index params";
    private List<String> sample;
    private boolean buildIndex;
    private boolean annotate;
    private boolean familyIndex;
    private boolean overwrite;

    public VariantSampleIndexParams() {
    }

    public VariantSampleIndexParams(List<String> sample, boolean buildIndex, boolean annotate, boolean familyIndex, boolean overwrite) {
        this.sample = sample;
        this.buildIndex = buildIndex;
        this.annotate = annotate;
        this.familyIndex = familyIndex;
        this.overwrite = overwrite;
    }

    public List<String> getSample() {
        return sample;
    }

    public VariantSampleIndexParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public boolean isBuildIndex() {
        return buildIndex;
    }

    public VariantSampleIndexParams setBuildIndex(boolean buildIndex) {
        this.buildIndex = buildIndex;
        return this;
    }

    public boolean isAnnotate() {
        return annotate;
    }

    public VariantSampleIndexParams setAnnotate(boolean annotate) {
        this.annotate = annotate;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public VariantSampleIndexParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public boolean isFamilyIndex() {
        return familyIndex;
    }

    public VariantSampleIndexParams setFamilyIndex(boolean familyIndex) {
        this.familyIndex = familyIndex;
        return this;
    }
}
