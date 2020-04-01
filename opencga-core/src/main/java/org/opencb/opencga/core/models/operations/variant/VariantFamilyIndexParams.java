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

public class VariantFamilyIndexParams extends ToolParams {

    public static final String DESCRIPTION = "Variant family index params.";
    private List<String> family;
    private boolean overwrite;
    private boolean skipIncompleteFamilies;

    public VariantFamilyIndexParams() {
    }

    public VariantFamilyIndexParams(List<String> family, boolean overwrite, boolean skipIncompleteFamilies) {
        this.family = family;
        this.overwrite = overwrite;
        this.skipIncompleteFamilies = skipIncompleteFamilies;
    }

    public List<String> getFamily() {
        return family;
    }

    public VariantFamilyIndexParams setFamily(List<String> family) {
        this.family = family;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public VariantFamilyIndexParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public boolean isSkipIncompleteFamilies() {
        return skipIncompleteFamilies;
    }

    public void setSkipIncompleteFamilies(boolean skipIncompleteFamilies) {
        this.skipIncompleteFamilies = skipIncompleteFamilies;
    }
}
