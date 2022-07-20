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

public class VariantFamilyIndexParams extends ToolParams {

    public static final String DESCRIPTION = "Variant family index params.";
    @DataField(description = ParamConstants.VARIANT_FAMILY_INDEX_PARAMS_FAMILY_DESCRIPTION)
    private List<String> family;
    @DataField(description = ParamConstants.VARIANT_FAMILY_INDEX_PARAMS_OVERWRITE_DESCRIPTION)
    private boolean overwrite;
    @DataField(description = ParamConstants.VARIANT_FAMILY_INDEX_PARAMS_UPDATE_INDEX_DESCRIPTION)
    private boolean updateIndex;
    @DataField(description = ParamConstants.VARIANT_FAMILY_INDEX_PARAMS_SKIP_INCOMPLETE_FAMILIES_DESCRIPTION)
    private boolean skipIncompleteFamilies;

    public VariantFamilyIndexParams() {
    }

    public VariantFamilyIndexParams(List<String> family, boolean overwrite, boolean updateIndex, boolean skipIncompleteFamilies) {
        this.family = family;
        this.overwrite = overwrite;
        this.updateIndex = updateIndex;
        this.skipIncompleteFamilies = skipIncompleteFamilies;
    }

    public List<String> getFamily() {
        return family;
    }

    public VariantFamilyIndexParams setFamily(List<String> family) {
        this.family = family;
        return this;
    }

    public boolean isUpdateIndex() {
        return updateIndex;
    }

    public VariantFamilyIndexParams setUpdateIndex(boolean updateIndex) {
        this.updateIndex = updateIndex;
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

    public VariantFamilyIndexParams setSkipIncompleteFamilies(boolean skipIncompleteFamilies) {
        this.skipIncompleteFamilies = skipIncompleteFamilies;
        return this;
    }
}
