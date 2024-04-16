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

public class VariantAggregateParams extends ToolParams {

    public static final String DESCRIPTION = "Variant aggregate params.";
    //    private String region
    @DataField(description = "Overwrite aggregation for all files and variants. Repeat operation for already processed variants.")
    private boolean overwrite;
    @DataField(description = ParamConstants.RESUME_DESCRIPTION)
    private boolean resume;

    public VariantAggregateParams() {
    }

    public VariantAggregateParams(boolean overwrite, boolean resume) {
        this.overwrite = overwrite;
        this.resume = resume;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public VariantAggregateParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantAggregateParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }
}
