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

package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.common.Enums;

@Tool(id = VariantSecondaryIndexSamplesDeleteOperationTool.ID,
        description = VariantSecondaryIndexSamplesDeleteOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION,
        resource = Enums.Resource.VARIANT)
@Deprecated
public class VariantSecondaryIndexSamplesDeleteOperationTool extends OperationTool {

    public static final String ID = "variant-secondary-index-samples-delete";
    public static final String DESCRIPTION = "Remove a secondary index from the search engine for a specific set of samples.";

    @Override
    protected void run() throws Exception {
        step(() -> {
            variantStorageManager.removeSearchIndexSamples(getStudyFqn(), params.getAsStringList("samples"), params, token);
        });
    }
}
