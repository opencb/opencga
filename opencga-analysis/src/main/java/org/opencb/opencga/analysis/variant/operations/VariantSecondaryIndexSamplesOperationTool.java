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
import org.opencb.opencga.core.models.operations.variant.VariantSecondaryIndexParams;
import org.opencb.opencga.core.models.common.Enums;

@Tool(id = VariantSecondaryIndexSamplesOperationTool.ID,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantSecondaryIndexSamplesOperationTool extends OperationTool {

    public static final String ID = "variant-search-index-samples";
    private VariantSecondaryIndexParams indexParams;
    private String studyFqn;

    @Override
    protected void check() throws Exception {
        super.check();
        indexParams = VariantSecondaryIndexParams.fromParams(VariantSecondaryIndexParams.class, params);
        studyFqn = getStudyFqn();
    }

    @Override
    protected void run() throws Exception {
        step(() -> variantStorageManager.secondaryIndexSamples(studyFqn, indexParams.getSample(), params, token));
    }
}
