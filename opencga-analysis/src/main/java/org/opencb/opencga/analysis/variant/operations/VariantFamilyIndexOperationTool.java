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

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.operations.variant.VariantFamilyIndexParams;
import org.opencb.opencga.core.models.common.Enums;

import java.util.List;

@Tool(id = VariantFamilyIndexOperationTool.ID, description = VariantFamilyIndexOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantFamilyIndexOperationTool extends OperationTool {

    public static final String ID = "variant-family-index";
    public static final String DESCRIPTION = "Build the family index";

    private String study;
    private VariantFamilyIndexParams variantFamilyIndexParams;

    @Override
    protected void check() throws Exception {
        super.check();

        variantFamilyIndexParams = VariantFamilyIndexParams.fromParams(VariantFamilyIndexParams.class, params);
        study = getStudyFqn();

        if (CollectionUtils.isEmpty(variantFamilyIndexParams.getFamily())) {
            throw new IllegalArgumentException("Empty list of families");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            DataResult<List<String>> trios = variantStorageManager.familyIndex(
                    study,
                    variantFamilyIndexParams.getFamily(),
                    variantFamilyIndexParams.isSkipIncompleteFamilies(),
                    params,
                    token);
            if (trios.getEvents() != null) {
                for (Event event : trios.getEvents()) {
                    addEvent(event.getType(), event.getMessage());
                }
            }
        });
    }
}
