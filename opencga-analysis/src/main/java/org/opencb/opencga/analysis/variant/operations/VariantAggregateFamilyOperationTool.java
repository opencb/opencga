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

import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.List;

@Tool(id = VariantAggregateFamilyOperationTool.ID, description = VariantAggregateFamilyOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION,
        resource = Enums.Resource.VARIANT,
        priority = Enums.Priority.HIGH)
public class VariantAggregateFamilyOperationTool extends OperationTool {
    public static final String ID = "variant-aggregate-family";
    public static final String DESCRIPTION = "Find variants where not all the samples are present, and fill the empty values.";
    private String study;

    @ToolParams
    protected VariantAggregateFamilyParams variantAggregateFamilyParams;


    @Override
    protected void check() throws Exception {
        super.check();

        List<String> samples = variantAggregateFamilyParams.getSamples();
        if (samples == null || samples.size() < 2) {
            throw new IllegalArgumentException("Fill gaps operation requires at least two samples!");
        }

        study = getStudyFqn();
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            variantStorageManager.aggregateFamily(study, variantAggregateFamilyParams, token, getOutDir().toUri());
        });
    }
}
