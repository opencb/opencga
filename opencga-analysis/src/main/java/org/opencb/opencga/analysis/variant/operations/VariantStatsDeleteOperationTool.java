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
import org.opencb.opencga.core.models.operations.variant.VariantStatsDeleteParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

@Tool(id = VariantStatsDeleteOperationTool.ID, description = VariantStatsDeleteOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.STUDY,
        resource = Enums.Resource.VARIANT)
public class VariantStatsDeleteOperationTool extends OperationTool {

    public static final String ID = "variant-stats-delete";
    public static final String DESCRIPTION = "Deletes the VariantStats of a cohort/s from the database";

    @ToolParams
    protected VariantStatsDeleteParams toolParams;

    private String study;

    @Override
    protected void check() throws Exception {
        super.check();

        if (toolParams.getCohort() == null || toolParams.getCohort().isEmpty()) {
            throw new IllegalArgumentException("Missing cohort/s to delete!");
        }

        study = getStudyFqn();

    }

    @Override
    protected void run() throws Exception {
        step(() -> {
//            if (toolParams.isResume()) {
//                params.put(VariantStorageOptions.RESUME.key(), true);
//            }
            if (toolParams.isForce()) {
                params.put(VariantStorageOptions.FORCE.key(), true);
            }
            variantStorageManager.deleteStats(study, toolParams.getCohort(), params, token);
        });
    }
}
