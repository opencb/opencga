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
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.operations.variant.VariantSecondarySampleIndexParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.ArrayList;
import java.util.List;

@Tool(id = VariantSecondarySampleIndexOperationTool.ID, description = VariantSecondarySampleIndexOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantSecondarySampleIndexOperationTool extends OperationTool {

    public static final String ID = "variant-secondary-sample-index";
    public static final String DESCRIPTION = "Build and annotate the sample index.";
    protected String study;

    @ToolParams
    protected VariantSecondarySampleIndexParams sampleIndexParams;

    @Override
    protected void check() throws Exception {
        super.check();
        study = getStudyFqn();

        if (CollectionUtils.isEmpty(sampleIndexParams.getSample())) {
            throw new IllegalArgumentException("Empty list of samples");
        }
        if (!sampleIndexParams.isBuildIndex() && !sampleIndexParams.isAnnotate() && !sampleIndexParams.isFamilyIndex()) {
            sampleIndexParams.setBuildIndex(true);
            sampleIndexParams.setAnnotate(true);
            sampleIndexParams.setFamilyIndex(true);
        }
        params.put(ParamConstants.OVERWRITE, sampleIndexParams.isOverwrite());
    }

    @Override
    protected List<String> getSteps() {
        ArrayList<String> steps = new ArrayList<>();
        if (sampleIndexParams.isBuildIndex()) {
            steps.add("buildIndex");
        }
        if (sampleIndexParams.isAnnotate()) {
            steps.add("annotate");
        }
        if (sampleIndexParams.isFamilyIndex()) {
            steps.add("familyIndex");
        }
        return steps;
    }

    @Override
    protected void run() throws Exception {
        if (sampleIndexParams.isBuildIndex()) {
            step("buildIndex", () -> variantStorageManager.sampleIndex(study, sampleIndexParams.getSample(), params, token));
        }
        if (sampleIndexParams.isAnnotate()) {
            step("annotate", () -> variantStorageManager.sampleIndexAnnotate(study, sampleIndexParams.getSample(), params, token));
        }
        if (sampleIndexParams.isFamilyIndex()) {
            step("familyIndex", () -> {
                DataResult<List<String>> result = variantStorageManager.familyIndexBySamples(study, sampleIndexParams.getSample(), params,
                        getToken());
                if (result.getEvents() != null) {
                    for (Event event : result.getEvents()) {
                        addEvent(event.getType(), event.getMessage());
                    }
                }
            });
        }
    }
}
