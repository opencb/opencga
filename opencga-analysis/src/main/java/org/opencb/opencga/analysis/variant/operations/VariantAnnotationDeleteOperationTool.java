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
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationDeleteParams;
import org.opencb.opencga.core.models.common.Enums;

@Tool(id = VariantAnnotationDeleteOperationTool.ID, description = VariantAnnotationDeleteOperationTool.ID,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.PROJECT,
        resource = Enums.Resource.VARIANT)
public class VariantAnnotationDeleteOperationTool extends OperationTool {

    public static final String ID = "variant-annotation-delete";
    public static final String DESCRIPTION = "Deletes a saved copy of variant annotation";
    private VariantAnnotationDeleteParams variantAnnotationDeleteParams;
    private String project;

    @Override
    protected void check() throws Exception {
        super.check();

        variantAnnotationDeleteParams = VariantAnnotationDeleteParams.fromParams(VariantAnnotationDeleteParams.class, params);
        project = getProjectFqn();

    }

    @Override
    protected void run() throws Exception {
        step(()->{
            variantStorageManager.deleteAnnotation(project, variantAnnotationDeleteParams.getAnnotationId(), params, token);
        });
    }
}
