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
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationSaveParams;
import org.opencb.opencga.core.models.common.Enums;

@Tool(id = VariantAnnotationSaveOperationTool.ID, description = VariantAnnotationSaveOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.PROJECT,
        resource = Enums.Resource.VARIANT)
public class VariantAnnotationSaveOperationTool extends OperationTool {

    public static final String ID = "variant-annotation-save";
    public static final String DESCRIPTION = "Save a copy of the current variant annotation at the database";
    public String project;
    private VariantAnnotationSaveParams annotationSaveParams;

    @Override
    protected void check() throws Exception {
        super.check();
        annotationSaveParams = VariantAnnotationSaveParams.fromParams(VariantAnnotationSaveParams.class, params);
        project = getProjectFqn();
    }

    @Override
    protected void run() throws Exception {
        step(()->{
            variantStorageManager.saveAnnotation(project, annotationSaveParams.getAnnotationId(), params, token);
        });
    }
}
