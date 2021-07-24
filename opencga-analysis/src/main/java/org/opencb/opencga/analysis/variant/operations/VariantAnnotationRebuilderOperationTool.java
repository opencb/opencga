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
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = VariantAnnotationRebuilderOperationTool.ID, description = VariantAnnotationRebuilderOperationTool.ID,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.PROJECT,
        resource = Enums.Resource.VARIANT)
public class VariantAnnotationRebuilderOperationTool extends OperationTool {

    public static final String ID = "variant-annotation-rebuilder";
    public static final String DESCRIPTION = "Regenerate and repairs variant annotation. "
            + "This tool does not compute any new annotation";

    @Override
    protected void check() throws Exception {
        super.check();
    }

    @Override
    protected void run() throws Exception {
        step(()-> {
            setUpStorageEngineExecutorByProjectId(getProjectFqn());
            getToolExecutor().execute();
        });
    }
}
