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

import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantAnnotationSaveParams extends ToolParams {

    public static final String DESCRIPTION = "Variant annotation save params";
    @DataField(description = ParamConstants.VARIANT_ANNOTATION_SAVE_PARAMS_ANNOTATION_ID_DESCRIPTION)
    private String annotationId;

    public VariantAnnotationSaveParams() {
    }

    public VariantAnnotationSaveParams(String annotationId) {
        this.annotationId = annotationId;
    }

    public String getAnnotationId() {
        return annotationId;
    }

    public VariantAnnotationSaveParams setAnnotationId(String annotationId) {
        this.annotationId = annotationId;
        return this;
    }
}
