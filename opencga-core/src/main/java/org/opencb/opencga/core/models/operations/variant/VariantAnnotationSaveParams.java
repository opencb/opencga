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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

public class VariantAnnotationSaveParams extends ToolParams {

    public static final String DESCRIPTION = "Variant annotation save params";
    /** ObjectMap key for the {@link #fromAnnotationSet} field. */
    public static final String FROM_ANNOTATION_SET = "fromAnnotationSet";

    @DataField(description = "New Variant Annotation identifier")
    private String annotationId;
    @DataField(description = "Source annotation set to save. When set to a transition's auto-name "
            + "(e.g. recorded by an earlier autobump on setCellbaseConfiguration), the matching "
            + "variants are copied and the transition entry is promoted to saved under "
            + "'annotationId'. When unset (or 'current'), the current annotation state is "
            + "captured: autobump current into a new transition, then immediately promote it. "
            + "This is the field exposed via the --from-annotation-set CLI flag.")
    private String fromAnnotationSet;

    public VariantAnnotationSaveParams() {
    }

    public VariantAnnotationSaveParams(String annotationId) {
        this.annotationId = annotationId;
    }

    public VariantAnnotationSaveParams(String annotationId, String fromAnnotationSet) {
        this.annotationId = annotationId;
        this.fromAnnotationSet = fromAnnotationSet;
    }

    public String getAnnotationId() {
        return annotationId;
    }

    public VariantAnnotationSaveParams setAnnotationId(String annotationId) {
        this.annotationId = annotationId;
        return this;
    }

    public String getFromAnnotationSet() {
        return fromAnnotationSet;
    }

    public VariantAnnotationSaveParams setFromAnnotationSet(String fromAnnotationSet) {
        this.fromAnnotationSet = fromAnnotationSet;
        return this;
    }
}
