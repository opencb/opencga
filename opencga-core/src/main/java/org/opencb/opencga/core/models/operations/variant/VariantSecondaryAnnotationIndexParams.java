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

public class VariantSecondaryAnnotationIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Variant secondary annotation index params.";

    private String region;
    private boolean overwrite;

    public VariantSecondaryAnnotationIndexParams() {
    }

    public VariantSecondaryAnnotationIndexParams(String region, boolean overwrite) {
        this.region = region;
        this.overwrite = overwrite;
    }

    public String getRegion() {
        return region;
    }

    public VariantSecondaryAnnotationIndexParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public VariantSecondaryAnnotationIndexParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }
}
