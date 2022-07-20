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

package org.opencb.opencga.core.models.common;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class TsvAnnotationParams {

    /**
     * Content of the TSV file if this does has not been registered yet in OpenCGA.
     */
    @DataField(description = ParamConstants.TSV_ANNOTATION_PARAMS_CONTENT_DESCRIPTION)
    private String content;


    public TsvAnnotationParams() {
    }

    public TsvAnnotationParams(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TsvAnnotationParams{");
        sb.append("content='").append(content).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getContent() {
        return content;
    }

    public TsvAnnotationParams setContent(String content) {
        this.content = content;
        return this;
    }
}
