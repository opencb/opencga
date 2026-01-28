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

package org.opencb.opencga.core.models.clinical.pipeline.genomics;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineTool;

import java.util.Map;

public class GenomicsAlignmentPipelineTool extends PipelineTool {

    @DataField(id = "index", description = "Tool index path")
    private String index;

    public GenomicsAlignmentPipelineTool() {
    }

    public GenomicsAlignmentPipelineTool(String index) {
        this.index = index;
    }

    public GenomicsAlignmentPipelineTool(String id, Map<String, Object> parameters, Map<String, Object> options, String index) {
        super(id, parameters, options);
        this.index = index;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenomicsAlignmentPipelineTool{");
        sb.append("index='").append(index).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", parameters=").append(parameters);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public String getIndex() {
        return index;
    }

    public GenomicsAlignmentPipelineTool setIndex(String index) {
        this.index = index;
        return this;
    }
}