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

package org.opencb.opencga.core.models.file;

import java.util.Map;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileLinkInternalParams {

    @DataField(description = ParamConstants.FILE_LINK_INTERNAL_PARAMS_SAMPLE_MAP_DESCRIPTION)
    private Map<String, String> sampleMap;

    public FileLinkInternalParams() {
    }

    public FileLinkInternalParams(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileLinkInternalParams{");
        sb.append("sampleMap=").append(sampleMap);
        sb.append('}');
        return sb.toString();
    }

    public Map<String, String> getSampleMap() {
        return sampleMap;
    }

    public FileLinkInternalParams setSampleMap(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
        return this;
    }
}
