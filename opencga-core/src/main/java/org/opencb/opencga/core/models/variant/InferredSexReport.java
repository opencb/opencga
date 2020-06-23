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

package org.opencb.opencga.core.models.variant;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InferredSexReport {

    private String method;
    private String inferredKaryotypicSex;
    private Map<String, Object> values;
    private List<String> files;

    public InferredSexReport() {
        this("CoverageRatio", "", new LinkedHashMap<>(), new ArrayList<>());
    }

    public InferredSexReport(String method, String inferredKaryotypicSex, Map<String, Object> values, List<String> files) {
        this.method = method;
        this.inferredKaryotypicSex = inferredKaryotypicSex;
        this.values = values;
        this.files = files;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InferredSexReport{");
        sb.append("method='").append(method).append('\'');
        sb.append(", inferredKaryotypicSex='").append(inferredKaryotypicSex).append('\'');
        sb.append(", values=").append(values);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }

    public String getMethod() {
        return method;
    }

    public InferredSexReport setMethod(String method) {
        this.method = method;
        return this;
    }

    public String getInferredKaryotypicSex() {
        return inferredKaryotypicSex;
    }

    public InferredSexReport setInferredKaryotypicSex(String inferredKaryotypicSex) {
        this.inferredKaryotypicSex = inferredKaryotypicSex;
        return this;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public InferredSexReport setValues(Map<String, Object> values) {
        this.values = values;
        return this;
    }

    public List<String> getFiles() {
        return files;
    }

    public InferredSexReport setFiles(List<String> files) {
        this.files = files;
        return this;
    }
}
