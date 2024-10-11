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

package org.opencb.opencga.core.models.resource;

import java.util.ArrayList;
import java.util.List;

public class AnalysisResource {

    private String id;
    private List<String> resources;

    public AnalysisResource() {
        this("", new ArrayList<>());
    }

    public AnalysisResource(String id, List<String> resources) {
        this.id = id;
        this.resources = resources;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnalysisResource{");
        sb.append("id='").append(id).append('\'');
        sb.append(", resources=").append(resources);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public AnalysisResource setId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getResources() {
        return resources;
    }

    public AnalysisResource setResources(List<String> resources) {
        this.resources = resources;
        return this;
    }
}
