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

public class AnalysisResourceList {

    private String analysisId;
    private String analysisVersion;
    private List<AnalysisResource> resources;

    public AnalysisResourceList() {
        this("", "", new ArrayList<>());
    }

    public AnalysisResourceList(String analysisId, String analysisVersion, List<AnalysisResource> resources) {
        this.analysisId = analysisId;
        this.analysisVersion = analysisVersion;
        this.resources = resources;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnalysisResourceList{");
        sb.append("analysisId='").append(analysisId).append('\'');
        sb.append(", analysisVersion='").append(analysisVersion).append('\'');
        sb.append(", resources=").append(resources);
        sb.append('}');
        return sb.toString();
    }

    public String getAnalysisId() {
        return analysisId;
    }

    public AnalysisResourceList setAnalysisId(String analysisId) {
        this.analysisId = analysisId;
        return this;
    }

    public String getAnalysisVersion() {
        return analysisVersion;
    }

    public AnalysisResourceList setAnalysisVersion(String analysisVersion) {
        this.analysisVersion = analysisVersion;
        return this;
    }

    public List<AnalysisResource> getResources() {
        return resources;
    }

    public AnalysisResourceList setResources(List<AnalysisResource> resources) {
        this.resources = resources;
        return this;
    }
}
