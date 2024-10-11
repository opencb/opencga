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

public class ResourceMetadata  {

    private String version;
    List<AnalysisResource> analysisResources;

    public ResourceMetadata() {
        this("", new ArrayList<>());
    }

    public ResourceMetadata(String version, List<AnalysisResource> analysisResources) {
        this.version = version;
        this.analysisResources = analysisResources;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResourceMetadata{");
        sb.append("version='").append(version).append('\'');
        sb.append(", analysisResources=").append(analysisResources);
        sb.append('}');
        return sb.toString();
    }

    public String getVersion() {
        return version;
    }

    public ResourceMetadata setVersion(String version) {
        this.version = version;
        return this;
    }

    public List<AnalysisResource> getAnalysisResources() {
        return analysisResources;
    }

    public ResourceMetadata setAnalysisResources(List<AnalysisResource> analysisResources) {
        this.analysisResources = analysisResources;
        return this;
    }
}
