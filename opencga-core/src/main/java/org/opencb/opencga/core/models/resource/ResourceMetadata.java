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
    List<AnalysisResourceList> analysisResourceLists;

    public ResourceMetadata() {
        this("", "", new ArrayList<>());
    }

    public ResourceMetadata(String version, String urlBase, List<AnalysisResourceList> analysisResourceLists) {
        this.version = version;
        this.analysisResourceLists = analysisResourceLists;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResourceMetadata{");
        sb.append("version='").append(version).append('\'');
        sb.append(", analysisResourceLists=").append(analysisResourceLists);
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

    public List<AnalysisResourceList> getAnalysisResourceLists() {
        return analysisResourceLists;
    }

    public ResourceMetadata setAnalysisResourceLists(List<AnalysisResourceList> analysisResourceLists) {
        this.analysisResourceLists = analysisResourceLists;
        return this;
    }
}