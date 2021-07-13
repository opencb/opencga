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

package org.opencb.opencga.catalog.templates.config;

public class TemplateConfiguration {

    private String version;
    private String baseUrl;
    private boolean index;
    private String projectId;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TemplateConfiguration{");
        sb.append("version='").append(version).append('\'');
        sb.append(", baseUrl='").append(baseUrl).append('\'');
        sb.append(", index=").append(index);
        sb.append(", projectId='").append(projectId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public boolean isIndex() {
        return index;
    }

    public void setIndex(boolean index) {
        this.index = index;
    }

    public String getProjectId() {
        return projectId;
    }

    public TemplateConfiguration setProjectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

}
