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

package org.opencb.opencga.core.models.monitor;

public class HealthCheckDependency {
    // 'Url of the service'
    private String url;
    private HealthCheckResponse.Status status;
    // 'Type of dependency. For example: `REST API`'
    private String type;
    // 'Description of the dependency. For example: `Exomiser`'
    private String description;
    private Object additionalProperties;

    public HealthCheckDependency() {
    }

    public HealthCheckDependency(String url, String type, String description) {
        this.url = url;
        this.type = type;
        this.description = description;
    }

    public HealthCheckDependency(String url, HealthCheckResponse.Status status, String type, String description,
                                 Object additionalProperties) {
        this.url = url;
        this.status = status;
        this.type = type;
        this.description = description;
        this.additionalProperties = additionalProperties;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HealthCheckDependency{");
        sb.append("url='").append(url).append('\'');
        sb.append(", status=").append(status);
        sb.append(", type='").append(type).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", additionalProperties=").append(additionalProperties);
        sb.append('}');
        return sb.toString();
    }

    public String getUrl() {
        return url;
    }

    public HealthCheckDependency setUrl(String url) {
        this.url = url;
        return this;
    }

    public HealthCheckResponse.Status getStatus() {
        return status;
    }

    public HealthCheckDependency setStatus(HealthCheckResponse.Status status) {
        this.status = status;
        return this;
    }

    public String getType() {
        return type;
    }

    public HealthCheckDependency setType(String type) {
        this.type = type;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public HealthCheckDependency setDescription(String description) {
        this.description = description;
        return this;
    }

    public Object getAdditionalProperties() {
        return additionalProperties;
    }

    public HealthCheckDependency setAdditionalProperties(Object additionalProperties) {
        this.additionalProperties = additionalProperties;
        return this;
    }

}
