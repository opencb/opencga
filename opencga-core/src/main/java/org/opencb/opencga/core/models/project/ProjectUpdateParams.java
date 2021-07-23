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

package org.opencb.opencga.core.models.project;

import java.util.Map;

public class ProjectUpdateParams {
    private String name;
    private String description;
    private String creationDate;
    private ProjectOrganism organism;
    private Map<String, Object> attributes;

    public ProjectUpdateParams() {
    }

    public ProjectUpdateParams(String name, String description, String creationDate, ProjectOrganism organism,
                               Map<String, Object> attributes) {
        this.name = name;
        this.description = description;
        this.creationDate = creationDate;
        this.organism = organism;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", organism=").append(organism);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public ProjectUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ProjectUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ProjectUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public ProjectOrganism getOrganism() {
        return organism;
    }

    public ProjectUpdateParams setOrganism(ProjectOrganism organism) {
        this.organism = organism;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ProjectUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
