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

import java.util.Collections;
import java.util.Map;

public class ProjectCreateParams {

    private String id;
    private String name;
    private String description;
    private String creationDate;
    private String modificationDate;
    private ProjectOrganism organism;
    private Map<String, Object> attributes;

    public ProjectCreateParams() {
    }

    public ProjectCreateParams(String id, String name, String description, String creationDate, String modificationDate,
                               ProjectOrganism organism, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.organism = organism;
        this.attributes = attributes;
    }

    public static ProjectCreateParams of(Project project) {
        return new ProjectCreateParams(project.getId(), project.getName(), project.getCreationDate(), project.getModificationDate(),
                project.getDescription(), project.getOrganism(), project.getAttributes());
    }

    public Project toProject() {
        return new Project(id, name, creationDate, modificationDate, description, organism, Collections.emptyList(), 1,
                ProjectInternal.init(), attributes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", organism=").append(organism);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ProjectCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ProjectCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ProjectCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ProjectCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public ProjectCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public ProjectOrganism getOrganism() {
        return organism;
    }

    public ProjectCreateParams setOrganism(ProjectOrganism organism) {
        this.organism = organism;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ProjectCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
