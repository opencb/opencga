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

public class ProjectCreateParams {

    private String id;

    private String name;
    private String description;;
    private ProjectOrganism organism;

    public ProjectCreateParams() {
    }

    public ProjectCreateParams(String id, String name, String description, ProjectOrganism organism) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.organism = organism;
    }

    public static ProjectCreateParams of(Project project) {
        return new ProjectCreateParams(project.getId(), project.getName(), project.getDescription(), project.getOrganism());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", organism=").append(organism);
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

    public ProjectOrganism getOrganism() {
        return organism;
    }

    public ProjectCreateParams setOrganism(ProjectOrganism organism) {
        this.organism = organism;
        return this;
    }
}
