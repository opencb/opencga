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

package org.opencb.opencga.core.models.job;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultNonNullObjectMapper;

public class JobUpdateParams {
    private String description;

    private List<String> tags;
    private Boolean visited;

    private Map<String, Object> attributes;

    public JobUpdateParams() {
    }

    public JobUpdateParams(String description, List<String> tags, Boolean visited, Map<String, Object> attributes) {
        this.description = description;
        this.tags = tags;
        this.visited = visited;
        this.attributes = attributes;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getDefaultNonNullObjectMapper().writeValueAsString(this));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobUpdateParams{");
        sb.append(", description='").append(description).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", visited=").append(visited);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getDescription() {
        return description;
    }

    public JobUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public JobUpdateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public Boolean getVisited() {
        return visited;
    }

    public JobUpdateParams setVisited(Boolean visited) {
        this.visited = visited;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public JobUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
