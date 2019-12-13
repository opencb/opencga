package org.opencb.opencga.catalog.models.update;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultNonNullObjectMapper;

public class JobUpdateParams {

    private String name;
    private String description;

    private List<String> tags;
    private Boolean visited;

    private Map<String, Object> attributes;

    public JobUpdateParams() {
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws CatalogException {
        try {
            return new ObjectMap(getDefaultNonNullObjectMapper().writeValueAsString(this));
        } catch (JsonProcessingException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", visited=").append(visited);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public JobUpdateParams setName(String name) {
        this.name = name;
        return this;
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
