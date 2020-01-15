package org.opencb.opencga.core.models.study;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class StudyUpdateParams {

    private String name;
    private String alias;
    private Study.Type type;
    private String description;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;

    public StudyUpdateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", type=").append(type);
        sb.append(", description='").append(description).append('\'');
        sb.append(", stats=").append(stats);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public StudyUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public StudyUpdateParams setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public Study.Type getType() {
        return type;
    }

    public StudyUpdateParams setType(Study.Type type) {
        this.type = type;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public StudyUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public StudyUpdateParams setStats(Map<String, Object> stats) {
        this.stats = stats;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public StudyUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
