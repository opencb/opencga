package org.opencb.opencga.core.models.study.configuration;

import java.util.Map;

public class VariantInclusionQueryConfiguration {

    private String id;
    private Map<String, Object> query;

    public VariantInclusionQueryConfiguration() {
    }

    public VariantInclusionQueryConfiguration(String id, Map<String, Object> query) {
        this.id = id;
        this.query = query;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantInclusionQueryConfiguration{");
        sb.append("id='").append(id).append('\'');
        sb.append(", query=").append(query);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public VariantInclusionQueryConfiguration setId(String id) {
        this.id = id;
        return this;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public VariantInclusionQueryConfiguration setQuery(Map<String, Object> query) {
        this.query = query;
        return this;
    }
}
