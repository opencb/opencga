package org.opencb.opencga.storage.benchmark.variant.queries;

import java.util.Map;

/**
 * Created by wasim on 30/10/18.
 */
public class FixedQuery {
    private String id;
    private String description;
    private Map<String, String> query;

    public FixedQuery() {
    }

    public FixedQuery(String id, String description, Map<String, String> query) {
        this.id = id;
        this.description = description;
        this.query = query;
    }


    public String getId() {
        return id;
    }

    public FixedQuery setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FixedQuery setDescription(String description) {
        this.description = description;
        return this;
    }

    public Map<String, String> getQuery() {
        return query;
    }

    public FixedQuery setQuery(Map<String, String> query) {
        this.query = query;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FixedQuery{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", query=").append(query);
        sb.append('}');
        return sb.toString();
    }
}
