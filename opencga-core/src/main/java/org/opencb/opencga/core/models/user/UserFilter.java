package org.opencb.opencga.core.models.user;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.common.Enums;

public class UserFilter {

    private String id;
    private String description;
    private Enums.Resource resource;
    private Query query;
    private QueryOptions options;

    public UserFilter() {
    }

    public UserFilter(String id, String description, Enums.Resource resource, Query query, QueryOptions options) {
        this.id = id;
        this.description = description;
        this.resource = resource;
        this.query = query;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Filter{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", resource=").append(resource);
        sb.append(", query=").append(query);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public UserFilter setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public UserFilter setDescription(String description) {
        this.description = description;
        return this;
    }

    public Enums.Resource getResource() {
        return resource;
    }

    public UserFilter setResource(Enums.Resource resource) {
        this.resource = resource;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public UserFilter setQuery(Query query) {
        this.query = query;
        return this;
    }

    public QueryOptions getOptions() {
        return options;
    }

    public UserFilter setOptions(QueryOptions options) {
        this.options = options;
        return this;
    }
}
