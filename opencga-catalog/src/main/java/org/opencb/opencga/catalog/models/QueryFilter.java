package org.opencb.opencga.catalog.models;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

/**
 * Created by pfurio on 13/04/16.
 */
public class QueryFilter {

    private String id;
    private Query query;
    private QueryOptions queryOptions;

    public QueryFilter() {
        this("", new Query(), new QueryOptions());
    }

    public QueryFilter(String id, Query query, QueryOptions queryOptions) {
        this.id = id;
        this.query = query;
        this.queryOptions = queryOptions;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueryFilter{");
        sb.append("id='").append(id).append('\'');
        sb.append(", query=").append(query);
        sb.append(", queryOptions=").append(queryOptions);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public QueryFilter setId(String id) {
        this.id = id;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public QueryFilter setQuery(Query query) {
        this.query = query;
        return this;
    }

    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    public QueryFilter setQueryOptions(QueryOptions queryOptions) {
        this.queryOptions = queryOptions;
        return this;
    }

}
