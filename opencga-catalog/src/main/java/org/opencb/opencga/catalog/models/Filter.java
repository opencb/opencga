package org.opencb.opencga.catalog.models;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

/**
 * Created by pfurio on 13/04/16.
 */
public class Filter {

    private String id;
    private Query query;
    private QueryOptions queryOptions;

    public Filter() {
        this("", new Query(), new QueryOptions());
    }

    public Filter(String id, Query query, QueryOptions queryOptions) {
        this.id = id;
        this.query = query;
        this.queryOptions = queryOptions;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    public void setQueryOptions(QueryOptions queryOptions) {
        this.queryOptions = queryOptions;
    }
}
