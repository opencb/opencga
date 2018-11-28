package org.opencb.opencga.storage.benchmark.variant.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 30/10/18.
 */
public class FixedQueries {

    private List<FixedQuery> queries;
    private List<String> sessionIds;
    private Map<String, String> baseQuery;

    public FixedQueries() {
    }

    public Map<String, String> getBaseQuery() {
        return baseQuery;
    }

    public FixedQueries setBaseQuery(Map<String, String> baseQuery) {
        this.baseQuery = baseQuery;
        return this;
    }

    public List<FixedQuery> getQueries() {
        return queries;
    }

    public FixedQueries setQueries(List<FixedQuery> queries) {
        this.queries = queries;
        return this;
    }

    public FixedQueries addQuery(FixedQuery query) {
        if (this.queries == null) {
            this.queries = new ArrayList<>();
        }
        this.queries.add(query);
        return this;
    }

    public List<String> getSessionIds() {
        return sessionIds;
    }

    public FixedQueries setSessionIds(List<String> sessionIds) {
        this.sessionIds = sessionIds;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FixedQueries{");
        sb.append("queries=").append(queries);
        sb.append(", sessionIds=").append(sessionIds);
        sb.append(", baseQuery=").append(baseQuery);
        sb.append('}');
        return sb.toString();
    }
}

