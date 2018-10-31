package org.opencb.opencga.storage.benchmark.variant.queries;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wasim on 30/10/18.
 */
public class FixedQueries {

    private List<FixedQuery> queries;

    public FixedQueries() {
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


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FixedQueries{");
        sb.append("queries=").append(queries);
        sb.append('}');
        return sb.toString();
    }
}

