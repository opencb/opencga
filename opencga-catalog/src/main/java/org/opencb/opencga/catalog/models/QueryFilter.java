/*
 * Copyright 2015-2016 OpenCB
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
