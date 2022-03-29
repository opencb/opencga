/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.models.user;

import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.common.Enums;

public class UserFilter {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", defaultValue = "No description available",
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "resource",
            description = FieldConstants.USER_FILTER_RESOURCE_DESCRIPTION)
    private Enums.Resource resource;

    @DataField(id = "query",
            description = FieldConstants.USER_FILTER_QUERY)
    private Query query;


    @DataField(id = "query", uncommentedClasses = {"QueryOptions"},
            description = FieldConstants.USER_FILTER_QUERY_OPTIONS)
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
